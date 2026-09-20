"""
A small but faithful in-memory Firestore simulator.

Faithful in the one way that matters for this test suite: documents
are version-stamped, a transaction records the version it read, and
committing a transaction fails (Aborted) if any read document's
version changed since -- exactly Firestore's real optimistic
concurrency model. @firestore.transactional's real behavior is to
retry the wrapped function automatically on Aborted, so this fake
reproduces that too, bounded by max_attempts.
"""

from __future__ import annotations

from google.api_core.exceptions import AlreadyExists


class Aborted(Exception):
    pass


class _Sentinel:
    def __repr__(self):
        return "SERVER_TIMESTAMP"


SERVER_TIMESTAMP = _Sentinel()


def _resolve(data: dict) -> dict:
    """Replace the SERVER_TIMESTAMP sentinel with a monotonically increasing fake clock."""
    out = {}
    for key, value in data.items():
        out[key] = FakeClock.tick() if value is SERVER_TIMESTAMP else value
    return out


class FakeClock:
    """Monotonic counter standing in for real wall-clock timestamps in tests."""
    _t = 0

    @classmethod
    def tick(cls):
        cls._t += 1
        return cls._t

    @classmethod
    def now(cls):
        return cls._t


class DocSnapshot:
    def __init__(self, doc_id, data, exists):
        self.id = doc_id
        self._data = data
        self.exists = exists

    def to_dict(self):
        return dict(self._data) if self._data is not None else None


class FakeDocumentRef:
    def __init__(self, store, path):
        self.store = store
        self.path = path

    @property
    def _id(self):
        return self.path.rsplit("/", 1)[-1]

    def get(self, transaction=None):
        if transaction is not None:
            return transaction.get(self)

        entry = self.store.get(self.path)
        if entry is None:
            return DocSnapshot(self._id, None, False)

        return DocSnapshot(self._id, dict(entry["data"]), True)

    def create(self, data):
        if self.path in self.store:
            raise AlreadyExists(self.path)
        self.store[self.path] = {"data": _resolve(data), "version": 1}

    def set(self, data, merge=False):
        entry = self.store.get(self.path)
        if merge and entry:
            merged = dict(entry["data"])
            merged.update(_resolve(data))
            self.store[self.path] = {"data": merged, "version": entry["version"] + 1}
        else:
            version = entry["version"] + 1 if entry else 1
            self.store[self.path] = {"data": _resolve(data), "version": version}

    def update(self, data):
        entry = self.store.get(self.path)
        if entry is None:
            raise Exception(f"NotFound: {self.path}")
        merged = dict(entry["data"])
        merged.update(_resolve(data))
        self.store[self.path] = {"data": merged, "version": entry["version"] + 1}

    def collection(self, name):
        return FakeCollectionRef(self.store, f"{self.path}/{name}")


class FakeCollectionRef:
    def __init__(self, store, path):
        self.store = store
        self.path = path

    def document(self, doc_id):
        return FakeDocumentRef(self.store, f"{self.path}/{doc_id}")

    def where(self, field, op, value):
        return FakeQuery(self.store, self.path, [(field, op, value)])

    def limit(self, n):
        return FakeQuery(self.store, self.path, []).limit(n)

    def stream(self):
        depth = self.path.count("/") + 1
        prefix = self.path + "/"
        for key, entry in list(self.store.items()):
            if key.startswith(prefix) and key.count("/") == depth:
                doc_id = key.rsplit("/", 1)[-1]
                yield DocSnapshot(doc_id, dict(entry["data"]), True)


class FakeQuery:
    def __init__(self, store, path, filters, limit_n=None):
        self.store = store
        self.path = path
        self.filters = list(filters)
        self.limit_n = limit_n

    def where(self, field, op, value):
        return FakeQuery(self.store, self.path, self.filters + [(field, op, value)], self.limit_n)

    def limit(self, n):
        return FakeQuery(self.store, self.path, self.filters, n)

    def stream(self):
        depth = self.path.count("/") + 1
        prefix = self.path + "/"
        results = []
        for key, entry in self.store.items():
            if not (key.startswith(prefix) and key.count("/") == depth):
                continue
            data = entry["data"]
            if all(self._matches(data, f) for f in self.filters):
                results.append(DocSnapshot(key.rsplit("/", 1)[-1], dict(data), True))
        if self.limit_n is not None:
            results = results[: self.limit_n]
        return results

    @staticmethod
    def _matches(data, filt):
        field, op, value = filt
        actual = data.get(field)
        if op == "==":
            return actual == value
        raise NotImplementedError(op)


class FakeTransaction:
    """
    Records the version of every document read; on commit, aborts if
    any of those versions changed. Writes are buffered and only
    applied on a successful commit.
    """

    def __init__(self, store):
        self.store = store
        self._reads = {}
        self._writes = []

    def get(self, doc_ref):
        entry = self.store.get(doc_ref.path)
        version = entry["version"] if entry else 0
        self._reads[doc_ref.path] = version
        return doc_ref.get()

    def update(self, doc_ref, data):
        self._writes.append(("update", doc_ref, data))

    def set(self, doc_ref, data, merge=False):
        self._writes.append(("set", doc_ref, data, merge))

    def _commit(self):
        for path, read_version in self._reads.items():
            entry = self.store.get(path)
            current_version = entry["version"] if entry else 0
            if current_version != read_version:
                raise Aborted(f"Document {path} changed since it was read in this transaction")

        for write in self._writes:
            if write[0] == "update":
                _, doc_ref, data = write
                doc_ref.update(data)
            else:
                _, doc_ref, data, merge = write
                doc_ref.set(data, merge=merge)


def transactional(func, max_attempts=5):
    """Stand-in for firebase_admin.firestore.transactional: retries on Aborted."""
    def wrapper(transaction, *args, **kwargs):
        last_exc = None
        for _ in range(max_attempts):
            transaction._reads.clear()
            transaction._writes.clear()
            try:
                result = func(transaction, *args, **kwargs)
                transaction._commit()
                return result
            except Aborted as exc:
                last_exc = exc
                continue
        raise last_exc
    return wrapper


class FakeFirestoreClient:
    def __init__(self):
        self.store = {}

    def collection(self, name):
        return FakeCollectionRef(self.store, name)

    def transaction(self):
        return FakeTransaction(self.store)
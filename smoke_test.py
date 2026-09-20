import firestore_repo
from sync_common import authenticate, scrape_date_with_retry


HALL_TICKET = "23n01a0501"
TEST_DATE = "2026-09-19"


def main():
    print(f"Testing hall ticket: {HALL_TICKET}")
    print(f"Testing date: {TEST_DATE}")

    # Make sure the student document exists
    firestore_repo.create_student(HALL_TICKET)

    # Login to SCCE once
    session = authenticate(HALL_TICKET)

    try:
        # Scrape exactly one date
        result, session = scrape_date_with_retry(
            session=session,
            hall_ticket=HALL_TICKET,
            scrape_date=TEST_DATE,
        )

        print("\nSCRAPE SUCCESS")
        print(f"Date:       {result['date']}")
        print(f"Attended:   {result['attended']}")
        print(f"Conducted:  {result['conducted']}")
        print(f"Percentage: {result['percentage']}")
        print(f"Records:    {len(result['records'])}")

        # Save to Firebase
        firestore_repo.save_attendance_date(
            HALL_TICKET,
            result,
        )

        print("\nFIREBASE SAVE SUCCESS")

        # Read it back from Firebase
        saved = firestore_repo.get_attendance_date(
            HALL_TICKET,
            TEST_DATE,
        )

        print("\nFIREBASE READ SUCCESS")
        print(saved)

    finally:
        session.close()


if __name__ == "__main__":
    main()
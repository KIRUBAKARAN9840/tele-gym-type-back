"""
Backfill Script: Generate referral codes for existing users
Run this AFTER the database migration has been applied
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.utils.referral_code_generator import generate_unique_referral_code
import os
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()


def backfill_client_referral_codes(batch_size: int = 100):
    """
    Backfill referral codes for all existing clients without one.

    Args:
        batch_size: Number of records to process in each batch
    """
    # Get database URL from environment
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables")

    # Create engine and session
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        # Count total clients without referral codes
        count_query = text(
            "SELECT COUNT(*) FROM clients WHERE referral_code IS NULL OR referral_code = ''"
        )
        total_clients = db.execute(count_query).scalar()

        print(f"Found {total_clients} clients without referral codes")

        if total_clients == 0:
            print("No clients to update. All clients already have referral codes.")
            return

        # Process in batches
        offset = 0
        updated_count = 0
        failed_count = 0

        # Create progress bar
        with tqdm(total=total_clients, desc="Generating referral codes") as pbar:
            while offset < total_clients:
                # Fetch batch of clients without referral codes
                fetch_query = text("""
                    SELECT client_id, email, name
                    FROM clients
                    WHERE referral_code IS NULL OR referral_code = ''
                    ORDER BY client_id
                    LIMIT :limit OFFSET :offset
                """)

                clients = db.execute(
                    fetch_query,
                    {"limit": batch_size, "offset": offset}
                ).fetchall()

                if not clients:
                    break

                # Generate and update referral codes
                for client in clients:
                    client_id, email, name = client

                    try:
                        # Generate unique referral code using user_id
                        referral_code = generate_unique_referral_code(
                            db=db,
                            name=name,
                            user_id=client_id,
                            method="sequential",
                            table_name="clients"
                        )

                        # Update client with referral code
                        update_query = text("""
                            UPDATE clients
                            SET referral_code = :code
                            WHERE client_id = :client_id
                        """)

                        db.execute(
                            update_query,
                            {"code": referral_code, "client_id": client_id}
                        )

                        updated_count += 1
                        pbar.update(1)

                    except Exception as e:
                        print(f"\nError generating code for client {client_id} ({email}): {str(e)}")
                        failed_count += 1
                        pbar.update(1)

                # Commit batch
                db.commit()
                offset += batch_size

        print(f"\n{'='*60}")
        print(f"Backfill completed!")
        print(f"Successfully updated: {updated_count} clients")
        print(f"Failed: {failed_count} clients")
        print(f"{'='*60}")

        # Show some examples
        print("\nSample referral codes generated:")
        sample_query = text("""
            SELECT client_id, name, email, referral_code
            FROM clients
            WHERE referral_code IS NOT NULL AND referral_code != ''
            ORDER BY client_id
            LIMIT 10
        """)

        samples = db.execute(sample_query).fetchall()
        for sample in samples:
            print(f"  Client {sample[0]}: {sample[1]} ({sample[2]}) -> {sample[3]}")

    except Exception as e:
        db.rollback()
        print(f"Error during backfill: {str(e)}")
        raise

    finally:
        db.close()


def verify_uniqueness():
    """
    Verify all referral codes are unique.
    """
    database_url = os.getenv("DATABASE_URL")
    engine = create_engine(database_url)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()

    try:
        # Check for duplicates
        duplicate_query = text("""
            SELECT referral_code, COUNT(*) as count
            FROM clients
            WHERE referral_code IS NOT NULL AND referral_code != ''
            GROUP BY referral_code
            HAVING count > 1
        """)

        duplicates = db.execute(duplicate_query).fetchall()

        if duplicates:
            print(f"WARNING: Found {len(duplicates)} duplicate referral codes:")
            for dup in duplicates:
                print(f"  {dup[0]}: {dup[1]} occurrences")
            return False
        else:
            print("✓ All referral codes are unique!")
            return True

    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill referral codes for existing users")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of records to process per batch (default: 100)"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify uniqueness, don't generate codes"
    )

    args = parser.parse_args()

    if args.verify_only:
        print("Verifying referral code uniqueness...")
        verify_uniqueness()
    else:
        print("Starting referral code backfill...")
        print(f"Batch size: {args.batch_size}")
        print("-" * 60)

        backfill_client_referral_codes(batch_size=args.batch_size)

        print("\nVerifying uniqueness after backfill...")
        verify_uniqueness()

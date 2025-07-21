"""
Periodic MongoDB reader for counting documents by location and timestamp.
"""
#!/usr/bin/env python3

import os
import time
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, ReadPreference

# --- Config ---
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "aaDB"
COLLECTION_NAME = "aaColl"
INTERVAL = 30  # seconds

def main():
    """
    Connects to MongoDB, periodically counts documents in a collection by location,
    and prints the counts for AMER and EMEA regions every INTERVAL seconds.
    """

    while True:
        time.sleep(INTERVAL)
        client = MongoClient(
        MONGO_URI,
        read_preference=ReadPreference.PRIMARY_PREFERRED,
        )

        db = client.get_database(DB_NAME)
        coll = db[COLLECTION_NAME]

        try:
            amer_count = coll.count_documents(
                { "location": { "$in": ["US", "CA"]} } #  **query,
            )
            emea_count = coll.count_documents(
                { "location": { "$in": ["DE", "BE"]} }
            )
            print(f"[{datetime.now()}] AMER: {amer_count}, EMEA: {emea_count}")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error during read: {e}")

if __name__ == "__main__":
    main()

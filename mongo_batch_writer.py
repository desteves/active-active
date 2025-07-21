"""
Batch writer for MongoDB collections.

Reads documents from a JSON lines file named 'mongo_batch_<COLLECTION>.json'
and inserts them into the specified collection.

Usage:
    python mongo_batch_writer.py <MONGO_URI> <DB_NAME> <COLLECTION>
"""
#!/usr/bin/env python3

import sys
import json
import os
from datetime import datetime
from pymongo import MongoClient, WriteConcern, read_preferences
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Parse URI and arguments
if len(sys.argv) >= 2:
    uri = sys.argv[1]
else:
    uri = os.getenv("MONGO_URI")

if len(sys.argv) != 4:
    print("Usage: python mongo_batch_writer.py <MONGO_URI> <DB_NAME> <COLLECTION>")
    sys.exit(1)

db_name = sys.argv[2]
collection_name = sys.argv[3]
default_region = os.environ.get("REGION", "AMER")
batch_file = f"./mongo_batch_{default_region}.json.arr"
# Load documents
try:
    with open(batch_file, 'r', encoding='utf-8') as f:
        docs = json.load(f)
    if not isinstance(docs, list) or not docs:
        print("No documents to insert: batch file is empty or not a list.")
        sys.exit(0)
except Exception as e:
    print(f"Failed to read batch file: {e}")
    sys.exit(1)

# Connect and insert
def try_insert(documents):
    client = MongoClient(uri, retryWrites=True, read_preference=read_preferences.PrimaryPreferred())
    collection = client[db_name].get_collection(collection_name)
    return collection.insert_many(documents, ordered=False)

try:
    print(f"⏳ Bulk insert starting for REGION={default_region} at {datetime.now()}")
    try_insert(docs)
    print(f"✅ Inserted {len(docs)} documents successfully at {datetime.now()}")
except BulkWriteError as bwe:
    print(f"❌ Bulk write error on REGION={default_region} at {datetime.now()}")
    # Modify location based on region
    fallback_location = None
    if default_region == "AMER":
        fallback_location = "BE"
    elif default_region == "EMEA":
        fallback_location = "CA"

    if fallback_location:
        print(f"🔁 Retrying with fallback location: {fallback_location}")
        for doc in docs:
            doc["location"] = fallback_location
        try:
            try_insert(docs)
            print(f"✅ Fallback insert successful with location={fallback_location} at {datetime.now()}")
        except Exception as e2:
            print(f"❌ Fallback insert failed: {e2} at {datetime.now()}")
            sys.exit(1)
    else:
        print("⚠️ No fallback location defined for region.")
        sys.exit(1)
except Exception as e:
    print(f"❌ Insert failed: {e} at {datetime.now()}")
    sys.exit(1)

# Active-Active MongoDB Demo

This project demonstrates active-active MongoDB usage with region-aware read and write scripts.

## Prerequisites
- Python 3.x
- `pymongo` library (`pip install pymongo`)
- Bash shell (for app_write.sh)

## 1. Reading from MongoDB

The `app_read.py` script periodically counts and prints documents by region (AMER and EMEA) in a MongoDB collection.

**Run the reader:**
```sh
python app_read.py
```
- Edit the `MONGO_URI`, `DB_NAME`, and `COLLECTION_NAME` variables in the script as needed.

## 2. Writing to MongoDB by Region

The `app_write.sh` script writes batches of documents to MongoDB, tagging each with a region.

**Usage:**

- Replace `<REGION>` with your desired region
- The script will use the region flag to set the `location` field in the documents it writes.
- You may need to make the script executable:
  ```sh
  chmod +x app_write.sh
  ```

**Example:**
```sh
REGION=AMER ./app_write.sh 
REGION=EMEA ./app_write.sh
```

## Notes
- Ensure your MongoDB URI and credentials are set correctly in the scripts.
- The reader and writer can be run in parallel to simulate active-active workloads. 

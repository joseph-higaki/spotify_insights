import sqlite3
import os

_METADATA_DB = "metadata_store.db"


### NOT TESTED

# Initialize SQLite Metadata Store
def get_metadata_connection(user_name):
    exists_metadata_db = os.path.exists(_METADATA_DB)
    conn = sqlite3.connect(_METADATA_DB)
    with conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                user_name TEXT PRIMARY KEY, 
                file_name TEXT PRIMARY KEY,
                file_created_at TEXT,
                content_hash TEXT,
                file_processed_at TEXT,                       
                max_ts TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS max_stream_ts (
                user_name TEXT PRIMARY KEY,
                max_ts TEXT
            )
        """)
        # Ensure at least one record for max_stream_ts
        cursor.execute("SELECT COUNT(*) FROM max_stream_ts WHERE user_name = ? ", (user_name,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO max_stream_ts (user_name, max_ts) VALUES (?,?) ", (user_name, '1970-01-01T00:00:00Z'))
        conn.commit()
    return conn
    

# Get last max_ts
def get_last_max_ts(user_name):
    with get_metadata_connection(user_name) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT max_ts FROM max_stream_ts ORDER BY id DESC LIMIT 1")
        max_ts = cursor.fetchone()[0]
    #conn.close()
    return max_ts

# Update max_ts in the metadata store
def update_max_ts(user_name, new_max_ts):
    with get_metadata_connection(user_name) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO max_stream_ts (user_name, max_ts) VALUES (?)", (user_name, new_max_ts))
        conn.commit()    

# Check if file has been processed
def is_file_processed(user_name, filename):
    with get_metadata_connection(user_name) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_files WHERE user_name = ? and filename = ?", (user_name, filename))
        result = cursor.fetchone()[0]
    #conn.close()
    return result > 0

# Mark file as processed
def mark_file_as_processed(user_name, filename, creation_date, max_ts):
    with get_metadata_connection(user_name) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO processed_files (user_name, filename, creation_date, max_ts) 
            VALUES (?, ?, ?)
        """, (user_name, filename, creation_date, max_ts))
        conn.commit()




# Main Function from extract_raw.py
def main():       

    for root, user_name, files in os.walk(LOCAL_DIR):
        #last_max_ts = get_last_max_ts()
        logging.info(f"Last max_ts: {last_max_ts}")
        for file in files:
            if file.endswith(".json"):
                local_file_path = os.path.join(root, file)
                creation_date = datetime.fromtimestamp(os.path.getctime(local_file_path)).isoformat()

                # Skip if file has already been processed
                #if is_file_processed(file):
                #    logging.info(f"File {file} already processed. Skipping.")
                #    continue

                # Process JSON file
                filtered_file, max_ts = process_json_file(local_file_path, last_max_ts)
                if filtered_file:
                    gcs_path = os.path.join(RAW_BUCKET_PREFIX, file)
                    upload_to_gcs(GCS_BUCKET, filtered_file, gcs_path)

                    # Update metadata store
                    mark_file_as_processed(file, creation_date, max_ts)
                    if max_ts > last_max_ts:
                        update_max_ts(max_ts)
                else:
                    logging.info(f"File {file} contains no new data.")

if __name__ == "__main__":
    main()

import sys
import json
import os

# Add extractors/ to path so 'storage' is importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))

from storage.postgres_storage import PostgresStorageManager

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Missing argument: table name"}), flush=True)
        sys.exit(1)

    table_name = sys.argv[1]

    print("Reading JSON from stdin...", file=sys.stderr, flush=True)
    raw_data = sys.stdin.read()

    try:
        data = json.loads(raw_data)
    except Exception as e:
        print(json.dumps({"error": f"Invalid JSON data: {e}"}), flush=True)
        sys.exit(1)

    try:
        print("Connecting to Postgres...", file=sys.stderr, flush=True)
        storage = PostgresStorageManager()
        print("Connected", file=sys.stderr, flush=True)

        print(f"Saving {len(data)} records to '{table_name}'...", file=sys.stderr, flush=True)
        storage.save(data, table_name)
        storage.close()

        # ✅ Final output to be parsed by API
        print(json.dumps({"status": "success", "records_saved": len(data)}), flush=True)

    except Exception as e:
        print(json.dumps({"error": f"Exception: {str(e)}"}), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

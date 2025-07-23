import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))
from connectors.notion_connector import NotionConnector

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Missing arguments: database_id and token"}), flush=True)
        sys.exit(1)

    database_id = sys.argv[1]
    token = sys.argv[2]

    # ✅ Pass token explicitly
    # print(token, 'hi')
    notion = NotionConnector(token)

    try:
        data = notion.extract_data(database_id)
        print(json.dumps(data), flush=True)
    except Exception as e:
        print(json.dumps({"error": f"Failed to fetch data: {str(e)}"}), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

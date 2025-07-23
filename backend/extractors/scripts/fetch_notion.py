import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))
from connectors.notion_connector import NotionConnector

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Missing Notion database_id"}), flush=True)
        sys.exit(1)

    database_id = sys.argv[1]
    notion = NotionConnector()

    try:
        data = notion.extract_data(database_id)
        print(json.dumps(data), flush=True)
    except Exception as e:
        print(json.dumps({"error": f"Failed to fetch data: {str(e)}"}), flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

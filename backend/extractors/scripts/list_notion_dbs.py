import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))

from connectors.notion_connector import NotionConnector

if __name__ == "__main__":
    notion = NotionConnector()
    if not notion.is_token_valid():
        print(json.dumps({"error": "Invalid token"}), flush=True)
        sys.exit(1)

    objects = notion.get_available_objects()
    print(json.dumps(objects), flush=True)

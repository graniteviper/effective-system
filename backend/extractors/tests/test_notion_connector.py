import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))

from connectors.notion_connector import NotionConnector

# Replace this with the actual database ID you got from list_notion_dbs.py
DATABASE_ID = "2392a826-7e82-803b-a6c3-d950e5d43177"

notion = NotionConnector()

if not notion.is_token_valid():
    print("❌ Invalid token")
    sys.exit(1)

data = notion.extract_data(DATABASE_ID)

print(f"🧾 Records fetched: {len(data)}")
print("📄 Sample record:")
print(data[0] if data else "No data found")

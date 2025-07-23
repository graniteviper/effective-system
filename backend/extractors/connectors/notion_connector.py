import requests
import os
from base.api_connector import APIConnector
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env.local"))


NOTION_API_URL = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

class NotionConnector(APIConnector):
    def __init__(self, token=None):
        self.token = token or os.getenv("NOTION_TOKEN")
        # print(self.token)
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json"
        }


    def authenticate(self):
        pass  # Notion uses static token, no auth flow needed

    def refresh_token(self):
        pass  # Notion tokens don’t expire

    def get_object_fields(self, object_id):
        return []  # Optional: implement if needed


    def is_token_valid(self):
        url = f"{NOTION_API_URL}/users/me"
        res = requests.get(url, headers=self.headers)
        return res.status_code == 200

    def get_available_objects(self):
        url = f"{NOTION_API_URL}/search"
        body = {
            "filter": {"property": "object", "value": "database"}
        }
        res = requests.post(url, headers=self.headers, json=body)
        if res.status_code != 200:
            raise Exception(f"Failed to list databases: {res.text}")
        results = res.json().get("results", [])
        return [{"id": db["id"], "title": self._get_title(db)} for db in results]

    def _get_title(self, db):
        title = db.get("title", [])
        if title and isinstance(title, list) and "plain_text" in title[0]:
            return title[0]["plain_text"]
        return db.get("id")

    def extract_data(self, object_id, fields=None):
        url = f"{NOTION_API_URL}/databases/{object_id}/query"
        res = requests.post(url, headers=self.headers)
        if res.status_code != 200:
            raise Exception(f"Failed to extract data: {res.text}")

        results = res.json().get("results", [])
        data = [self._flatten(page) for page in results]
        return data

    def _flatten(self, page):
        flat = {"id": page["id"]}
        props = page.get("properties", {})
        for key, val in props.items():
            flat[key] = self._extract_value(val)
        return flat

    def _extract_value(self, val):
        if val["type"] == "title":
            return val["title"][0]["plain_text"] if val["title"] else ""
        elif val["type"] == "rich_text":
            return val["rich_text"][0]["plain_text"] if val["rich_text"] else ""
        elif val["type"] == "select":
            return val["select"]["name"] if val["select"] else ""
        elif val["type"] == "multi_select":
            return ", ".join(opt["name"] for opt in val["multi_select"])
        elif val["type"] == "number":
            return val["number"]
        elif val["type"] == "checkbox":
            return val["checkbox"]
        elif val["type"] == "date":
            return val["date"]["start"] if val["date"] else ""
        elif val["type"] == "people":
            return ", ".join(p.get("name", "") for p in val["people"])
        else:
            return str(val[val["type"]]) if val.get(val["type"]) else ""

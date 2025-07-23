import requests
from base.api_connector import APIConnector

class GitHubConnector(APIConnector):
    def __init__(self, token):
        self.token = token
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json"
        }

    def authenticate(self):
        # GitHub PAT doesn't need an auth step — just validate
        return self.is_token_valid()

    def is_token_valid(self):
        response = requests.get("https://api.github.com/user", headers=self.headers)
        return response.status_code == 200

    def get_available_objects(self):
        """List user repositories"""
        url = "https://api.github.com/user/repos"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch repos: {response.text}")
        repos = response.json()
        return [{"name": repo["full_name"]} for repo in repos]

    def get_object_fields(self, object_name):
        # Optional: return static or dummy field metadata
        return [
            {"name": "title", "type": "string"},
            {"name": "state", "type": "string"},
            {"name": "created_at", "type": "datetime"}
        ]

    def extract_data(self, object_name, fields=None, params=None):
        """Fetch issues for the selected repo"""
        url = f"https://api.github.com/repos/{object_name}/issues"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch issues: {response.text}")
        return response.json()

    def refresh_token(self):
        # GitHub PAT doesn't support refresh
        return None

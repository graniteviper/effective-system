from backend.extractors.connectors.github_connector import GitHubConnector
from backend.extractors.storage.postgres_storage import PostgresStorageManager
import os
from dotenv import load_dotenv

dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.env.local"))
load_dotenv(dotenv_path)

if __name__ == "__main__":
    token = os.getenv("GITHUB_PAT")
    print("🔐 Loaded token:", token[:6] + "..." if token else "None")
    gh = GitHubConnector(token)

    repos = gh.get_available_objects()
    print("🔍 Using repo:", repos[0]['name'])
    issues = gh.extract_data(repos[0]['name'])
    print("🐛 Number of issues fetched:", len(issues))
    if issues:
        print("📋 Sample issue:", issues[0])


    storage = PostgresStorageManager()
    storage.save(issues, "github_issues")
    storage.close()

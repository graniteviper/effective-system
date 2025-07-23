import sys
import json
import os

# 🔧 Fix path: point directly to backend/extractors
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))

from connectors.github_connector import GitHubConnector

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Token argument missing"}))
        sys.exit(1)

    token = sys.argv[1]
    connector = GitHubConnector(token)

    if not connector.is_token_valid():
        print(json.dumps({"error": "Invalid token"}))
        sys.exit(1)

    repos = connector.get_available_objects()
    print(json.dumps({"repos": repos}))

if __name__ == "__main__":
    main()

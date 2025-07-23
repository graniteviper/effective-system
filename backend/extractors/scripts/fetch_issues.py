import sys
import json
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "extractors")))

from connectors.github_connector import GitHubConnector

def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Missing arguments: token and repo"}))
        sys.exit(1)

    token = sys.argv[1]
    repo_name = sys.argv[2]

    connector = GitHubConnector(token)

    if not connector.is_token_valid():
        print(json.dumps({"error": "Invalid token"}))
        sys.exit(1)

    try:
        issues = connector.extract_data(repo_name)
        print(json.dumps({"issues": issues}))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()

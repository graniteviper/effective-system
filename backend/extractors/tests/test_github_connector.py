from backend.extractors.connectors.github_connector import GitHubConnector
import os

if __name__ == "__main__":
    token = os.getenv("GITHUB_PAT")
    gh = GitHubConnector(token)

    print("🔹 Token valid:", gh.is_token_valid())

    print("\n🔹 Repositories:")
    repos = gh.get_available_objects()
    for repo in repos[:3]:
        print("-", repo["name"])

    if repos:
        print("\n🔹 Issues for:", repos[0]['name'])
        issues = gh.extract_data(repos[0]['name'])
        for issue in issues[:3]:
            print("-", issue.get("title"))

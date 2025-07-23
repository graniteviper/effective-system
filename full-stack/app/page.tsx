"use client";

import { useState } from "react";

export default function GitHubExtractorPage() {
  const [source, setSource] = useState<"github" | "notion">("github");
  const [token, setToken] = useState("");
  const [repos, setRepos] = useState<{ name: string }[]>([]);
  const [issuesByRepo, setIssuesByRepo] = useState<Record<string, any[]>>({});
  const [loadingRepo, setLoadingRepo] = useState(false);
  const [loadingIssues, setLoadingIssues] = useState<Record<string, boolean>>(
    {}
  );
  const [savingIssues, setSavingIssues] = useState<Record<string, boolean>>({});
  const [notionToken, setNotionToken] = useState("");
  const [notionDBs, setNotionDBs] = useState<{ id: string; title: string }[]>(
    []
  );
  const [savingDB, setSavingDB] = useState<Record<string, boolean>>({});
  const [notionDataByDB, setNotionDataByDB] = useState<Record<string, any[]>>(
    {}
  );
  const [loadingNotionDBs, setLoadingNotionDBs] = useState(false);
  const [loadingNotionData, setLoadingNotionData] = useState<
    Record<string, boolean>
  >({});

  const fetchRepos = async () => {
    if (!token) return alert("Please enter your GitHub PAT.");
    setLoadingRepo(true);
    try {
      const res = await fetch("/api/extractors/github/objects", {
        method: "GET",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });
      const data = await res.json();
      if (data.repos.length >= 0) {
        setRepos(data.repos);
      }
    } catch (err) {
      setRepos([]);
      alert(
        "Failed to fetch repositories. Make sure the PAT is correct and has the required permissions."
      );
    }
    setLoadingRepo(false);
  };

  const fetchIssues = async (repo: string) => {
    setLoadingIssues((prev) => ({ ...prev, [repo]: true }));
    try {
      const res = await fetch("/api/extractors/github/data", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ repo, token }),
      });
      const data = await res.json();
      setIssuesByRepo((prev) => ({ ...prev, [repo]: data.issues }));
    } catch (err) {
      alert("Failed to fetch issues");
    }
    setLoadingIssues((prev) => ({ ...prev, [repo]: false }));
  };

  const saveIssues = async (repo: string) => {
    const issues = issuesByRepo[repo];
    if (!issues || issues.length === 0) return alert("No issues to save");
    setSavingIssues((prev) => ({ ...prev, [repo]: true }));
    try {
      const res = await fetch("/api/load", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data: issues, table_name: "github_issues" }),
      });
      const result = await res.json();
      alert(`✅ Saved ${result.records_saved} issues to DB.`);
    } catch (err) {
      alert("Failed to save issues to DB");
    }
    setSavingIssues((prev) => ({ ...prev, [repo]: false }));
  };

  const fetchDatabases = async () => {
    if (!notionToken) return alert("Please enter your Notion token.");
    setLoadingNotionDBs(true);
    try {
      const res = await fetch("/api/extractors/notion/objects", {
        method: "GET",
        headers: {
          Authorization: `Bearer ${notionToken}`,
        },
      });
      const data = await res.json();
      console.log(data);
      setNotionDBs(data.databases || []);
    } catch (err) {
      alert("Failed to fetch databases");
    }
    setLoadingNotionDBs(false);
  };

  const saveDatabase = async (id: string) => {
    setSavingDB((prev) => ({ ...prev, [id]: true }));
    try {
      const res = await fetch("/api/extractors/notion/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ database_id: id, table_name: "notion_data",token: notionToken }),
      });
      const result = await res.json();
      alert(`✅ Saved to DB`);
    } catch (e) {
      alert("Failed to save to DB");
    }
    setSavingDB((prev) => ({ ...prev, [id]: false }));
  };

  const fetchSavedNotionData = async (dbId: string) => {
    setLoadingNotionData((prev) => ({ ...prev, [dbId]: true }));
    try {
      const res = await fetch(
        `/api/extractors/notion/data?database_id=${dbId}`
      , {
        headers: {
          "Token": `${notionToken}`
        }
      });
      const data = await res.json();
      console.log(data);
      
      setNotionDataByDB((prev) => ({ ...prev, [dbId]: data.records || [] }));
    } catch (e) {
      alert("Failed to fetch saved data from DB");
    }
    setLoadingNotionData((prev) => ({ ...prev, [dbId]: false }));
  };

  return (
    <main className="max-w-2xl mx-auto p-6 bg-white text-black min-h-screen">
      <div className="border-2 border-blue-500 py-6 px-10 rounded-xl">
        <h1 className="text-2xl font-bold mb-6 text-center">
          📊 Data Extractor
        </h1>

        <label className="block mb-2 font-semibold">Select Source</label>
        <select
          value={source}
          onChange={(e) => setSource(e.target.value as "github" | "notion")}
          className="border border-gray-300 px-3 py-2 mb-4 w-full rounded"
        >
          <option value="github">GitHub</option>
          <option value="notion">Notion</option>
        </select>

        {source === "notion" && (
          <div className="space-y-4">
            <input
              type="text"
              placeholder="Enter your Notion Integration Token"
              className="border border-gray-300 px-3 py-2 w-full rounded"
              value={notionToken}
              onChange={(e) => setNotionToken(e.target.value)}
            />
            <button
              onClick={fetchDatabases}
              className="bg-indigo-600 hover:bg-indigo-700 text-white px-4 py-2 rounded"
              disabled={loadingNotionDBs}
            >
              {loadingNotionDBs ? "Loading..." : "Show Databases"}
            </button>

            {notionDBs.map((db) => (
              <div
                key={db.id}
                className="border border-gray-300 p-4 rounded shadow-sm bg-gray-50"
              >
                <div className="font-medium mb-2">📚 {db.title}</div>
                <div className="flex gap-2 mb-2">
                  <button
                    onClick={() => saveDatabase(db.id)}
                    className="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded"
                    disabled={savingDB[db.id]}
                  >
                    {savingDB[db.id] ? "Saving..." : "Save to DB"}
                  </button>
                  <button
                    onClick={() => fetchSavedNotionData(db.id)}
                    className="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded"
                    disabled={loadingNotionData[db.id]}
                  >
                    {loadingNotionData[db.id] ? "Extracting..." : "Extract"}
                  </button>
                </div>
                {notionDataByDB[db.id] &&
                  (notionDataByDB[db.id].length === 0 ? (
                    <p className="text-sm text-gray-500 italic">
                      No entries found.
                    </p>
                  ) : (
                    <ul className="list-disc list-inside text-sm text-gray-800 space-y-1">
                      {notionDataByDB[db.id].map((item, i) => (
                        <li key={i}>{item["Task name"] || `Row ${i + 1}`}</li>
                      ))}
                    </ul>
                  ))}
              </div>
            ))}
          </div>
        )}

        {source === "github" && (
          <div className="space-y-4">
            <input
              type="text"
              placeholder="Enter your GitHub PAT"
              className="border border-gray-300 px-3 py-2 w-full rounded"
              value={token}
              onChange={(e) => setToken(e.target.value)}
            />
            <button
              onClick={fetchRepos}
              className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded"
              disabled={loadingRepo}
            >
              {loadingRepo ? "Fetching..." : "Fetch Repositories"}
            </button>

            {repos.map((repo) => (
              <div
                key={repo.name}
                className="border border-gray-300 p-4 rounded shadow-sm bg-gray-50"
              >
                <div className="font-medium mb-2">📦 {repo.name}</div>
                <div className="flex gap-2">
                  <button
                    onClick={() => fetchIssues(repo.name)}
                    className="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded"
                    disabled={loadingIssues[repo.name]}
                  >
                    {loadingIssues[repo.name] ? "Extracting..." : "Extract Issues"}
                  </button>
                  <button
                    onClick={() => saveIssues(repo.name)}
                    className="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded"
                    disabled={savingIssues[repo.name]}
                  >
                    {savingIssues[repo.name]
                      ? "Saving..."
                      : "Save Issues to DB"}
                  </button>
                </div>
                {issuesByRepo[repo.name] &&
                  (issuesByRepo[repo.name].length === 0 ? (
                    <p className="mt-3 text-sm text-gray-500 italic">
                      No issues found.
                    </p>
                  ) : (
                    <ul className="mt-3 list-disc list-inside text-sm text-gray-800">
                      {issuesByRepo[repo.name].map((issue, idx) => (
                        <li key={idx}>{issue.title}</li>
                      ))}
                    </ul>
                  ))}
              </div>
            ))}
          </div>
        )}
      </div>
    </main>
  );
}

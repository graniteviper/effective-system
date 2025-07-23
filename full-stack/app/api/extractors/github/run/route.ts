import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function POST(req: NextRequest) {
  const { token, repo, table_name = "github_issues" } = await req.json();

  if (!token || !repo) {
    return NextResponse.json({ error: "Missing token or repo" }, { status: 400 });
  }

  // Step 1: Run fetch_issues.py
  const extractScript = path.join(process.cwd(), "../backend/extractors/scripts/fetch_issues.py");

  const issues: any = await new Promise((resolve) => {
    const python = spawn("python", [extractScript, token, repo]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    python.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    python.on("close", (code) => {
      if (code !== 0) {
        return resolve({ error: stderr || "Failed to extract data" });
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ error: "Invalid JSON from extract script", raw: stdout });
      }
    });
  });

  if ("error" in issues) {
    return NextResponse.json({ step: "extract", ...issues }, { status: 500 });
  }

  // Step 2: Run save_to_postgres.py and send issues to stdin
  const loadScript = path.join(process.cwd(), "../backend/extractors/scripts/save_to_postgres.py");

  return new Promise((resolve) => {
    const python = spawn("python", [loadScript, table_name]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    python.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    python.on("close", (code) => {
      if (code !== 0) {
        return resolve(
          NextResponse.json({ step: "load", error: stderr || "Failed to save data" }, { status: 500 })
        );
      }
      try {
        const result = JSON.parse(stdout);
        return resolve(NextResponse.json({
          status: "success",
          repo,
          table: table_name,
          records_saved: result.records_saved,
        }));
      } catch {
        return resolve(
          NextResponse.json({ step: "load", error: "Invalid JSON from save script", raw: stdout }, { status: 500 })
        );
      }
    });

    python.stdin.write(JSON.stringify(issues.issues));
    python.stdin.end();
  });
}

import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function POST(req: NextRequest) {
  const { database_id, table_name = "notion_data" } = await req.json();

  if (!database_id) {
    return NextResponse.json({ error: "Missing Notion database_id" }, { status: 400 });
  }

  // Step 1: Extract data via fetch_notion.py
  const extractScript = path.join(process.cwd(), "../backend/extractors/scripts/fetch_notion.py");

  const records: any = await new Promise((resolve) => {
    const python = spawn("python", [extractScript, database_id]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    python.stderr.on("data", (chunk) => (stderr += chunk.toString()));

    python.on("close", () => {
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ error: "Invalid JSON from Notion fetch", raw: stdout });
      }
    });
  });

  if ("error" in records) {
    return NextResponse.json({ step: "extract", ...records }, { status: 500 });
  }

  // Step 2: Save to Postgres
  const loadScript = path.join(process.cwd(), "../backend/extractors/scripts/save_to_postgres.py");

  return new Promise((resolve) => {
    const python = spawn("python", [loadScript, table_name]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    python.stderr.on("data", (chunk) => (stderr += chunk.toString()));

    python.on("close", () => {
      try {
        const result = JSON.parse(stdout);
        return resolve(NextResponse.json({
          status: "success",
          database_id,
          records_saved: result.records_saved
        }));
      } catch {
        return resolve(NextResponse.json({ step: "load", error: "Invalid JSON from Postgres", raw: stdout }, { status: 500 }));
      }
    });

    python.stdin.write(JSON.stringify(records));
    python.stdin.end();
  });
}

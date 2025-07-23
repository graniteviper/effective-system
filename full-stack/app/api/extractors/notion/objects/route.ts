import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function GET(req: NextRequest) {
  const token = req.headers.get("authorization")?.replace("Bearer ", "");
  if (!token) {
    return NextResponse.json({ error: "Missing Notion token" }, { status: 400 });
  }

  const scriptPath = path.join(process.cwd(), "../backend/extractors/scripts/list_notion_dbs.py");

  return new Promise((resolve) => {
    const python = spawn("python", [scriptPath], {
      env: {
        ...process.env,
        NOTION_TOKEN: token, // passed as env var
      },
    });

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    python.stderr.on("data", (chunk) => (stderr += chunk.toString()));

    python.on("close", () => {
      try {
        const data = JSON.parse(stdout);
        resolve(NextResponse.json({ databases: data }));
      } catch {
        resolve(
          NextResponse.json(
            { error: "Failed to parse Notion response", raw: stdout, stderr },
            { status: 500 }
          )
        );
      }
    });
  });
}

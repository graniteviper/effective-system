import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function GET(req: NextRequest) {
  const dbId = req.nextUrl.searchParams.get("database_id");

  if (!dbId) {
    return NextResponse.json(
      { error: "Missing Notion token or database_id" },
      { status: 400 }
    );
  }

  const scriptPath = path.join(
    process.cwd(),
    "../backend/extractors/scripts/fetch_notion.py"
  );

  return new Promise((resolve) => {
    const python = spawn("python", [scriptPath, dbId], {
      env: {
        ...process.env,
      },
    });

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    python.stderr.on("data", (chunk) => (stderr += chunk.toString()));

    python.on("close", () => {
      try {
        const parsed = JSON.parse(stdout);
        resolve(NextResponse.json({ records: parsed }));
      } catch {
        resolve(
          NextResponse.json(
            {
              error: "Failed to parse Notion data",
              raw: stdout,
              stderr,
            },
            { status: 500 }
          )
        );
      }
    });
  });
}

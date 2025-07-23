import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function POST(req: NextRequest) {
  const { token, repo } = await req.json();

  if (!token || !repo) {
    return NextResponse.json({ error: "Missing token or repo" }, { status: 400 });
  }

  const scriptPath = path.join(process.cwd(), "../backend/extractors/scripts/fetch_issues.py");

  return new Promise((resolve) => {
    const python = spawn("python", [scriptPath, token, repo]);

    let data = "";
    let error = "";

    python.stdout.on("data", (chunk) => {
      data += chunk.toString();
    });

    python.stderr.on("data", (chunk) => {
      error += chunk.toString();
    });

    python.on("close", (code) => {
      if (code !== 0) {
        return resolve(NextResponse.json({ error: error || "Python error" }, { status: 500 }));
      }

      try {
        const parsed = JSON.parse(data);
        return resolve(NextResponse.json(parsed));
      } catch (err) {
        return resolve(NextResponse.json({ error: "Invalid JSON", raw: data }, { status: 500 }));
      }
    });
  });
}

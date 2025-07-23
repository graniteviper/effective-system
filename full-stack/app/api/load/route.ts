import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function POST(req: NextRequest) {
  const { data, table_name } = await req.json();

  if (!data || !table_name) {
    return NextResponse.json(
      { error: "Missing data or table_name" },
      { status: 400 }
    );
  }

  const scriptPath = path.join(
    process.cwd(),
    "../backend/extractors/scripts/save_to_postgres.py"
  );

  return new Promise((resolve) => {
    const python = spawn("python", [scriptPath, table_name]);

    let stdout = "";
    let stderr = "";

    python.stdout.on("data", (chunk) => {
      const out = chunk.toString();
      console.log("🟢 Python stdout:", out);
      stdout += out;
    });

    python.stderr.on("data", (chunk) => {
      const err = chunk.toString();
      console.error("🔴 Python stderr:", err);
      stderr += err;
    });

    // ✅ Send data to stdin
    python.stdin.write(JSON.stringify(data));
    python.stdin.end();

    python.on("close", (code) => {
      if (code !== 0) {
        return resolve(
          NextResponse.json(
            { error: stderr || "Python error" },
            { status: 500 }
          )
        );
      }

      try {
        const parsed = JSON.parse(stdout);
        return resolve(NextResponse.json(parsed));
      } catch (err) {
        return resolve(
          NextResponse.json(
            { error: "Invalid JSON from Python", raw: stdout },
            { status: 500 }
          )
        );
      }
    });
  });
}

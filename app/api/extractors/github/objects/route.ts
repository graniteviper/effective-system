// app/api/extractors/github/objects/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { spawn } from 'child_process';
import path from 'path';

export async function GET(req: NextRequest) {
  const token = req.headers.get("authorization")?.replace("Bearer ", "");

  if (!token) {
    return NextResponse.json({ error: "Missing GitHub token" }, { status: 400 });
  }

  const scriptPath = path.join(process.cwd(), 'backend/extractors/scripts/list_repos.py');

  return new Promise((resolve) => {
    const python = spawn('python', [scriptPath, token]);

    let data = '';
    let error = '';

    python.stdout.on('data', (chunk) => {
      data += chunk.toString();
    });

    python.stderr.on('data', (chunk) => {
      error += chunk.toString();
    });

    python.on('close', (code) => {
      if (code !== 0) {
        return resolve(NextResponse.json({ error }, { status: 500 }));
      }

      try {
        const parsed = JSON.parse(data);
        return resolve(NextResponse.json(parsed));
      } catch (err) {
        return resolve(NextResponse.json({ error: "Invalid JSON from Python" }, { status: 500 }));
      }
    });
  });
}

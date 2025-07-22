// app/api/websocket-proxy/route.ts
import { NextRequest, NextResponse } from 'next/server'
import httpProxy from 'http-proxy'
import { IncomingMessage, ServerResponse } from 'http'
import { unknown } from 'zod'
import { None } from 'vega'

const proxy = httpProxy.createProxyServer()
const FLASK_SERVER_URL = process.env.NEXT_PUBLIC_NOTEBOOK_VM_URL

export const dynamic = 'force-dynamic'
export const runtime = 'nodejs'

function requestToIncomingMessage(req: NextRequest): IncomingMessage {
  const { method, headers, url } = req
  const incomingMessage = new IncomingMessage(null as any)
  Object.assign(incomingMessage, { method, headers, url })
  return incomingMessage
}

async function handleProxy(req: NextRequest) {
  const url = new URL(req.url)
  url.pathname = url.pathname.replace('/api/socket-proxy', '')

  return new Promise<NextResponse>((resolve, reject) => {
    proxy.web(
      requestToIncomingMessage(req),
      {} as ServerResponse,
      { target: FLASK_SERVER_URL, ws: true, ignorePath: true, secure: false },
      //@ts-ignore
      err => {
        if (err) {
          console.error('Proxy error:', err)
          resolve(NextResponse.json({ error: 'Proxy error' }, { status: 500 }))
        }
      }
    )
 //@ts-ignore
    proxy.once('proxyRes', (proxyRes, req, res) => {
      let body = ''
       //@ts-ignore
      proxyRes.on('data', chunk => {
        body += chunk
      })
      proxyRes.on('end', () => {
        resolve(
          new NextResponse(body, {
            status: proxyRes.statusCode,
            headers: proxyRes.headers as HeadersInit,
          })
        )
      })
    })
  })
}

export async function GET(req: NextRequest) {
  return handleProxy(req)
}

export async function POST(req: NextRequest) {
  return handleProxy(req)
}
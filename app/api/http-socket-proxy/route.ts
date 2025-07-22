import { NextResponse } from 'next/server'
import axios from 'axios'

export async function POST(req: Request) {
  try {
    const body = await req.json()
    const { event, data } = body

    const response = await axios.post(`${process.env.NEXT_PUBLIC_NOTEBOOK_VM_URL}/socket.io/`, {
      event,
      data
    }, {
      headers: {
        'Content-Type': 'application/json',
      }
    })

    return NextResponse.json(response.data)
  } catch (error: any) {
    console.error('Error in WebSocket proxy request:', error)
    if (error.response) {
      return NextResponse.json(error.response.data, {
        status: error.response.status,
      })
    } else {
      return new NextResponse('Internal server error', { status: 500 })
    }
  }
}
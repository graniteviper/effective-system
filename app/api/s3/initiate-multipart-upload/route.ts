import { NextRequest, NextResponse } from 'next/server'
import { S3Client, CreateMultipartUploadCommand } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
})

export async function POST(req: NextRequest) {
  try {
    const { fileName, userId } = await req.json()

    if (!fileName || !userId) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
    }

    const command = new CreateMultipartUploadCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: `${userId}/${fileName}`,
    })

    const { UploadId } = await s3Client.send(command)

    return NextResponse.json({ uploadId: UploadId })
  } catch (error) {
    console.error('Error initiating multipart upload:', error)
    return NextResponse.json({ error: 'Error initiating multipart upload' }, { status: 500 })
  }
}
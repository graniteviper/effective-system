import { NextRequest, NextResponse } from 'next/server'
import { S3Client, CompleteMultipartUploadCommand } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
})

export async function POST(req: NextRequest) {
  try {
    const { uploadId, parts, fileName, userId } = await req.json()

    if (!uploadId || !parts || !fileName || !userId) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
    }

    const command = new CompleteMultipartUploadCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: `${userId}/${fileName}`,
      UploadId: uploadId,
      MultipartUpload: { Parts: parts },
    })

    const response = await s3Client.send(command)

    return NextResponse.json({ response: 'success completion' })
  } catch (error) {
    console.error('Error completing multipart upload:', error)
    return NextResponse.json({ error: 'Error completing multipart upload' }, { status: 500 })
  }
}
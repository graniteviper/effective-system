import { NextRequest, NextResponse } from 'next/server'
import { S3Client, UploadPartCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
})

export async function POST(req: NextRequest) {
  try {
    const { fileName, userId, uploadId, partNumber } = await req.json()

    if (!fileName || !userId || !uploadId || !partNumber) {
      return NextResponse.json({ error: 'Missing required fields' }, { status: 400 })
    }

    const command = new UploadPartCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME,
      Key: `${userId}/${fileName}`,
      UploadId: uploadId,
      PartNumber: partNumber,
    })

    const url = await getSignedUrl(s3Client, command, { expiresIn: 3600 })

    return NextResponse.json({ url })
  } catch (error) {
    console.error('Error generating pre-signed URL:', error)
    return NextResponse.json({ error: 'Error generating pre-signed URL' }, { status: 500 })
  }
}
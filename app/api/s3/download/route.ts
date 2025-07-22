import { NextRequest, NextResponse } from 'next/server'
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { getAutonomisUser } from '@/lib/user'

const s3Client = new S3Client({
  region: process.env.AWS_REGION!,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
})

export async function POST(req: NextRequest) {
  try {
    // Verify user authentication
    const { data: user, error: userError } = await getAutonomisUser()
    if (!user || userError) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { key } = await req.json()

    if (!key) {
      return NextResponse.json({ error: 'File key is required' }, { status: 400 })
    }

    // Generate a presigned URL for downloading the file
    const command = new GetObjectCommand({
      Bucket: process.env.AWS_S3_BUCKET_NAME!,
      Key: key,
    })

    const signedUrl = await getSignedUrl(s3Client, command, { 
      expiresIn: 3600 // URL expires in 1 hour
    })

    return NextResponse.json({
      downloadUrl: signedUrl,
      fileName: key.split('/').pop() || key
    })

  } catch (error) {
    console.error('Error generating download URL:', error)
    return NextResponse.json(
      { error: 'Failed to generate download URL', details: (error as Error).message },
      { status: 500 }
    )
  }
} 
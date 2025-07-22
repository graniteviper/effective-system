import { NextRequest, NextResponse } from 'next/server'
import {
    PutObjectCommand,
    S3Client,
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'

type SinglePresignedUrl = {
    fileName: string
    userId: string
}

export async function POST(req: NextRequest) {
    try {
        const { fileName, userId }: SinglePresignedUrl = await req.json()

        if (!fileName || !userId) {
            return NextResponse.json(
                { error: 'Missing required fields' },
                { status: 400 }
            )
        }

        const key = `${userId}/${fileName}`

        const createPresignedUrlWithClient = () => {
            const client = new S3Client({
                region: process.env.AWS_REGION,
                credentials: {
                    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
                    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
                },
            })

            const command = new PutObjectCommand({
                Bucket: process.env.AWS_S3_BUCKET_NAME,
                Key: key,
                ContentType: 'application/octet-stream', // Specify correct Content-Type
            })
            return getSignedUrl(client, command, { expiresIn: 3600 })
        }

        const url = await createPresignedUrlWithClient()
        console.log('Generated presigned URL:', url)

        return NextResponse.json({ data: url, status: 200 })
    } catch (err) {
        console.error('Error generating presigned URL:', err)
        return NextResponse.json(
            { error: 'Error generating presigned URL' },
            { status: 500 }
        )
    }
}

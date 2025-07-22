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

export async function POST(req: NextRequest, res: NextResponse) {
    try {
        const { uploadId, partNumber, fileName, userId } = await req.json()

        if (!uploadId || !partNumber || !fileName || !userId) {
            return NextResponse.json(
                { error: 'Missing required fields' },
                { status: 400 }
            )
        }

        const key = `${userId}/${fileName}`

        const command = new UploadPartCommand({
            Bucket: process.env.AWS_S3_BUCKET_NAME,
            Key: key,
            UploadId: uploadId,
            PartNumber: partNumber,
        })

        const presignedUrl = await getSignedUrl(s3Client, command, {
            expiresIn: 3600,
        })

        console.log('Generated presigned URL:', presignedUrl)

        return NextResponse.json({
            data: presignedUrl,
            message: 'Successfully generated presigned URL',
            status: 200, // Explicit success status
        })
    } catch (error) {
        console.error('Error generating presigned URL:', error)
        return NextResponse.json(
            { error: 'Error generating presigned URL' },
            { status: 500 }
        )
    }
}

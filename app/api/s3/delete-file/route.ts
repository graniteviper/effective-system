import { NextRequest, NextResponse } from 'next/server'
import { DeleteObjectCommand, S3Client } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
	region: process.env.AWS_REGION!,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
	},
})

export async function POST(req: NextRequest) {
	try {
		const { fileKey } = await req.json()

		if (!fileKey) {
			return NextResponse.json(
				{ error: 'Missing required field: fileKey' },
				{ status: 400 }
			)
		}

		const params = {
			Bucket: process.env.AWS_S3_BUCKET_NAME!,
			Key: fileKey,
		}

		// Execute the command to delete the object
		const command = new DeleteObjectCommand(params)
		await s3Client.send(command)

		return NextResponse.json({
			message: 'File deleted successfully',
		})
	} catch (error) {
		console.error('Error deleting S3 file:', error)
		return NextResponse.json({ error: error }, { status: 500 })
	}
}

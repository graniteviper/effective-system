import { NextRequest, NextResponse } from 'next/server'
import { ListObjectsCommand, S3Client } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
	region: process.env.AWS_REGION!,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
	},
})

export async function POST(req: NextRequest) {
	try {
		const { userId } = await req.json()

		if (!userId) {
			return NextResponse.json(
				{ error: 'Missing required field: userId' },
				{ status: 400 }
			)
		}

		const params = {
			Bucket: process.env.AWS_S3_BUCKET_NAME!,
			Prefix: `${userId}`,
		}

		// Execute the command to list objects
		const command = new ListObjectsCommand(params)
		const response = await s3Client.send(command)
		// console.log('the response is', response)

		const files = response.Contents || []

		return NextResponse.json({
			files: files,
			message: 'Files Fetched Successfully',
		})
	} catch (error) {
		console.error('Error listing S3 files:', error)
		return NextResponse.json({ error: error }, { status: 500 })
	}
}

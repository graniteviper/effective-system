import { getAutonomisUser } from '@/lib/user'
import {
	S3Client,
	CopyObjectCommand,
	DeleteObjectCommand,
} from '@aws-sdk/client-s3'
import { NextRequest, NextResponse } from 'next/server'

const s3 = new S3Client({
	region: process.env.AWS_REGION,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
	},
})

export async function POST(req: NextRequest) {
	try {
		const { data, error } = await getAutonomisUser()

		if (!data) {
			return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
		}

		const body = await req.json()

		const { oldFile, newFile } = body

		const bucketName = process.env.AWS_S3_BUCKET_NAME

		const copyParams = {
			Bucket: bucketName,
			CopySource: `/${bucketName}/${oldFile}`,
			Key: newFile,
		}

		const copyCommand = new CopyObjectCommand(copyParams)
		const copyResponse = await s3.send(copyCommand)

		const deleteParams = {
			Bucket: bucketName,
			Key: oldFile,
		}

		const deleteCommand = new DeleteObjectCommand(deleteParams)
		const deleteResponse = await s3.send(deleteCommand)

		return Response.json(
			{ message: 'File renamed successfully' },
			{ status: 201 }
		)
	} catch (error) {
		console.log(error)
		return Response.json({ error }, { status: 500 })
	}
}

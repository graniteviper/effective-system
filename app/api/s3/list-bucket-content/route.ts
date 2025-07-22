import { NextRequest, NextResponse } from 'next/server'
import { ListObjectsCommand, S3Client } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
	region: process.env.AWS_REGION!,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
	},
})

async function listFilesInFolder(
	bucket: string,
	prefix?: string
): Promise<any> {
	const params = {
		Bucket: bucket,
		Prefix: prefix,
		Delimiter: '/',
	}

	const command = new ListObjectsCommand(params)
	const response = await s3Client.send(command)

	const files = response.Contents || []
	const commonPrefixes = response.CommonPrefixes || []

	const folderStructure: any = {
		folder: prefix,
		files: files.map(file => file.Key),
		subfolders: [],
	}

	// Recursively fetch files in each subfolder
	for (const folder of commonPrefixes) {
		const nestedFolder = await listFilesInFolder(bucket, folder.Prefix)
		folderStructure.subfolders.push(nestedFolder)
	}

	return folderStructure
}

export async function POST(req: NextRequest) {
	try {
		const { connection_string } = await req.json()

		if (!connection_string) {
			return NextResponse.json(
				{ error: 'Missing required field: connection_string' },
				{ status: 400 }
			)
		}

		const bucket = connection_string.bucket
		const organizedFiles = await listFilesInFolder(bucket, '')

		return NextResponse.json({
			organizedFiles,
			message: 'Files Fetched Successfully',
		})
	} catch (error) {
		console.error('Error fetching bucket data:', error)
		return NextResponse.json(
			{ error: (error as Error).message },
			{ status: 500 }
		)
	}
}

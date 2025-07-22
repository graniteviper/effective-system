// pages/api/s3-files.js
import { getAutonomisUser } from '@/lib/user'
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3'
import { NextRequest, NextResponse } from 'next/server'

export async function POST(req: NextRequest, res: NextResponse) {
	// Only allow GET requests

	const { data, error } = await getAutonomisUser()

	if (!data) {
		return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
	}

	const body = await req.json()
	const {
		AWS_REGION,
		AWS_ACCESS_KEY_ID,
		AWS_SECRET_ACCESS_KEY,
		AWS_BUCKET,
		prefix,
	} = body

	// Initialize S3 client outside of handler to reuse across requests
	const s3Client = new S3Client({
		region: AWS_REGION!,
		credentials: {
			accessKeyId: AWS_ACCESS_KEY_ID!,
			secretAccessKey: AWS_SECRET_ACCESS_KEY!,
		},
	})

	try {
		// Configure the S3 list command
		const command = new ListObjectsV2Command({
			Bucket: AWS_BUCKET!,
			Prefix: prefix || '', // Get the current folder path from query
			Delimiter: '/', // Use delimiter to simulate folder structure
		})

		// Execute the command
		const response = await s3Client.send(command)

		// Process files (Contents)
		const files =
			response.Contents?.map(item => ({
				name: item.Key,
				size: item.Size,
				lastModified: item.LastModified,
				type: 'file',
			})) || []

		// Process folders (CommonPrefixes)
		const folders =
			response.CommonPrefixes?.map(prefix => ({
				name: prefix.Prefix,
				type: 'folder',
			})) || []

		console.log({ files, folders })

		return NextResponse.json({
			files: files.filter(file => file.name !== prefix), // Remove current folder from list
			folders,
		})
	} catch (error) {
		console.error('Error listing S3 objects:', error)
		return NextResponse.json(
			{ error: 'Cannot list s3 objects' },
			{ status: 500 }
		)
	}
}

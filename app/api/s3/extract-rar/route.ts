import { getAutonomisUser } from '@/lib/user'
import AWS from 'aws-sdk'
import fs from 'fs/promises'
import { NextRequest, NextResponse } from 'next/server'
import os from 'os'
import path from 'path'
//@ts-ignore
import { unrar } from 'unrar-js'
import { v4 as uuidv4 } from 'uuid'

export async function POST(req: NextRequest) {
	let body
	try {
		body = await req.json()
	} catch (error) {
		return NextResponse.json(
			{ message: 'Invalid request body' },
			{ status: 400 }
		)
	}

	const { fileKey, userId } = body

	// Initialize S3
	const s3 = new AWS.S3({
		accessKeyId: process.env.AWS_ACCESS_KEY_ID,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
		region: process.env.AWS_REGION,
	})

	// Create temp directory
	const tempDir = path.join(os.tmpdir(), uuidv4())
	const rarPath = path.join(tempDir, 'archive.rar')

	try {
		await fs.mkdir(tempDir, { recursive: true })

		// Download RAR file from S3
		const s3Object = await s3
			.getObject({
				Bucket: process.env.S3_BUCKET_NAME!,
				Key: fileKey,
			})
			.promise()

		// Ensure we handle S3 Body type correctly
		if (!s3Object.Body) {
			throw new Error('S3 object body is empty')
		}

		// Save to temp location (handle the Buffer type properly)
		await fs.writeFile(rarPath, s3Object.Body as Buffer)

		// Extract RAR
		const extractDir = path.join(tempDir, 'extracted')
		await fs.mkdir(extractDir, { recursive: true })

		// Use unrar to extract files
		await new Promise<void>((resolve, reject) => {
			unrar({
				path: rarPath,
				targetPath: extractDir,
				cb: (err: any) => {
					if (err) reject(err)
					else resolve()
				},
			})
		})

		// Get list of extracted files
		const files = await fs.readdir(extractDir)

		const userData = await getAutonomisUser()
		const username = userData.data.user_id || userId

		// Upload each supported file to S3
		const uploadPromises = files
			.filter(file => {
				const ext = path.extname(file).toLowerCase()
				return ['.csv', '.xlsx', '.xls', '.json', '.parquet'].includes(ext)
			})
			.map(async file => {
				const filePath = path.join(extractDir, file)
				const fileContent = await fs.readFile(filePath)
				const uploadPath = `uploads/${username}_${userId}/${file}`

				return s3
					.putObject({
						Bucket: process.env.S3_BUCKET_NAME!,
						Key: uploadPath,
						Body: fileContent,
						ContentType: getContentType(path.extname(file).toLowerCase()),
					})
					.promise()
			})

		await Promise.all(uploadPromises)

		// Clean up temp directory
		await fs.rm(tempDir, { recursive: true, force: true })

		return NextResponse.json(
			{
				success: true,
				message: `Extracted and uploaded ${uploadPromises.length} files`,
			},
			{ status: 200 }
		)
	} catch (error) {
		console.error('RAR extraction failed:', error)

		// Clean up on error
		try {
			await fs.rm(tempDir, { recursive: true, force: true })
		} catch (cleanupError) {
			console.error('Cleanup failed:', cleanupError)
		}

		return NextResponse.json(
			{
				success: false,
				message: 'Failed to process RAR file',
			},
			{ status: 500 }
		)
	}
}

function getContentType(extension: string) {
	switch (extension) {
		case '.csv':
			return 'text/csv'
		case '.xlsx':
		case '.xls':
			return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
		case '.json':
			return 'application/json'
		case '.parquet':
			return 'application/octet-stream'
		default:
			return 'application/octet-stream'
	}
}

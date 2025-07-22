import { NextRequest, NextResponse } from 'next/server'
import { ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3'

const s3Client = new S3Client({
	region: process.env.AWS_REGION!,
	credentials: {
		accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
		secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
	},
})

export async function GET() {
	try {
		// Common folder structures where JSON config files might be stored
		const possiblePrefixes = [
			'notebooks/',
			'sql-configs/', 
			'configs/',
			'json-configs/',
			'json/',
			'', // root directory
		]

		let allJsonFiles: any[] = []

		// Try each possible prefix
		for (const prefix of possiblePrefixes) {
			try {
				const command = new ListObjectsV2Command({
					Bucket: process.env.AWS_S3_BUCKET_NAME!,
					Prefix: prefix,
				})

				const response = await s3Client.send(command)
				
				const jsonFiles = (response.Contents || [])
					.filter(item => item.Key?.endsWith('.json'))
					.map(item => ({
						key: item.Key!,
						name: item.Key!.split('/').pop()!,
						size: item.Size || 0,
						lastModified: item.LastModified?.toISOString() || '',
						folder: prefix || 'root',
					}))

				allJsonFiles = allJsonFiles.concat(jsonFiles)
			} catch (prefixError) {
				console.log(`No access or files in ${prefix}:`, prefixError)
			}
		}

		// Remove duplicates based on key
		const uniqueJsonFiles = allJsonFiles.filter((file, index, self) =>
			index === self.findIndex(f => f.key === file.key)
		)

		return NextResponse.json({
			files: uniqueJsonFiles,
			message: `Found ${uniqueJsonFiles.length} JSON files`,
			searchedPrefixes: possiblePrefixes,
			stats: {
				totalFiles: uniqueJsonFiles.length,
				sqlConfigFiles: uniqueJsonFiles.filter(f => f.name.startsWith('sql_')).length,
				dataFiles: uniqueJsonFiles.filter(f => f.name.includes('data')).length,
				totalSize: uniqueJsonFiles.reduce((sum, f) => sum + f.size, 0),
			}
		})
	} catch (error) {
		console.error('Error listing JSON files:', error)
		return NextResponse.json(
			{ error: 'Failed to fetch JSON files' },
			{ status: 500 }
		)
	}
} 
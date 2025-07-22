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
		// Common folder structures where Python scripts/notebooks might be stored
		const possiblePrefixes = [
			'notebooks/',
			'python-scripts/',
			'scripts/',
			'py-files/',
			'python/',
			'', // root directory
		]

		let allPythonFiles: any[] = []

		// Try each possible prefix
		for (const prefix of possiblePrefixes) {
			try {
				const command = new ListObjectsV2Command({
					Bucket: process.env.AWS_S3_BUCKET_NAME!,
					Prefix: prefix,
				})

				const response = await s3Client.send(command)
				
				const pythonFiles = (response.Contents || [])
					.filter(item => item.Key?.endsWith('.py') || item.Key?.endsWith('.ipynb'))
					.map(item => ({
						key: item.Key!,
						name: item.Key!.split('/').pop()!,
						size: item.Size || 0,
						lastModified: item.LastModified?.toISOString() || '',
						folder: prefix || 'root',
						type: item.Key!.endsWith('.ipynb') ? 'notebook' : 'script',
					}))

				allPythonFiles = allPythonFiles.concat(pythonFiles)
			} catch (prefixError) {
				console.log(`No access or files in ${prefix}:`, prefixError)
			}
		}

		// Remove duplicates based on key
		const uniquePythonFiles = allPythonFiles.filter((file, index, self) =>
			index === self.findIndex(f => f.key === file.key)
		)

		return NextResponse.json({
			scripts: uniquePythonFiles,
			message: `Found ${uniquePythonFiles.length} Python files`,
			searchedPrefixes: possiblePrefixes,
			stats: {
				totalFiles: uniquePythonFiles.length,
				scripts: uniquePythonFiles.filter(f => f.type === 'script').length,
				notebooks: uniquePythonFiles.filter(f => f.type === 'notebook').length,
				edgeFunctions: uniquePythonFiles.filter(f => f.name.startsWith('edge_')).length,
				totalSize: uniquePythonFiles.reduce((sum, f) => sum + f.size, 0),
			}
		})
	} catch (error) {
		console.error('Error listing Python scripts:', error)
		return NextResponse.json(
			{ error: 'Failed to fetch Python scripts' },
			{ status: 500 }
		)
	}
} 
import { NextRequest, NextResponse } from 'next/server'
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3'
import { getAutonomisUser } from '@/lib/user'

const s3Client = new S3Client({
  region: process.env.AWS_REGION!,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
  },
})

export async function POST(req: NextRequest) {
  try {
    // Verify user authentication
    const { data: user, error: userError } = await getAutonomisUser()
    if (!user || userError) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { prefix = '' } = await req.json()

    // List objects with delimiter to get both files and "folders"
    const command = new ListObjectsV2Command({
      Bucket: process.env.AWS_S3_BUCKET_NAME!,
      Prefix: prefix,
      Delimiter: '/',
      MaxKeys: 1000, // Reasonable limit for UI performance
    })

    const response = await s3Client.send(command)

    // Process folders (CommonPrefixes)
    const folders = (response.CommonPrefixes || [])
      .map(prefix => {
        const key = prefix.Prefix!
        const name = key.split('/').filter(Boolean).pop() + '/'
        return {
          key,
          name,
          size: 0,
          lastModified: new Date().toISOString(),
          type: 'folder' as const
        }
      })

    // Process files (Contents)
    const files = (response.Contents || [])
      .filter(item => item.Key !== prefix) // Exclude the current directory itself
      .filter(item => !item.Key!.endsWith('/')) // Exclude folder markers
      .map(item => {
        const name = item.Key!.split('/').pop() || item.Key!
        return {
          key: item.Key!,
          name,
          size: item.Size || 0,
          lastModified: item.LastModified?.toISOString() || new Date().toISOString(),
          type: 'file' as const
        }
      })

    // Combine and sort: folders first, then files
    const objects = [
      ...folders.sort((a, b) => a.name.localeCompare(b.name)),
      ...files.sort((a, b) => a.name.localeCompare(b.name))
    ]

    return NextResponse.json({
      objects,
      prefix,
      truncated: response.IsTruncated || false,
      nextContinuationToken: response.NextContinuationToken,
    })

  } catch (error) {
    console.error('Error browsing S3:', error)
    return NextResponse.json(
      { error: 'Failed to browse S3 bucket', details: (error as Error).message },
      { status: 500 }
    )
  }
} 
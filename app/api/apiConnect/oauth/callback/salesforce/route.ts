import { getAutonomisUser } from '@/lib/user'
import axios from 'axios'
import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'

// Use hardcoded values when environment variables are not loaded properly

export async function GET(req: NextRequest) {
	const { data, error } = await getAutonomisUser()

	if (!data) {
		return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
	}

	const searchParams = req.nextUrl.searchParams
	const code = searchParams.get('code')

	const cookieStore = cookies()

	if (!code) {
		return Response.json({ error: 'Authorization code is missing.' }, { status: 405 })
	}

	try {
		const tokenResponse = await axios.post(
			'https://login.salesforce.com/services/oauth2/token',
			null,
			{
				params: {
					grant_type: 'authorization_code',
					client_id: CLIENT_ID,
					client_secret: CLIENT_SECRET,
					redirect_uri: REDIRECT_URI,
					code,
				},
			}
		)
		const { access_token, instance_url, refresh_token } = tokenResponse.data

		// Store access token
		cookieStore.set('sf_access_token', access_token, {
			httpOnly: true, // Cannot be accessed by JavaScript
			secure: true, // Only sent over HTTPS
			sameSite: 'lax', // CSRF protection
			maxAge: 60 * 60 * 2, // 2 hours expiry
		})

		// Store refresh token
		cookieStore.set('sf_refresh_token', refresh_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
		})

		// Store instance URL
		cookieStore.set('sf_instance_url', instance_url, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			// Instance URL rarely changes, so we can tie it to the refresh token
		})

		// Store tokens in a secure way (e.g., cookies or database)
		return Response.json({
			suceess:
				'You have successfully added your salesforce account in autonmis. You can close this tab now',
		})
		// res.status(200).json({ access_token, instance_url, refresh_token })
	} catch (error) {
		console.error('Error fetching Salesforce tokens:', error)
		return Response.json({ error: 'Failed to authenticate with Salesforce.' }, { status: 500})
		// res.status(500).json({ error: 'Failed to authenticate with Salesforce.' })
	}
}

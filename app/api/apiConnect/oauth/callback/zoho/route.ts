import { getAutonomisUser } from '@/lib/user'
import axios from 'axios'
import { cookies } from 'next/headers'
import { NextRequest } from 'next/server'

interface ExtendedNextRequest extends NextRequest {
	query: {
		code: string
	}
}

export async function GET(req: ExtendedNextRequest) {
	const { data, error } = await getAutonomisUser()

	if (!data) {
		return Response.json({ error: 'Invalid Request' }, { status: 400 })
	}

	const code = req.nextUrl.searchParams.get('code')

	if (!code) {
		return Response.json(
			{ message: 'Authorization code missing of Zoho' },
			{ status: 405 }
		)
	}

	const CLIENT_ID = process.env.NEXT_PUBLIC_ZOHO_CLIENT_ID
	const CLIENT_SECRET = process.env.NEXT_PUBLIC_ZOHO_CLIENT_SECRET
	const ZOHO_REDIRECT_URI = process.env.NEXT_PUBLIC_ZOHO_REDIRECT_URI

	console.log({ CLIENT_ID, CLIENT_SECRET, ZOHO_REDIRECT_URI })
	if (!CLIENT_ID && !CLIENT_SECRET && !ZOHO_REDIRECT_URI) {
		return Response.json({ message: 'credentials not found' }, { status: 400 })
	}

	const authCodeProf = {
		grant_type: 'authorization_code',
		client_id: CLIENT_ID,
		client_secret: CLIENT_SECRET,
		redirect_uri: ZOHO_REDIRECT_URI,
		code,
	}

	const headers = {
		'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
	}

	try {
		const zoho_response: any = await axios.post(
			'https://accounts.zoho.in/oauth/v2/token',
			null,
			{
				params: authCodeProf,
				headers,
			}
		)

		const tokens = zoho_response?.data

		if (!tokens) {
			return Response.json(
				{ message: 'Token exchange failed' },
				{ status: 400 }
			)
		}
		const cookieStore = cookies()

		cookieStore.set('zoho_access_token', tokens?.access_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			maxAge: 60 * 60 * 2,
		})
		cookieStore.set('zoho_refresh_token', tokens?.refresh_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			maxAge: 60 * 60 * 2,
		})

		return Response.json(
			{
				tokens,
				success:
					'You have successfully added your Zoho account in autonmis. You can close this tab now.',
			},
			{ status: 200 }
		)
	} catch (error) {
		return Response.json(
			{ message: 'Error fetching Zoho tokens', error },
			{ status: 500 }
		)
	}
}

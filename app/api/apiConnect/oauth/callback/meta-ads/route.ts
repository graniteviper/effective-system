import { getAutonomisUser } from '@/lib/user'
import axios from 'axios'
import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/utils/supabase/server'

interface ExtendedNextRequest extends NextRequest {
	query: {
		code: string,
		state?: string
	}
}

export async function GET(req: ExtendedNextRequest) {
	const { data, error } = await getAutonomisUser()

	if (!data) {
		return Response.json({ error: 'Invalid Request' }, { status: 400 })
	}

	const code = req.nextUrl.searchParams.get('code')
	const state = req.nextUrl.searchParams.get('state')

	if (!code) {
		return Response.json(
			{ message: 'Authorization code missing for Meta Ads' },
			{ status: 405 }
		)
	}

	const APP_ID = process.env.NEXT_PUBLIC_META_ADS_APP_ID
	const APP_SECRET = process.env.NEXT_PUBLIC_META_ADS_APP_SECRET

	const baseUrl = process.env.NEXT_PUBLIC_APP_URL || req.nextUrl.origin
	const REDIRECT_URI = `${baseUrl}/api/apiConnect/oauth/callback/meta-ads`

	console.log("Meta Ads OAuth callback with: ", { 
		APP_ID: APP_ID ? "[PRESENT]" : "[MISSING]", 
		APP_SECRET: APP_SECRET ? "[PRESENT]" : "[MISSING]", 
		REDIRECT_URI, 
		state 
	})
	
	if (!APP_ID || !APP_SECRET) {
		return Response.json({ message: 'credentials not found' }, { status: 400 })
	}

	try {
		// Exchange code for access token
		const tokenUrl = `https://graph.facebook.com/v18.0/oauth/access_token?client_id=${APP_ID}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&client_secret=${APP_SECRET}&code=${code}`
		
		const meta_response: any = await axios.get(tokenUrl)
		const tokens = meta_response?.data

		if (!tokens || !tokens.access_token) {
			return Response.json(
				{ message: 'Token exchange failed' },
				{ status: 400 }
			)
		}
		
		const cookieStore = cookies()

		cookieStore.set('meta_access_token', tokens.access_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			maxAge: 60 * 60 * 2,
		})
		
		let connectionId = '';
		
		if (state) {
			try {
				const supabase = createClient()
				const { data: userData } = await supabase.auth.getUser()
				
				if (userData.user) {
					const { data: metaAdsData } = await supabase
						.from('autonomis_data_source')
						.select('id')
						.eq('name', 'Meta Ads')
						.single();
						
					if (!metaAdsData) {
						console.error('Could not find Meta Ads datasource');
						return Response.json({ error: 'Could not find Meta Ads datasource' }, { status: 500 });
					}
						
					const { data: connection, error: connError } = await supabase
						.from('autonomis_user_database')
						.insert([
							{
								user_id: userData.user.id,
								datasource_id: metaAdsData.id,
								connection_string: JSON.stringify({
									conn_name: state,
									access_token: tokens.access_token,
									app_id: APP_ID,
									app_secret: APP_SECRET,
									expires_in: tokens.expires_in,
									token_type: tokens.token_type || 'bearer'
								})
							}
						])
						.select()
						
					if (connError) {
						console.error('Error storing Meta Ads connection:', connError)
					} else if (connection && connection.length > 0) {
						connectionId = connection[0].id;
						console.log(`Created Meta Ads connection with ID: ${connectionId}`);
					}
				}
			} catch (error) {
				console.error('Error creating Meta Ads connection:', error)
			}
		}

		const successUrl = `${baseUrl}/connect/connection-success?name=${state || 'Meta Ads'}&provider=meta-ads`;
		console.log(`Redirecting to success page: ${successUrl}`);
		return NextResponse.redirect(successUrl);
	} catch (error: any) {
		console.error('Error in Meta Ads OAuth flow:', error.response?.data || error.message);
		return Response.json(
			{ 
				message: 'Error fetching Meta Ads tokens', 
				error: error.response?.data || error.message 
			},
			{ status: 500 }
		)
	}
} 
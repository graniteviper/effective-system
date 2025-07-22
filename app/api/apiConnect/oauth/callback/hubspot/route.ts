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
	const state = req.nextUrl.searchParams.get('state') // Get state parameter

	if (!code) {
		return Response.json(
			{ message: 'Authorization code missing of HubSpot' },
			{ status: 405 }
		)
	}

	const CLIENT_ID = process.env.NEXT_PUBLIC_HUBSPOT_CLIENT_ID
	const CLIENT_SECRET = process.env.NEXT_PUBLIC_HUBSPOT_CLIENT_SECRET

	// Get the base URL from environment or use the request URL
	const baseUrl = process.env.NEXT_PUBLIC_APP_URL || req.nextUrl.origin
	const REDIRECT_URI = `${baseUrl}/api/apiConnect/oauth/callback/hubspot`

	console.log("Hubspot OAuth callback with: ", { 
		CLIENT_ID: CLIENT_ID ? "[PRESENT]" : "[MISSING]", 
		CLIENT_SECRET: CLIENT_SECRET ? "[PRESENT]" : "[MISSING]", 
		REDIRECT_URI, 
		state 
	})
	
	if (!CLIENT_ID || !CLIENT_SECRET) {
		return Response.json({ message: 'credentials not found' }, { status: 400 })
	}

	const authCodeProf = {
		grant_type: 'authorization_code',
		client_id: CLIENT_ID,
		client_secret: CLIENT_SECRET,
		redirect_uri: REDIRECT_URI,
		code,
	}

	const headers = {
		'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
	}

	try {
		const hubapi_response: any = await axios.post(
			'https://api.hubapi.com/oauth/v1/token',
			authCodeProf,
			{
				headers,
			}
		)

		const tokens = hubapi_response?.data

		if (!tokens) {
			return Response.json(
				{ message: 'Token exchange failed' },
				{ status: 400 }
			)
		}
		
		const cookieStore = cookies()

		cookieStore.set('hs_access_token', tokens?.access_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			maxAge: 60 * 60 * 2,
		})
		cookieStore.set('hs_refresh_token', tokens?.refresh_token, {
			httpOnly: true,
			secure: true,
			sameSite: 'lax',
			maxAge: 60 * 60 * 2,
		})
		
		let connectionId = '';
		
		// Store the connection in the database if we have a state (connection name)
		if (state) {
			try {
				const supabase = createClient()
				const { data: userData } = await supabase.auth.getUser()
				
				if (userData.user) {
					// Get the HubSpot datasource ID
					const { data: hubspotData } = await supabase
						.from('autonomis_data_source')
						.select('id')
						.eq('name', 'HubSpot')
						.single();
						
					if (!hubspotData) {
						console.error('Could not find HubSpot datasource');
						return Response.json({ error: 'Could not find HubSpot datasource' }, { status: 500 });
					}
						
					// Create a new connection record
					const { data: connection, error: connError } = await supabase
						.from('autonomis_user_database')
						.insert([
							{
								user_id: userData.user.id,
								datasource_id: hubspotData.id,
								connection_string: JSON.stringify({
									conn_name: state,
									access_token: tokens.access_token,
									refresh_token: tokens.refresh_token,
									expires_in: tokens.expires_in,
									token_type: tokens.token_type
								})
							}
						])
						.select()
						
					if (connError) {
						console.error('Error storing HubSpot connection:', connError)
					} else if (connection && connection.length > 0) {
						connectionId = connection[0].id;
						console.log(`Created HubSpot connection with ID: ${connectionId}`);
					}
				}
			} catch (error) {
				console.error('Error creating HubSpot connection:', error)
			}
		}

		// Redirect to success page with connection name and provider
		const successUrl = `${baseUrl}/connect/connection-success?name=${state || 'HubSpot'}&provider=hubspot`;
		console.log(`Redirecting to success page: ${successUrl}`);
		return NextResponse.redirect(successUrl);
	} catch (error: any) {
		console.error('Error in Hubspot OAuth flow:', error.response?.data || error.message);
		return Response.json(
			{ 
				message: 'Error fetching HubSpot tokens', 
				error: error.response?.data || error.message 
			},
			{ status: 500 }
		)
	}
}

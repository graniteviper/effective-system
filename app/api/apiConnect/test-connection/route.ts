// In your callback route after successful authorization:
import { getAutonomisUser } from '@/lib/user'
import zohoClient from '@/utils/connectors/Zoho'
import axios from 'axios'
import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'

export async function GET(req: NextRequest) {
	const { data, error } = await getAutonomisUser()

	if (!data) {
		return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
	}

	const cookieStore = cookies()

	const searchParams = req.nextUrl.searchParams
	const thirdPartyApp = searchParams.get('app')

	switch (thirdPartyApp) {
		case 'salesforce':
			// Instead get this from db, this will avoid writing all the codes here that each app requires
			const access_token = cookieStore.get('sf_access_token')?.value
			const instance_url = cookieStore.get('sf_instance_url')?.value
			const refresh_token = cookieStore.get('sf_refresh_token')?.value

			if (!access_token || !instance_url) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}
			try {
				const response = await axios.get(
					`${instance_url}/services/oauth2/userinfo`,
					{
						headers: {
							Authorization: `Bearer ${access_token}`,
						},
					}
				)

				return Response.json({
					access_token,
					instance_url,
					refresh_token,
				})
			} catch (error) {
				// console.log('Error while fetching your salesforce credentials', error)
				return Response.json(
					{
						error: 'Error while fetching your salesforce credentials',
					},
					{ status: 401 }
				)
			}

		case 'hubspot':
			const hub_access_token = cookieStore.get('hs_access_token')?.value
			const hub_refresh_token = cookieStore.get('hs_refresh_token')?.value
			if (!hub_access_token) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}
			// console.log(hub_access_token)
			const hub_headers = {
				accept: 'application/json',
				authorization: `Bearer ${hub_access_token}`,
			}
			try {
				const data: any = await axios.get(
					'https://api.hubapi.com/account-info/v3/api-usage/daily/private-apps',
					{
						headers: hub_headers,
					}
				)

				return Response.json(
					{ access_token: hub_access_token, refresh_token: hub_refresh_token },
					{ status: 200 }
				)
			} catch (error) {
				// console.log(error)
				return Response.json(
					{ error: 'Error while fetching your daily limit of hubspot' },
					{ status: 401 }
				)
			}
		case 'zoho':
			const zoho_access_token = cookieStore.get('zoho_access_token')?.value
			const zoho_refresh_token = cookieStore.get('zoho_refresh_token')?.value
			// console.log(zoho_access_token)
			if (!zoho_access_token) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}
			const zoho_headers = {
				authorization: `Bearer ${zoho_access_token}`,
			}
			// console.log(zoho_headers)
			try {
				const res = await zohoClient.apiCall(
					'GET',
					'/crm/v6/users/actions/count',
					null,
					{ headers: zoho_headers }
				)
				if (res.data) {
					const tokens = {
						access_token: zoho_access_token,
						refresh_token: zoho_refresh_token,
					}
					console.log(tokens)
					return Response.json({ ...tokens }, { status: 200 })
				}
			} catch (error) {
				return Response.json(
					{ error: "Error while fetching your zoho's users" },
					{ status: 401 }
				)
			}
		case 'mixpanel':
			const mixpanel_username = cookieStore.get('mixpanel_username')?.value
			const mixpanel_secret = cookieStore.get('mixpanel_secret')?.value
			console.log(mixpanel_username)
			if (!mixpanel_username) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}

			// console.log(zoho_headers)
			try {
				const data: any = await axios.get('')
				return Response.json(
					{ mixpanel_username, mixpanel_secret },
					{ status: 200 }
				)
			} catch (error) {
				return Response.json(
					{ error: "Error while fetching your zoho's users" },
					{ status: 401 }
				)
			}
		case 'googleanalytics4':
			const google_analytics_4_access_token =
				cookieStore.get('ga_4_access_token')?.value
			const google_analytics_4_refresh_token =
				cookieStore.get('ga_4_refresh_token')?.value
			// console.log(google_analytics_4_access_token)

			if (!google_analytics_4_access_token) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}
			const google_analytics_4_headers = {
				authorization: `Bearer ${google_analytics_4_access_token}`,
			}
			// console.log(google_analytics_4_headers)
			try {
				const data: any = await axios.get(
					`https://analyticsdata.googleapis.com/v1beta/properties/0/metadata`,
					{
						headers: google_analytics_4_headers,
					}
				)
				const tokens = {
					access_token: google_analytics_4_access_token,
					refresh_token: google_analytics_4_refresh_token,
				}
				console.log(tokens)
				return Response.json({ ...tokens }, { status: 201 })
			} catch (error) {
				console.log(error)
				return Response.json(
					{ message: 'Error while fetching your Google Analytics Data', error },
					{ status: 401 }
				)
			}
		case 'airtable':
			const airtable_access_token = cookieStore.get(
				'airtable_access_token'
			)?.value
			const airtable_refresh_token = cookieStore.get(
				'airtable_refresh_token'
			)?.value

			if (!airtable_access_token && !airtable_refresh_token) {
				return NextResponse.json(
					{ error: 'Not authenticated', success: false },
					{
						status: 401,
					}
				)
			}
			const airtable_headers = {
				authorization: `Bearer ${airtable_access_token}`,
			}
			// console.log(google_analytics_4_headers)
			try {
				const data: any = await axios.get(
					`https://api.airtable.com/v0/meta/bases`,
					{
						headers: airtable_headers,
					}
				)
				const tokens = {
					access_token: airtable_access_token,
					refresh_token: airtable_refresh_token,
				}
				console.log(tokens)
				return Response.json({ ...tokens }, { status: 201 })
			} catch (error) {
				console.log(error)
				return Response.json(
					{ message: 'Error while fetching your Airtable Data', error },
					{ status: 401 }
				)
			}
		default:
			return Response.json({ error: 'Wrong data source.' }, { status: 400 })
	}
}

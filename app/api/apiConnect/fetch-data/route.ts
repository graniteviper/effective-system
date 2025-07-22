import { HubspotAPI } from '@/lib/hubspotapi'
import { getAutonomisUser } from '@/lib/user'
import zohoClient from '@/utils/connectors/Zoho'
import axios from 'axios'
import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'

export async function GET(req: NextRequest) {
	try {
		const { data, error } = await getAutonomisUser()

		if (!data) {
			return NextResponse.json({ error: 'Invalid request' }, { status: 400 })
		}

		const cookieStore = cookies()

		const searchParams = req.nextUrl.searchParams
		const thirdPartyApp = searchParams.get('app')

		switch (thirdPartyApp) {
			case 'salesforce':
				const accessToken = cookieStore.get('sf_access_token')?.value
				const instance_url = cookieStore.get('sf_instance_url')?.value

				if (!accessToken || !instance_url) {
					return new Response(
						JSON.stringify({ error: 'Not authenticated', success: false }),
						{
							status: 401,
						}
					)
				}
				try {
					const response = await axios.get(
						`${instance_url}/services/data/v59.0/sobjects`,
						{
							headers: {
								Authorization: `Bearer ${accessToken}`,
							},
						}
					)

					return Response.json({ data: response.data.sobjects })
				} catch (error) {
					// console.log('Error while fetching your salesforce objects', error)
					return Response.json({
						error: 'Error while fetching your salesforce objects',
					})
				}
			case 'hubspot':
				const hub_access_token = cookieStore.get('hs_access_token')?.value
				const hub_refresh_token = cookieStore.get('hs_refresh_token')?.value

				// console.log(hub_access_token, hub_refresh_token)

				try {
					if (!hub_access_token) {
						return new Response(
							JSON.stringify({
								error: 'Not authenticated',
								success: false,
							}),
							{
								status: 401,
							}
						)
					}

					if (!hub_refresh_token) {
						return new Response(
							JSON.stringify({
								error: 'Not authenticated',
								success: false,
							}),
							{
								status: 401,
							}
						)
					}

					const hubspotAPI = new HubspotAPI(hub_access_token)
					const response = hubspotAPI.fetchProperties()

					// console.log(`FETCH_DATA[route]: `, response)

					return new Response(
						JSON.stringify({ data: response, success: true }),
						{ status: 200 }
					)
				} catch (error) {
					// console.log(error)
					return new Response(
						JSON.stringify({
							error: 'Error while accessing HubSpot API',
							success: false,
						}),
						{ status: 402 }
					)
				}

			case 'zoho':
				try {
					const data: any = await zohoClient.apiCall(
						'GET',
						'/crm/v6/settings/modules'
					)

					const response = data.data.modules.map((item: any) => {
						return { name: item.api_name, profiles: item?.profiles }
					})
					return new Response(
						JSON.stringify({
							data: response,
							success: true,
						}),
						{ status: 200 }
					)
				} catch (error) {
					// console.log(error)
					return new Response(
						JSON.stringify({
							error: 'Error while accessing Zoho API',
							success: false,
						}),
						{ status: 400 }
					)
				}

			case 'mixpanel':
				const mixpanel_secret = cookieStore.get('mixpanel_secret')?.value
				const mixpanel_username = cookieStore.get('mixpanel_username')?.value

				try {
					if (!mixpanel_secret && !mixpanel_username) {
						return new Response(
							JSON.stringify({
								error: 'Not authenticated',
								success: false,
							}),
							{
								status: 401,
							}
						)
					}

					const data = [
						{
							name: 'List Schemas',
							required_fields: ['project_id'],
						},
						{
							name: 'List Lookup Tables',
							required_fields: ['project_id'],
						},
						{
							name: 'List for Entity',
							required_fields: ['project_id', 'entityType'],
						},
						{
							name: 'List for Entity and Name',
							required_fields: ['project_id', 'entityType', 'name'],
						},
						{
							name: 'Query Segementation Report',
							required_fields: ['project_id', 'event', 'from_date', 'to_date'],
						},
					]

					console.log(data)
					return new Response(
						JSON.stringify({
							data: data,
							success: true,
						}),
						{ status: 200 }
					)
				} catch (error) {
					console.log(error)
					return new Response(
						JSON.stringify({
							error: 'Error while accessing MixPanel API',
							success: false,
						}),
						{ status: 400 }
					)
				}
			case 'googleanalytics4':
				const google_analytics_4_access_token =
					cookieStore.get('ga_4_access_token')?.value

				// console.log(google_analytics_4_access_token)

				try {
					if (!google_analytics_4_access_token) {
						return new Response(
							JSON.stringify({
								error: 'Not authenticated',
								success: false,
							}),
							{
								status: 401,
							}
						)
					}

					const response = [
						{
							name: 'Get Metadata',
							required_fields: [],
						},
						{
							name: 'Accounts List',
							required_fields: [],
						},
						{
							name: 'Specific Accounts',
							required_fields: ['account_name'],
						},
						{
							name: 'Account Summary',
							required_fields: [],
						},
					]

					return new Response(
						JSON.stringify({
							data: response,
							success: true,
						}),
						{ status: 200 }
					)
				} catch (error) {
					// console.log(error)
					return new Response(
						JSON.stringify({
							error: 'Error while accessing Zoho API',
							success: false,
						}),
						{ status: 402 }
					)
				}
			case 'airtable':
				const airtable_access_token = cookieStore.get(
					'airtable_access_token'
				)?.value

				// console.log(google_analytics_4_access_token)

				try {
					if (!airtable_access_token) {
						return new Response(
							JSON.stringify({
								error: 'Not authenticated',
								success: false,
							}),
							{
								status: 401,
							}
						)
					}

					const response = [
						{
							name: 'List Bases',
							required_fields: [],
						},
						{
							name: 'Get Base Schema',
							required_fields: ['baseId'],
						},
						{
							name: 'List Records',
							required_fields: [
								'baseId',
								'tableIdOrName',
								'maxRecords',
								'pageSize',
								'sort',
								'view',
								'filterByFormula',
								'fields',
							],
						},
						{
							name: 'Get Record',
							required_fields: ['baseId', 'tableIdOrName', 'recordId'],
						},
						{
							name: 'List Records In View',
							required_fields: [
								'baseId',
								'tableIdOrName',
								'viewIdOrName',
								'maxRecords',
								'pageSize',
								'sort',
								'filterByFormula',
								'fields',
							],
						},
						{
							name: 'List Webhooks',
							required_fields: ['baseId'],
						},
						{
							name: 'Get Webhook Info',
							required_fields: ['baseId', 'webhookId'],
						},
						{
							name: 'Get User Info',
							required_fields: [],
						},
					]

					return new Response(
						JSON.stringify({
							data: response,
							success: true,
						}),
						{ status: 200 }
					)
				} catch (error) {
					// console.log(error)
					return new Response(
						JSON.stringify({
							error: 'Error while accessing Airtable API',
							success: false,
						}),
						{ status: 402 }
					)
				}
		}
	} catch (error) {
		return Response.json({ error: 'Could not fetch the objects' })
	}
}

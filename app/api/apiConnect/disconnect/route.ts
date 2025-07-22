import { getAutonomisUser } from '@/lib/user'
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
				cookieStore.delete('sf_access_token')
				cookieStore.delete('sf_instance_token')
				// const refresh_token = cookieStore.get('sf_refresh_token')?.value

				return Response.json({ message: 'disconnected' })
			case 'hubspot':
				cookieStore.delete('hs_access_token')
				cookieStore.delete('hs_refresh_token')
				// const refresh_token = cookieStore.get('sf_refresh_token')?.value

				return Response.json({ message: 'disconnected' })
			case 'zoho':
				cookieStore.delete('zoho_access_token')
				cookieStore.delete('zoho_refresh_token')
				// const refresh_token = cookieStore.get('sf_refresh_token')?.value

				return Response.json({ message: 'disconnected' })
			case 'googleanalytics4':
				cookieStore.delete('ga_4_access_token')
				cookieStore.delete('ga_4_refresh_token')
				// const refresh_token = cookieStore.get('sf_refresh_token')?.value

				return Response.json({ message: 'disconnected' })
			case 'airtable':
				cookieStore.delete('airtable_access_token')
				cookieStore.delete('airtable_refresh_token')
				// const refresh_token = cookieStore.get('sf_refresh_token')?.value

				return Response.json({ message: 'disconnected' })
		}
	} catch (error) {
		return Response.json({ message: 'Could not disconnect' })
	}
}

import { createSupbaseServerClient } from '@/lib/supabase'
import { NextRequest, NextResponse } from 'next/server'

export async function GET(
  request: NextRequest,
  { params }: { params: { source: string } }
) {
  const source = params.source

  try {
    const supabase = createSupbaseServerClient()
    const { data: authData } = await supabase.auth.getUser()
    
    if (!authData.user) {
      return NextResponse.json(
        { success: false, error: 'Unauthorized' },
        { status: 401 }
      )
    }

    // Fetch the connection details
    const { data: connectionData } = await supabase
      .from('autonomis_database')
      .select('connection_string, autonomis_data_source(name)')
      .eq('user_id', authData.user.id)
      .ilike('autonomis_data_source.name', source)
      .single()

    if (!connectionData) {
      return NextResponse.json(
        { success: false, error: 'Connection not found' },
        { status: 404 }
      )
    }

    let objects: string[] = []
    
    // Get objects based on source type
    if (source === 'salesforce') {
      // For now, return some default Salesforce objects
      // In a real implementation, this would connect to Salesforce API
      objects = ['Account', 'Contact', 'Opportunity', 'Lead', 'Case']
    } else if (source === 'zoho') {
      // Default Zoho objects
      objects = ['Accounts', 'Contacts', 'Deals', 'Tasks']
    } else if (source === 'hubspot') {
      objects = ['Companies', 'Contacts', 'Deals', 'Tickets']
    } else if (source === 'google_analytics') {
      objects = ['Sessions', 'Users', 'Events', 'PageViews']
    }

    return NextResponse.json({ success: true, objects })
  } catch (error) {
    console.error('Error fetching objects:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to fetch objects' },
      { status: 500 }
    )
  }
} 
import { createSupbaseServerClient } from '@/lib/supabase'
import { NextRequest, NextResponse } from 'next/server'

export async function GET(request: NextRequest) {
  try {
    const supabase = createSupbaseServerClient()
    const { data: authData } = await supabase.auth.getUser()
    
    if (!authData.user) {
      return NextResponse.json(
        { success: false, error: 'Unauthorized' },
        { status: 401 }
      )
    }

    const searchParams = request.nextUrl.searchParams
    const pipelineId = searchParams.get('pipeline_id')
    
    if (!pipelineId) {
      return NextResponse.json(
        { success: false, error: 'Pipeline ID is required' },
        { status: 400 }
      )
    }
    
    // Get the extraction configuration
    const { data, error } = await supabase
      .from('autonomis_bapps_extraction_config')
      .select('*, autonomis_bapps_extraction_run(created_at)')
      .eq('pipeline_id', pipelineId)
      .eq('user_id', authData.user.id)
      .order('created_at', { ascending: false })
      .limit(1)
      .single()
      
    if (error) {
      if (error.code === 'PGRST116') {
        // No data found
        return NextResponse.json({ success: true, extraction: null })
      }
      throw error
    }
    
    // Parse the JSON config
    if (data && data.config) {
      data.config = JSON.parse(data.config)
    }

    // Get the last run time
    let lastRun = null
    if (data.autonomis_bapps_extraction_run && data.autonomis_bapps_extraction_run.length > 0) {
      // Sort by date descending and get the most recent
      const sortedRuns = [...data.autonomis_bapps_extraction_run].sort((a, b) => 
        new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      )
      lastRun = sortedRuns[0].created_at
    }
    
    // Format the response
    const extraction = {
      ...data,
      last_run: lastRun
    }
    
    return NextResponse.json({ success: true, extraction })
  } catch (error) {
    console.error('Error fetching extraction status:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to fetch extraction status' },
      { status: 500 }
    )
  }
} 
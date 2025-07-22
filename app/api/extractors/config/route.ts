import { createSupbaseServerClient } from '@/lib/supabase'
import { NextRequest, NextResponse } from 'next/server'

export async function POST(request: NextRequest) {
  try {
    const supabase = createSupbaseServerClient()
    const { data: authData } = await supabase.auth.getUser()
    
    if (!authData.user) {
      return NextResponse.json(
        { success: false, error: 'Unauthorized' },
        { status: 401 }
      )
    }

    const { pipeline_id, config } = await request.json()
    
    // Store the extraction configuration
    const { data, error } = await supabase
      .from('autonomis_bapps_extraction_config')
      .insert({
        pipeline_id,
        user_id: authData.user.id,
        config: JSON.stringify(config),
        status: 'active'
      })
      .select('id')
      .single()
      
    if (error) {
      throw error
    }
    
    return NextResponse.json({ success: true, id: data.id })
  } catch (error) {
    console.error('Error creating extraction config:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to create extraction configuration' },
      { status: 500 }
    )
  }
}

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
      .select('*')
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
    
    return NextResponse.json({ success: true, extraction: data })
  } catch (error) {
    console.error('Error fetching extraction config:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to fetch extraction configuration' },
      { status: 500 }
    )
  }
} 
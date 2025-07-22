import { createSupbaseServerClient } from '@/lib/supabase'
import { NextRequest, NextResponse } from 'next/server'

/*
-- SQL Migration for extraction tables
-- Run this in your Supabase SQL Editor

-- Table for extraction configuration
CREATE TABLE IF NOT EXISTS public.autonomis_bapps_extraction_config (
  id SERIAL PRIMARY KEY,
  pipeline_id INTEGER REFERENCES public.autonomis_pipeline(id) ON DELETE CASCADE,
  user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  config JSONB NOT NULL,
  status TEXT DEFAULT 'active',
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Table for extraction runs
CREATE TABLE IF NOT EXISTS public.autonomis_bapps_extraction_run (
  id SERIAL PRIMARY KEY,
  config_id INTEGER REFERENCES public.autonomis_bapps_extraction_config(id) ON DELETE CASCADE,
  user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  status TEXT DEFAULT 'queued',
  logs JSONB DEFAULT '[]'::jsonb,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add function to update the updated_at column
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add trigger to update the updated_at column for config table
CREATE TRIGGER update_extraction_config_updated_at
BEFORE UPDATE ON public.autonomis_bapps_extraction_config
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

-- Add trigger to update the updated_at column for run table
CREATE TRIGGER update_extraction_run_updated_at
BEFORE UPDATE ON public.autonomis_bapps_extraction_run
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();
*/

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

    const { pipeline_id } = await request.json()
    
    if (!pipeline_id) {
      return NextResponse.json(
        { success: false, error: 'Pipeline ID is required' },
        { status: 400 }
      )
    }

    // Get the extraction configuration
    const { data: configData, error: configError } = await supabase
      .from('autonomis_bapps_extraction_config')
      .select('*')
      .eq('pipeline_id', pipeline_id)
      .eq('user_id', authData.user.id)
      .order('created_at', { ascending: false })
      .limit(1)
      .single()
      
    if (configError) {
      if (configError.code === 'PGRST116') {
        return NextResponse.json(
          { success: false, error: 'No extraction configuration found for this pipeline' },
          { status: 404 }
        )
      }
      throw configError
    }
    
    // Create a new extraction run record
    const { data: runData, error: runError } = await supabase
      .from('autonomis_bapps_extraction_run')
      .insert({
        config_id: configData.id,
        user_id: authData.user.id,
        status: 'queued',
        logs: JSON.stringify([{
          timestamp: new Date().toISOString(),
          message: 'Extraction job queued'
        }])
      })
      .select('id')
      .single()
      
    if (runError) {
      throw runError
    }
    
    // In a real implementation, this would trigger a background job to run the extraction
    // For now, we'll just simulate the job by updating the status after a delay
    setTimeout(async () => {
      try {
        // Update the run status to 'in_progress'
        await supabase
          .from('autonomis_bapps_extraction_run')
          .update({
            status: 'in_progress',
            logs: JSON.stringify([
              {
                timestamp: new Date().toISOString(),
                message: 'Extraction job queued'
              },
              {
                timestamp: new Date().toISOString(),
                message: 'Extraction job started'
              }
            ])
          })
          .eq('id', runData.id)
        
        // Simulate some processing time
        setTimeout(async () => {
          try {
            // Update the run status to 'completed'
            await supabase
              .from('autonomis_bapps_extraction_run')
              .update({
                status: 'completed',
                logs: JSON.stringify([
                  {
                    timestamp: new Date().toISOString(),
                    message: 'Extraction job queued'
                  },
                  {
                    timestamp: new Date().toISOString(),
                    message: 'Extraction job started'
                  },
                  {
                    timestamp: new Date().toISOString(),
                    message: 'Extraction job completed successfully'
                  }
                ])
              })
              .eq('id', runData.id)
          } catch (error) {
            console.error('Error updating run status to completed:', error)
          }
        }, 10000) // Complete after 10 seconds
      } catch (error) {
        console.error('Error updating run status to in_progress:', error)
      }
    }, 2000) // Start after 2 seconds
    
    return NextResponse.json({ 
      success: true, 
      message: 'Extraction job started',
      run_id: runData.id
    })
  } catch (error) {
    console.error('Error starting extraction job:', error)
    return NextResponse.json(
      { success: false, error: 'Failed to start extraction job' },
      { status: 500 }
    )
  }
} 
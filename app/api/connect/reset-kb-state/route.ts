import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

export async function POST(req: NextRequest) {
  try {
    const { connectionId, userId } = await req.json();
    
    if (!connectionId || !userId) {
      return NextResponse.json(
        { error: 'Missing required fields' },
        { status: 400 }
      );
    }
    
    // Initialize Supabase client
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
    const supabaseServiceKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
    const client = createClient(supabaseUrl, supabaseServiceKey);
    
    // Get source ID for the connection
    const { data: source, error: sourceError } = await client
      .from('autonmis_kb_sources')
      .select('id')
      .eq('user_id', userId)
      .eq('connection_id', connectionId)
      .single();
    
    if (sourceError && sourceError.code !== 'PGRST116') { // PGRST116 is "no rows returned"
      console.error('Error fetching source:', sourceError);
      throw sourceError;
    }
    
    if (source) {
      // Delete set-source links
      const { error: linkError } = await client
        .from('autonmis_kb_set_sources')
        .delete()
        .eq('source_id', source.id);
      
      if (linkError) {
        console.error('Error deleting set-source links:', linkError);
        throw linkError;
      }
      
      // Delete documents
      const { error: docError } = await client
        .from('autonmis_kb_documents')
        .delete()
        .eq('source_id', source.id);
      
      if (docError) {
        console.error('Error deleting documents:', docError);
        throw docError;
      }
      
      // Delete source
      const { error: sourceDeleteError } = await client
        .from('autonmis_kb_sources')
        .delete()
        .eq('id', source.id);
      
      if (sourceDeleteError) {
        console.error('Error deleting source:', sourceDeleteError);
        throw sourceDeleteError;
      }
    }
    
    // Update connection status
    const { error: updateError } = await client
      .from('autonomis_user_database')
      .update({ is_vectorized: false })
      .eq('id', connectionId);
    
    if (updateError) {
      console.error('Error updating connection status:', updateError);
      throw updateError;
    }
    
    return NextResponse.json({
      success: true,
      message: 'Successfully reset knowledge base state'
    });
    
  } catch (error) {
    console.error('Error resetting KB state:', error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to reset knowledge base state' },
      { status: 500 }
    );
  }
} 
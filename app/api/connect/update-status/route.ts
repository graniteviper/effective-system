import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

export async function POST(req: NextRequest) {
  try {
    const { connectionId, isVectorized } = await req.json();
    
    if (!connectionId || typeof isVectorized !== 'boolean') {
      return NextResponse.json(
        { error: 'Missing required fields' },
        { status: 400 }
      );
    }
    
    // Initialize Supabase client
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
    const supabaseServiceKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
    const client = createClient(supabaseUrl, supabaseServiceKey);
    
    // Update connection status
    const { error } = await client
      .from('autonomis_user_database')
      .update({ is_vectorized: isVectorized })
      .eq('id', connectionId);
    
    if (error) {
      console.error('Error updating connection status:', error);
      throw error;
    }
    
    return NextResponse.json({
      success: true,
      message: `Connection status updated to ${isVectorized ? 'vectorized' : 'not vectorized'}`
    });
    
  } catch (error) {
    console.error('Error updating connection status:', error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to update connection status' },
      { status: 500 }
    );
  }
} 
import { NextRequest, NextResponse } from 'next/server';
import { addConnectionToKnowledgeBase } from '@/app/connect/dbQueries/query';

export async function POST(request: NextRequest) {
  try {
    const { connectionId, userId } = await request.json();
    
    if (!connectionId || !userId) {
      return NextResponse.json(
        { error: 'Missing required fields: connectionId and userId' },
        { status: 400 }
      );
    }
    
    const result = await addConnectionToKnowledgeBase(connectionId, userId);
    
    if (!result.success) {
      return NextResponse.json(
        { 
          success: false, 
          error: result.error,
          existing: result.existing,
          warning: result.warning
        },
        { status: result.existing ? 409 : 500 }
      );
    }
    
    return NextResponse.json({
      success: true,
      message: result.message || 'Successfully added to knowledge base',
      workspaceId: result.workspace
    });
    
  } catch (error) {
    console.error('Error adding to KB:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: error instanceof Error ? error.message : 'Unknown error occurred' 
      },
      { status: 500 }
    );
  }
} 
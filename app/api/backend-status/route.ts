import { NextRequest, NextResponse } from 'next/server'

interface ServiceStatus {
  name: string
  status: 'online' | 'offline' | 'error'
  lastChecked: string
  responseTime?: number
  version?: string
  details?: any
}

interface BackendStatusResponse {
  services: ServiceStatus[]
  overall: 'healthy' | 'degraded' | 'down'
  timestamp: string
}

// Service configuration
const SERVICES = {
  airflow: {
    name: 'Airflow',
    healthUrl: process.env.AIRFLOW_WEBSERVER_URL || 'http://44.209.110.93:8080',
    healthEndpoint: '/health',
    timeout: 5000
  },
  sling: {
    name: 'Sling',
    healthUrl: process.env.SLING_SERVICE_URL || 'http://44.209.110.93:5001',
    healthEndpoint: '/all_jobs',
    timeout: 5000
  }
}

async function checkServiceHealth(serviceName: string, config: any): Promise<ServiceStatus> {
  const startTime = Date.now()
  
  try {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), config.timeout)
    
    const response = await fetch(`${config.healthUrl}${config.healthEndpoint}`, {
      method: 'GET',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
      }
    })
    
    clearTimeout(timeoutId)
    const responseTime = Date.now() - startTime
    
    if (response.ok) {
      const data = await response.json().catch(() => ({}))
      
      // Handle different response formats
      if (serviceName === 'sling' && config.healthEndpoint === '/all_jobs') {
        // Sling /all_jobs endpoint - check if jobs object exists
        const jobCount = data.jobs ? Object.keys(data.jobs).length : 0
        return {
          name: config.name,
          status: 'online',
          lastChecked: new Date().toISOString(),
          responseTime,
          version: 'sling-service',
          details: { jobCount, endpoint: 'all_jobs' }
        }
      } else {
        // Standard health endpoint
        return {
          name: config.name,
          status: 'online',
          lastChecked: new Date().toISOString(),
          responseTime,
          version: data.version || 'unknown',
          details: data
        }
      }
    } else {
      return {
        name: config.name,
        status: 'error',
        lastChecked: new Date().toISOString(),
        responseTime,
        details: { statusCode: response.status, statusText: response.statusText }
      }
    }
  } catch (error: any) {
    const responseTime = Date.now() - startTime
    return {
      name: config.name,
      status: 'offline',
      lastChecked: new Date().toISOString(),
      responseTime,
      details: { 
        error: error.name === 'AbortError' ? 'Timeout' : error.message 
      }
    }
  }
}

export async function GET(request: NextRequest) {
  try {
    // Check all services in parallel
    const serviceChecks = await Promise.all([
      checkServiceHealth('airflow', SERVICES.airflow),
      checkServiceHealth('sling', SERVICES.sling)
    ])

    // Determine overall system health
    const onlineServices = serviceChecks.filter(s => s.status === 'online').length
    const totalServices = serviceChecks.length
    
    let overall: 'healthy' | 'degraded' | 'down'
    if (onlineServices === totalServices) {
      overall = 'healthy'
    } else if (onlineServices > 0) {
      overall = 'degraded'
    } else {
      overall = 'down'
    }

    const response: BackendStatusResponse = {
      services: serviceChecks,
      overall,
      timestamp: new Date().toISOString()
    }

    return NextResponse.json(response)
  } catch (error) {
    console.error('Backend status check failed:', error)
    return NextResponse.json(
      { 
        error: 'Failed to check backend status',
        services: [],
        overall: 'down',
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    )
  }
} 
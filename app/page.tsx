import { redirect } from 'next/navigation'
import { createSupbaseServerClient } from '@/lib/supabase'
import Register from './register/page'
import { Metadata } from 'next'

export const dynamic = 'force-dynamic'

export const metadata: Metadata = {
  title: 'Autonmis - AI Data Platform',
  description: 'Conversational Data Workspace',
  // ... rest of your existing metadata
}

export default async function Home() {
  // Initialize Supabase client on the server
  const supabase = createSupbaseServerClient()

  // Check for existing user session
  const { data: { user }, error: sessionError } = await supabase.auth.getUser()

  // If there's a session error, we'll still show the registration page
  if (sessionError) {
    console.error('Session error:', sessionError)
  }

  // If user is authenticated, check their registration status
  if (user) {
    const { data: userRegistrationData, error: registrationError } = await supabase
      .from('autonomis_user_registration')
      .select('*')
      .eq('user_id', user.id)
      .single()

    // Redirect based on registration status
    if (registrationError || !userRegistrationData) {
      redirect('/info')
    } else {
      redirect('/home')
    }
  }

  // If no user is authenticated, render the registration page
  return (
    <div className="min-h-screen bg-[#f9f7f4]">
      <Register />
    </div>
  )
}
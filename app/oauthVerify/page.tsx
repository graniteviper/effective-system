"use client";

import { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Loader2, CheckCircle, AlertCircle } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

export default function OAuthVerifyPage() {
  const [status, setStatus] = useState<'loading' | 'success' | 'error'>('loading');
  const [message, setMessage] = useState('');

  useEffect(() => {
    const handleOAuthFlow = async () => {
      const urlParams = new URLSearchParams(window.location.search);
      const error = urlParams.get('error');
      const success = urlParams.get('success');

      if (error || success) {
        // Handle callback from OAuth provider
        handleOAuthResult(urlParams);
      } else {
        // Start new OAuth flow
        const authUrl = urlParams.get('authUrl');
        if (!authUrl) {
          setStatus('error');
          setMessage('Missing authentication URL');
          return;
        }
        window.location.href = decodeURIComponent(authUrl);
      }
    };

    const handleOAuthResult = (params: URLSearchParams) => {
      if (params.get('success')) {
        setStatus('success');
        setMessage('Authentication successful! You can close this window.');
        
        // Notify parent window
        window.opener?.postMessage(
          { 
            type: 'OAUTH_COMPLETE', 
            success: true, 
            provider: new URLSearchParams(window.location.search).get('provider') 
          }, 
          window.location.origin
        );
        
        // Auto-close after 3 seconds
        setTimeout(() => window.close(), 3000);
      } else {
        setStatus('error');
        setMessage(params.get('error') || 'Authentication failed');
      }
    };

    handleOAuthFlow();
  }, []);

  return (
    <div className="flex items-center justify-center min-h-screen bg-gray-50 dark:bg-gray-900 p-4">
      <Card className="w-full max-w-md shadow-lg">
        <CardHeader className={`${status === 'success' ? 'bg-green-50 dark:bg-green-900/20' : status === 'error' ? 'bg-red-50 dark:bg-red-900/20' : 'bg-gray-50 dark:bg-gray-800/50'} rounded-t-lg transition-colors duration-300`}>
          <CardTitle className="text-center text-xl">
            {status === 'loading' ? 'Authentication in Progress' : 
             status === 'success' ? 'Authentication Complete' : 
             'Authentication Failed'}
          </CardTitle>
          <CardDescription className="text-center">
            {status === 'loading' ? 'Please wait while we connect to the service...' : ''}
          </CardDescription>
        </CardHeader>
        
        <CardContent className="pt-6 pb-4 flex flex-col items-center">
          {status === 'loading' && (
            <div className="flex flex-col items-center gap-4 py-6">
              <Loader2 className="h-16 w-16 text-primary animate-spin" />
              <p className="text-muted-foreground text-center">Redirecting to authentication provider...</p>
            </div>
          )}
          
          {status === 'success' && (
            <div className="flex flex-col items-center gap-4 py-6">
              <CheckCircle className="h-16 w-16 text-green-500" />
              <Alert variant="default" className="bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800">
                <AlertTitle>Success!</AlertTitle>
                <AlertDescription>{message}</AlertDescription>
              </Alert>
              <p className="text-sm text-muted-foreground text-center">This window will close automatically in a few seconds.</p>
            </div>
          )}
          
          {status === 'error' && (
            <div className="flex flex-col items-center gap-4 py-6">
              <AlertCircle className="h-16 w-16 text-red-500" />
              <Alert variant="destructive">
                <AlertTitle>Error</AlertTitle>
                <AlertDescription>{message}</AlertDescription>
              </Alert>
            </div>
          )}
        </CardContent>
        
        <CardFooter className="flex justify-center gap-2 pt-2 pb-6">
          {status === 'error' && (
            <>
              <Button onClick={() => window.location.reload()} variant="secondary">
                Try Again
              </Button>
              <Button onClick={() => window.close()}>
                Close Window
              </Button>
            </>
          )}
          
          {status === 'success' && (
            <Button onClick={() => window.close()}>
              Close Now
            </Button>
          )}
        </CardFooter>
      </Card>
    </div>
  );
}
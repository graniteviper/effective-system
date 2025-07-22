"""
Salesforce API connector implementation.
This module provides a connector for Salesforce APIs.
"""

import logging
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from datetime import datetime, timedelta

from backend.extractors.base.api_connector import BaseAPIConnector


class SalesforceConnector(BaseAPIConnector):
    """Salesforce API connector implementation.
    
    Attributes:
        credentials (Dict): Salesforce authentication credentials
        instance_url (str): Salesforce instance URL
        access_token (str): OAuth access token
        api_version (str): Salesforce API version
        logger (logging.Logger): Logger for this connector
    """
    
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        """Initialize the Salesforce connector.
        
        Args:
            credentials: Dictionary containing authentication credentials
                Required keys: client_id, client_secret, username, password
                Optional keys: security_token, instance_url, api_version
            rate_limit_config: Optional configuration for API rate limiting
        """
        super().__init__(credentials, rate_limit_config)
        
        self.instance_url = credentials.get('instance_url', '')
        self.api_version = credentials.get('api_version', '57.0')
        self.access_token = None
        self.last_request_time = None
        self.session = requests.Session()
        self.request_count = 0
        self.max_retries = 3
        
    def authenticate(self) -> bool:
        """Authenticate with Salesforce using the provided credentials.
        
        This method supports password-based OAuth flow for Salesforce.
        
        Returns:
            bool: True if authentication was successful, False otherwise
        """
        try:
            auth_url = 'https://login.salesforce.com/services/oauth2/token'
            if self.credentials.get('sandbox', False):
                auth_url = 'https://test.salesforce.com/services/oauth2/token'
                
            payload = {
                'grant_type': 'password',
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret'],
                'username': self.credentials['username'],
                'password': self.credentials['password'],
            }
            
            # Add security token to password if provided
            if 'security_token' in self.credentials:
                payload['password'] += self.credentials['security_token']
            
            response = self.session.post(auth_url, data=payload)
            
            if response.status_code == 200:
                auth_data = response.json()
                self.access_token = auth_data['access_token']
                self.instance_url = auth_data.get('instance_url', self.instance_url)
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json'
                })
                self.logger.info("Successfully authenticated with Salesforce")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
        
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            return False
    
    def validate_connection(self) -> bool:
        """Validate the connection to Salesforce.
        
        Returns:
            bool: True if connection is valid, False otherwise
        """
        if not self.access_token:
            return self.authenticate()
        
        try:
            # Try to make a simple request to verify the connection
            url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects"
            response = self.session.get(url)
            
            if response.status_code == 200:
                self.logger.info("Connection to Salesforce is valid")
                return True
            elif response.status_code == 401:
                # Token expired, try to reauthenticate
                self.logger.info("Access token expired, reauthenticating")
                return self.authenticate()
            else:
                self.logger.error(f"Connection validation failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False
    
    def handle_rate_limits(self):
        """Handle Salesforce API rate limits.
        
        Salesforce enforces various API limits, this method implements
        a simple delay mechanism to avoid hitting those limits.
        """
        # Simple rate limiting based on request count
        if self.request_count >= 100:  # Reset after 100 requests
            time.sleep(5)  # Wait 5 seconds
            self.request_count = 0
            return
            
        # Ensure at least 100ms between requests
        if self.last_request_time:
            elapsed = datetime.now() - self.last_request_time
            if elapsed.total_seconds() < 0.1:
                time.sleep(0.1 - elapsed.total_seconds())
                
        self.last_request_time = datetime.now()
        self.request_count += 1
    
    def fetch_data(self, 
                  object_name: str, 
                  query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Salesforce using SOQL queries.
        
        Args:
            object_name: The name of the Salesforce object
            query_params: Optional parameters for the SOQL query
                fields: List of fields to fetch
                where: SOQL WHERE clause
                limit: Maximum number of records
                order_by: SOQL ORDER BY clause
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch data")
            return []
            
        query_params = query_params or {}
        fields = query_params.get('fields', ['Id', 'Name', 'CreatedDate', 'LastModifiedDate'])
        where_clause = query_params.get('where', '')
        limit_clause = f"LIMIT {query_params.get('limit', 2000)}" if 'limit' in query_params else ""
        order_by = query_params.get('order_by', '')
        
        # Build SOQL query
        fields_str = ', '.join(fields)
        query = f"SELECT {fields_str} FROM {object_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        if order_by:
            query += f" ORDER BY {order_by}"
            
        if limit_clause:
            query += f" {limit_clause}"
            
        self.logger.debug(f"SOQL Query: {query}")
        
        try:
            # Execute query
            self.handle_rate_limits()
            url = f"{self.instance_url}/services/data/v{self.api_version}/query"
            response = self.session.get(url, params={'q': query})
            
            if response.status_code == 200:
                data = response.json()
                records = data.get('records', [])
                
                # Handle pagination for large result sets
                next_url = data.get('nextRecordsUrl')
                while next_url:
                    self.handle_rate_limits()
                    self.logger.debug(f"Fetching next batch from: {next_url}")
                    next_url_full = f"{self.instance_url}{next_url}"
                    response = self.session.get(next_url_full)
                    
                    if response.status_code == 200:
                        data = response.json()
                        records.extend(data.get('records', []))
                        next_url = data.get('nextRecordsUrl')
                    else:
                        self.logger.error(f"Error fetching next batch: {response.status_code} - {response.text}")
                        break
                
                # Remove Salesforce metadata attributes
                for record in records:
                    if 'attributes' in record:
                        del record['attributes']
                
                self.logger.info(f"Successfully fetched {len(records)} records from {object_name}")
                return records
            else:
                self.logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def fetch_schema(self, object_name: str) -> Dict[str, Any]:
        """Fetch the schema of a Salesforce object.
        
        Args:
            object_name: The name of the Salesforce object
            
        Returns:
            Dictionary containing the schema information
        """
        if not self.validate_connection():
            self.logger.error("Connection validation failed, cannot fetch schema")
            return {}
            
        try:
            # Get object description
            self.handle_rate_limits()
            url = f"{self.instance_url}/services/data/v{self.api_version}/sobjects/{object_name}/describe"
            response = self.session.get(url)
            
            if response.status_code == 200:
                schema = response.json()
                
                # Extract relevant schema information
                fields = schema.get('fields', [])
                field_info = {}
                
                for field in fields:
                    field_info[field['name']] = {
                        'type': field['type'],
                        'label': field['label'],
                        'length': field.get('length'),
                        'nillable': field.get('nillable', True),
                        'createable': field.get('createable', False),
                        'updateable': field.get('updateable', False),
                    }
                
                return {
                    'name': schema.get('name'),
                    'label': schema.get('label'),
                    'fields': field_info,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                self.logger.error(f"Error fetching schema: {response.status_code} - {response.text}")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error fetching schema: {str(e)}")
            return {}
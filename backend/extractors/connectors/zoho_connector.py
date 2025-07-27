"""
Zoho API connector implementation.
This module provides a connector for Zoho CRM APIs.
"""

import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any

from backend.extractors.base.api_connector import BaseAPIConnector

class ZohoConnector(BaseAPIConnector):
    """Zoho CRM API connector implementation."""

    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        super().__init__(credentials, rate_limit_config)
        self.access_token = credentials.get("access_token")
        self.refresh_token = credentials.get("refresh_token")
        self.client_id = credentials["client_id"]
        self.client_secret = credentials["client_secret"]
        self.api_domain = "https://www.zohoapis.com"
        self.session = requests.Session()
        self.last_request_time = None
        self.request_count = 0

        # Attach headers if access_token is already present
        if self.access_token:
            self.session.headers.update({
                "Authorization": f"Zoho-oauthtoken {self.access_token}",
                "Content-Type": "application/json"
            })

    def authenticate(self) -> bool:
        """Authenticate using refresh token."""
        try:
            token_url = "https://accounts.zoho.com/oauth/v2/token"
            payload = {
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token"
            }
            response = self.session.post(token_url, params=payload)
            if response.status_code == 200:
                data = response.json()
                self.access_token = data["access_token"]
                self.session.headers.update({
                    "Authorization": f"Zoho-oauthtoken {self.access_token}"
                })
                self.logger.info("Zoho authentication successful")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Authentication exception: {str(e)}")
            return False

    def validate_connection(self) -> bool:
        """Ping the Users API to check token validity."""
        try:
            url = f"{self.api_domain}/crm/v2/users"
            response = self.session.get(url)
            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                self.logger.info("Access token expired, trying to reauthenticate")
                return self.authenticate()
            return False
        except Exception as e:
            self.logger.error(f"Connection validation failed: {str(e)}")
            return False

    def handle_rate_limits(self):
        """Basic rate limit strategy: wait if too many requests."""
        if self.request_count >= 100:
            time.sleep(5)
            self.request_count = 0
        if self.last_request_time:
            elapsed = datetime.now() - self.last_request_time
            if elapsed.total_seconds() < 0.1:
                time.sleep(0.1 - elapsed.total_seconds())
        self.last_request_time = datetime.now()
        self.request_count += 1

    def fetch_data(self, object_name: str, query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from Zoho CRM for a given module."""
        if not self.validate_connection():
            return []

        try:
            records = []
            page = 1
            more_records = True
            base_url = f"{self.api_domain}/crm/v2/{object_name}"
            fields = query_params.get("fields") if query_params else None
            params = {
                "per_page": 200,
                "page": page
            }

            if fields:
                params["fields"] = ",".join(fields)

            while more_records:
                self.handle_rate_limits()
                response = self.session.get(base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    batch = data.get("data", [])
                    records.extend(batch)
                    more_records = data.get("info", {}).get("more_records", False)
                    page += 1
                    params["page"] = page
                else:
                    self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                    break

            self.logger.info(f"Fetched {len(records)} records from Zoho module '{object_name}'")
            return records

        except Exception as e:
            self.logger.error(f"Error fetching data from Zoho: {str(e)}")
            return []

    def fetch_schema(self, object_name: str) -> Dict[str, Any]:
        """Fetch metadata (field-level schema) for a Zoho module."""
        if not self.validate_connection():
            return {}

        try:
            url = f"{self.api_domain}/crm/v2/settings/fields?module={object_name}"
            self.handle_rate_limits()
            response = self.session.get(url)
            if response.status_code == 200:
                fields = response.json().get("fields", [])
                field_info = {}
                for field in fields:
                    field_info[field["api_name"]] = {
                        "label": field.get("field_label"),
                        "type": field.get("data_type"),
                        "length": field.get("length"),
                        "nillable": not field.get("system_mandatory", False)
                    }

                return {
                    "name": object_name,
                    "label": object_name,
                    "fields": field_info,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                self.logger.error(f"Failed to fetch schema: {response.status_code} - {response.text}")
                return {}

        except Exception as e:
            self.logger.error(f"Exception fetching schema: {str(e)}")
            return {}

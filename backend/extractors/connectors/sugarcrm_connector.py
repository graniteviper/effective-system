"""
SugarCRM API connector implementation.
This module provides a connector for SugarCRM APIs.
"""

import time
import logging
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime

from backend.extractors.base.api_connector import BaseAPIConnector


class SugarCRMConnector(BaseAPIConnector):
    def __init__(self, credentials: Dict[str, Any], rate_limit_config: Optional[Dict[str, Any]] = None):
        super().__init__(credentials, rate_limit_config)

        self.base_url = credentials.get("base_url")
        self.client_id = credentials.get("client_id")
        self.client_secret = credentials.get("client_secret")
        self.username = credentials.get("username")
        self.password = credentials.get("password")

        self.access_token = None
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.last_request_time = None
        self.request_count = 0

    def authenticate(self) -> bool:
        try:
            auth_url = f"{self.base_url}/oauth2/token"
            payload = {
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
                "password": self.password
            }

            response = self.session.post(auth_url, data=payload)
            if response.status_code == 200:
                data = response.json()
                self.access_token = data["access_token"]
                self.session.headers.update({
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                })
                self.logger.info("Authenticated with SugarCRM successfully.")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            return False

    def validate_connection(self) -> bool:
        if not self.access_token:
            return self.authenticate()

        try:
            response = self.session.get(f"{self.base_url}/rest/v11/me")
            if response.status_code == 200:
                return True
            elif response.status_code == 401:
                return self.authenticate()
            return False
        except Exception as e:
            self.logger.error(f"Connection validation error: {str(e)}")
            return False

    def handle_rate_limits(self):
        if self.request_count >= 100:
            time.sleep(3)
            self.request_count = 0
            return

        if self.last_request_time:
            elapsed = datetime.now() - self.last_request_time
            if elapsed.total_seconds() < 0.2:
                time.sleep(0.2 - elapsed.total_seconds())

        self.last_request_time = datetime.now()
        self.request_count += 1

    def fetch_data(self, object_name: str, query_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if not self.validate_connection():
            return []

        query_params = query_params or {}
        fields = query_params.get("fields", [])
        filters = query_params.get("filters", {})
        limit = query_params.get("limit", 100)

        try:
            url = f"{self.base_url}/rest/v11/{object_name}"
            params = {"max_num": limit}

            # Convert filters if provided
            if filters:
                params.update({"filter": filters})

            self.handle_rate_limits()
            response = self.session.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                return data.get("records", [])
            else:
                self.logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            self.logger.error(f"Fetch data error: {str(e)}")
            return []

    def fetch_schema(self, object_name: str) -> Dict[str, Any]:
        try:
            self.handle_rate_limits()
            url = f"{self.base_url}/rest/v11/{object_name}/fields"
            response = self.session.get(url)

            if response.status_code == 200:
                return {
                    "name": object_name,
                    "fields": response.json().get("fields", {}),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                self.logger.error(f"Failed to fetch schema: {response.status_code} - {response.text}")
                return {}
        except Exception as e:
            self.logger.error(f"Fetch schema error: {str(e)}")
            return {}

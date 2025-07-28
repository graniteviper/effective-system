#!/usr/bin/env python
"""
Test script for SugarCRM data extraction.
This script validates the SugarCRM extraction framework with mock data.
"""
import unittest
import logging
from unittest.mock import MagicMock, patch
import os
from datetime import datetime
from dotenv import load_dotenv


from backend.extractors.extractors.sugarcrm_extractor import SugarCRMExtractor
from backend.extractors.storage.postgres_storage import PostgresStorageManager


load_dotenv()


class TestSugarCRMExtraction(unittest.TestCase):
    """Test cases for SugarCRM extraction framework."""

    def setUp(self):
        logging.basicConfig(level=logging.INFO)

        self.test_dir = "/tmp/sugarcrm_test_data"
        os.makedirs(self.test_dir, exist_ok=True)

        self.mock_accounts = [
            {"id": "001A", "name": "Test Account 1", "industry": "Tech", "date_entered": "2023-01-01T00:00:00Z", "date_modified": "2023-01-02T00:00:00Z"},
            {"id": "001B", "name": "Test Account 2", "industry": "Finance", "date_entered": "2023-01-03T00:00:00Z", "date_modified": "2023-01-04T00:00:00Z"},
        ]

        self.mock_contacts = [
            {"id": "003A", "first_name": "John", "last_name": "Doe", "email": "john@example.com", "account_id": "001A", "date_modified": "2023-01-02T00:00:00Z"},
            {"id": "003B", "first_name": "Jane", "last_name": "Smith", "email": "jane@example.com", "account_id": "001B", "date_modified": "2023-01-04T00:00:00Z"},
        ]

        self.mock_schema = {
            "name": "Accounts",
            "fields": {
                "id": {"type": "id", "label": "ID"},
                "name": {"type": "string", "label": "Name"},
                "industry": {"type": "string", "label": "Industry"},
                "date_modified": {"type": "datetime", "label": "Modified Date"}
            },
            "timestamp": datetime.now().isoformat()
        }

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch('backend.extractors.connectors.sugarcrm_connector.SugarCRMConnector')
    def test_full_extraction(self, mock_connector_class):
        mock_connector = MagicMock()
        mock_connector_class.return_value = mock_connector
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_schema.return_value = self.mock_schema
        mock_connector.fetch_data.side_effect = lambda obj, params=None: self.mock_accounts if obj == "Accounts" else self.mock_contacts

        storage = PostgresStorageManager({
                "connection_url": os.getenv("DATABASE_URL")
})

        extractor = SugarCRMExtractor(
            connector=mock_connector,
            storage=storage,
            config={"extract_path": "sugarcrm", "schema_extract": True}
        )

        results = extractor.extract(["Accounts", "Contacts"], extraction_type="full")

        self.assertTrue(results["success"])
        self.assertEqual(len(results["object_results"]), 2)
        self.assertEqual(results["object_results"]["Accounts"]["record_count"], 2)
        self.assertEqual(results["object_results"]["Contacts"]["record_count"], 2)

    @patch('backend.extractors.connectors.sugarcrm_connector.SugarCRMConnector')
    def test_incremental_extraction(self, mock_connector_class):
        mock_connector = MagicMock()
        mock_connector_class.return_value = mock_connector
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_schema.return_value = self.mock_schema
        mock_connector.fetch_data.side_effect = lambda obj, params=None: [self.mock_accounts[1]] if obj == "Accounts" else [self.mock_contacts[1]]

        storage = PostgresStorageManager({
                "connection_url": os.getenv("DATABASE_URL")
})

        extractor = SugarCRMExtractor(
            connector=mock_connector,
            storage=storage,
            config={"extract_path": "sugarcrm", "schema_extract": True}
        )

        since = "2023-01-03T00:00:00Z"
        results = extractor.extract(["Accounts", "Contacts"], extraction_type="incremental", since_date=since)

        self.assertTrue(results["success"])
        self.assertEqual(results["object_results"]["Accounts"]["record_count"], 1)
        self.assertEqual(results["object_results"]["Contacts"]["record_count"], 1)

if __name__ == '__main__':
    unittest.main()

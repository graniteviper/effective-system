#!/usr/bin/env python
"""
Test script for Zoho data extraction.
This script validates the Zoho extraction framework with mock data.
"""

import unittest
import logging
from unittest.mock import MagicMock, patch
from dotenv import load_dotenv
import os
from datetime import datetime

from backend.extractors.extractors.zoho_extractor import ZohoExtractor
from backend.extractors.storage.postgres_storage import PostgresStorageManager


load_dotenv()

class TestZohoExtraction(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO)

        self.test_dir = "/tmp/zoho_test_data"
        os.makedirs(self.test_dir, exist_ok=True)

        self.mock_records = [
            {"id": "001", "Name": "Lead One", "Email": "lead1@example.com"},
            {"id": "002", "Name": "Lead Two", "Email": "lead2@example.com"}
        ]

        self.mock_schema = {
            "name": "Leads",
            "fields": {
                "id": {"type": "string"},
                "Name": {"type": "string"},
                "Email": {"type": "email"}
            },
            "timestamp": datetime.now().isoformat()
        }

    def tearDown(self):
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch('backend.extractors.connectors.zoho_connector.ZohoConnector')
    def test_full_extraction(self, MockConnector):
        mock_connector = MagicMock()
        MockConnector.return_value = mock_connector
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_data.return_value = self.mock_records
        mock_connector.fetch_schema.return_value = self.mock_schema

        storage = PostgresStorageManager({
    "connection_url": os.getenv("DATABASE_URL")
})

        extractor = ZohoExtractor(
            connector=mock_connector,
            storage=storage,
            config={
                "batch_size": 200,
                "schema_extract": True,
                "extract_path": "zoho",
                "fields": {"Leads": ["id", "Name", "Email"]}
            }
        )

        results = extractor.extract(
            object_names=["Leads"],
            extraction_type="full"
        )

        self.assertTrue(results["success"])
        self.assertEqual(results["object_results"]["Leads"]["record_count"], 2)

    @patch('backend.extractors.connectors.zoho_connector.ZohoConnector')
    def test_incremental_extraction(self, MockConnector):
        mock_connector = MagicMock()
        MockConnector.return_value = mock_connector
        mock_connector.validate_connection.return_value = True
        mock_connector.fetch_data.return_value = [self.mock_records[1]]
        mock_connector.fetch_schema.return_value = self.mock_schema

        storage = PostgresStorageManager({
                "connection_url": os.getenv("DATABASE_URL")
})

        extractor = ZohoExtractor(
            connector=mock_connector,
            storage=storage,
            config={
                "batch_size": 200,
                "schema_extract": True,
                "extract_path": "zoho",
                "fields": {"Leads": ["id", "Name", "Email"]}
            }
        )

        since = "2023-01-01T00:00:00Z"
        results = extractor.extract(
            object_names=["Leads"],
            extraction_type="incremental",
            since_date=since
        )

        self.assertTrue(results["success"])
        self.assertEqual(results["object_results"]["Leads"]["record_count"], 1)


if __name__ == "__main__":
    unittest.main()
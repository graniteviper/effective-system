"""
Zoho data extractor implementation.
This module orchestrates the extraction of data from Zoho CRM.
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from backend.extractors.base.base_storage_manager import BaseStorageManager
from backend.extractors.extractors.base_extractor import BaseExtractor
from backend.extractors.connectors.zoho_connector import ZohoConnector


class ZohoExtractor(BaseExtractor):
    def __init__(self, connector: ZohoConnector, storage: BaseStorageManager, config: Dict[str, Any]):
        super().__init__(connector, storage, config)
        self.config.setdefault("batch_size", 200)
        self.config.setdefault("schema_extract", True)
        self.config.setdefault("extract_path", "zoho")

    def extract(self, object_names: List[str], extraction_type: str = "full", since_date: Optional[str] = None) -> Dict[str, Any]:
        start_time = time.time()
        results = {
            "success": True,
            "extraction_type": extraction_type,
            "start_time": datetime.now().isoformat(),
            "object_results": {},
            "errors": [],
        }

        if not self.connector.validate_connection():
            results["success"] = False
            results["errors"].append("Failed to connect to Zoho")
            return results

        for object_name in object_names:
            self.logger.info(f"Extracting {object_name} ({extraction_type})")
            try:
                if self.config.get("schema_extract"):
                    self._extract_schema(object_name)

                if extraction_type.lower() == "incremental" and since_date:
                    success, metadata = self.extract_incremental(object_name, since_date)
                else:
                    success, metadata = self.extract_full(object_name)

                results["object_results"][object_name] = metadata
                if not success:
                    results["success"] = False
                    results["errors"].append(f"Failed to extract {object_name}")

            except Exception as e:
                self.logger.error(f"Error extracting {object_name}: {str(e)}")
                results["success"] = False
                results["errors"].append(str(e))
                results["object_results"][object_name] = {"success": False, "error": str(e)}

        execution_time = time.time() - start_time
        results["end_time"] = datetime.now().isoformat()
        results["execution_time_seconds"] = round(execution_time, 2)

        summary_path = f"{self.config['extract_path']}/extraction_summary_{int(time.time())}.json"
        self.storage.store_data(results, summary_path)

        return results

    def extract_full(self, object_name: str) -> Tuple[bool, Dict[str, Any]]:
        metadata = {
            "object_name": object_name,
            "extraction_type": "full",
            "start_time": datetime.now().isoformat(),
            "record_count": 0,
            "success": False,
        }
        try:
            query_params = {
                "fields": self.config.get("fields", {}).get(object_name, []),
            }
            records = self.connector.fetch_data(object_name, query_params)

            if not records:
                metadata["success"] = True
                metadata["message"] = "No records found"
                return True, metadata

            path = f"{self.config['extract_path']}/{object_name}/full_{int(time.time())}"
            success, storage_path = self.storage.store_data(records, path)
            metadata.update({
                "success": success,
                "record_count": len(records),
                "storage_path": storage_path,
                "end_time": datetime.now().isoformat()
            })
            return success, metadata
        except Exception as e:
            self.logger.error(f"Error in full extraction: {str(e)}")
            metadata["error"] = str(e)
            return False, metadata

    def extract_incremental(self, object_name: str, since_date: str) -> Tuple[bool, Dict[str, Any]]:
        metadata = {
            "object_name": object_name,
            "extraction_type": "incremental",
            "since_date": since_date,
            "start_time": datetime.now().isoformat(),
            "record_count": 0,
            "success": False,
        }
        try:
            fields = self.config.get("fields", {}).get(object_name, [])
            filters = {"Modified_Time": {"gt": since_date}}
            query_params = {"fields": fields, "filters": filters}
            records = self.connector.fetch_data(object_name, query_params)

            if not records:
                metadata["success"] = True
                metadata["message"] = "No incremental records found"
                return True, metadata

            path = f"{self.config['extract_path']}/{object_name}/incremental_{int(time.time())}"
            success, storage_path = self.storage.store_data(records, path)
            metadata.update({
                "success": success,
                "record_count": len(records),
                "storage_path": storage_path,
                "end_time": datetime.now().isoformat()
            })
            return success, metadata
        except Exception as e:
            self.logger.error(f"Error in incremental extraction: {str(e)}")
            metadata["error"] = str(e)
            return False, metadata

    def _extract_schema(self, object_name: str) -> bool:
        try:
            schema = self.connector.fetch_schema(object_name)
            if not schema:
                self.logger.warning(f"No schema found for {object_name}")
                return False
            path = f"{self.config['extract_path']}/schemas/{object_name}_schema_{int(time.time())}"
            success, _ = self.storage.store_data(schema, path)
            return success
        except Exception as e:
            self.logger.error(f"Error fetching schema: {str(e)}")
            return False

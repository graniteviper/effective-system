import os
import logging
from typing import Dict, Any, Optional, List, Union, Tuple
from abc import ABC, abstractmethod
import pandas as pd
import boto3
from io import StringIO, BytesIO

logger = logging.getLogger(__name__)


class BaseStorageManager(ABC):
    """Base class for all storage managers.
    
    This abstract class defines the interface that all storage managers must implement.
    It handles storing and retrieving data to/from various storage systems.
    
    Attributes:
        config (Dict): Configuration for the storage system
        logger (logging.Logger): Logger for the storage manager
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the storage manager with configuration.
        
        Args:
            config: Dictionary containing storage system configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
    @abstractmethod
    def connect(self) -> bool:
        """Connect to the storage system.
        
        Returns:
            bool: True if connection was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def validate_storage(self) -> bool:
        """Validate the connection to the storage system.
        
        Returns:
            bool: True if storage system is accessible, False otherwise
        """
        pass
    
    @abstractmethod
    def store_data(self, 
                  data: Union[List[Dict[str, Any]], str, bytes], 
                  path: str,
                  metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Store data to the storage system.
        
        Args:
            data: The data to store (can be list of dicts, string, or bytes)
            path: The path where the data should be stored
            metadata: Optional metadata to store with the data
            
        Returns:
            Tuple containing:
            - bool: True if storing was successful, False otherwise
            - str: Path/identifier where the data was stored or error message
        """
        pass
    
    @abstractmethod
    def retrieve_data(self, 
                     path: str,
                     as_type: str = 'dict') -> Tuple[bool, Union[List[Dict[str, Any]], str, bytes, None]]:
        """Retrieve data from the storage system.
        
        Args:
            path: The path from where to retrieve the data
            as_type: The return type ('dict', 'string', 'bytes')
            
        Returns:
            Tuple containing:
            - bool: True if retrieval was successful, False otherwise
            - Union[List[Dict], str, bytes, None]: The retrieved data or None if failed
        """
        pass
    
    @abstractmethod
    def list_files(self, path: str) -> List[str]:
        """List files in a specific path.
        
        Args:
            path: The path to list files from
            
        Returns:
            List of file paths
        """
        pass
    
    def handle_partitioning(self, data: List[Dict[str, Any]], partition_size: int = 1000) -> List[List[Dict[str, Any]]]:
        """Partition data for efficient storage.
        
        Args:
            data: The data to partition
            partition_size: Maximum number of records per partition
            
        Returns:
            List of partitioned data
        """
        return [data[i:i+partition_size] for i in range(0, len(data), partition_size)]
    
    def close(self):
        """Close any open connections."""
        pass
    
    def __enter__(self):
        """Context manager entry point."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()


class StorageManager(ABC):
    """Abstract base class for data storage managers"""
    
    @abstractmethod
    def save_data(self, df: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Save data to the destination.
        Args:
            df: DataFrame to save
            destination: Destination path/name
            **kwargs: Additional parameters
        Returns:
            bool: True if save successful, False otherwise
        """
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the storage operation.
        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        pass


class S3StorageManager(StorageManager):
    """Storage manager for S3 destinations"""
    
    def __init__(self, credentials: Dict[str, str], config: Dict[str, Any]):
        """
        Initialize S3 storage manager.
        Args:
            credentials: Dictionary containing AWS credentials:
                - aws_access_key_id: AWS access key
                - aws_secret_access_key: AWS secret key
            config: Dictionary containing configuration:
                - region_name: AWS region (default: us-east-1)
                - file_format: Output format (default: csv)
                - compression: Compression type (None, gzip, etc.)
        """
        self.credentials = credentials
        self.config = config
        self.metadata = {
            "bytes_written": 0,
            "format": config.get("file_format", "csv"),
            "compression": config.get("compression", None),
            "path": None
        }
        # Set default configuration
        self.config.setdefault("region_name", "us-east-1")
        self.config.setdefault("file_format", "csv")
        # Create S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials.get('aws_access_key_id'),
            aws_secret_access_key=credentials.get('aws_secret_access_key'),
            region_name=config.get('region_name')
        )
    
    def save_data(self, df: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Save DataFrame to S3.
        Args:
            df: DataFrame to save
            destination: S3 destination in format "bucket_name/key"
            **kwargs: Additional arguments:
                - file_format: Override configured format
                - compression: Override configured compression
        Returns:
            bool: True if save successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to save")
                return True
            
            # Parse S3 destination
            parts = destination.split('/', 1)
            if len(parts) != 2:
                logger.error(f"Invalid S3 destination format: {destination}")
                return False
            
            bucket_name, key = parts
            
            # Get format and compression
            file_format = kwargs.get("file_format", self.config.get("file_format", "csv"))
            compression = kwargs.get("compression", self.config.get("compression", None))
            
            # Prepare data for upload
            if file_format == "csv":
                buffer = StringIO()
                df.to_csv(buffer, index=False, compression=compression)
                content_type = 'text/csv'
                file_content = buffer.getvalue()
            elif file_format == "parquet":
                buffer = BytesIO()
                df.to_parquet(buffer, compression=compression)
                content_type = 'application/octet-stream'
                buffer.seek(0)
                file_content = buffer.getvalue()
            elif file_format == "json":
                buffer = StringIO()
                df.to_json(buffer, orient='records', lines=True)
                content_type = 'application/json'
                file_content = buffer.getvalue()
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
            
            # Upload to S3
            logger.info(f"Uploading to S3: {bucket_name}/{key}")
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=file_content,
                ContentType=content_type
            )
            
            # Update metadata
            self.metadata["bytes_written"] = len(file_content)
            self.metadata["format"] = file_format
            self.metadata["compression"] = compression
            self.metadata["path"] = f"s3://{bucket_name}/{key}"
            logger.info(f"Successfully uploaded {self.metadata['bytes_written']} bytes to {self.metadata['path']}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to S3: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the S3 storage operation.
        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        return self.metadata


class LocalStorageManager(StorageManager):
    """Storage manager for local filesystem destinations"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize local storage manager.
        Args:
            config: Dictionary containing configuration:
                - output_dir: Output directory (default: current directory)
                - file_format: Output format (default: csv)
                - compression: Compression type (None, gzip, etc.)
        """
        self.config = config
        self.metadata = {
            "bytes_written": 0,
            "format": config.get("file_format", "csv"),
            "compression": config.get("compression", None),
            "path": None
        }
        # Set default configuration
        self.config.setdefault("output_dir", ".")
        self.config.setdefault("file_format", "csv")
        # Ensure output directory exists
        os.makedirs(self.config["output_dir"], exist_ok=True)
    
    def save_data(self, df: pd.DataFrame, destination: str, **kwargs) -> bool:
        """
        Save DataFrame to local filesystem.
        Args:
            df: DataFrame to save
            destination: File path (relative or absolute)
            **kwargs: Additional arguments:
                - file_format: Override configured format
                - compression: Override configured compression
        Returns:
            bool: True if save successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to save")
                return True
            
            # Get format and compression
            file_format = kwargs.get("file_format", self.config.get("file_format", "csv"))
            compression = kwargs.get("compression", self.config.get("compression", None))
            
            # Determine file path
            if os.path.isabs(destination):
                file_path = destination
            else:
                file_path = os.path.join(self.config["output_dir"], destination)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Save data
            logger.info(f"Saving data to: {file_path}")
            if file_format == "csv":
                df.to_csv(file_path, index=False, compression=compression)
            elif file_format == "parquet":
                df.to_parquet(file_path, compression=compression)
            elif file_format == "json":
                df.to_json(file_path, orient='records', lines=True)
            else:
                logger.error(f"Unsupported file format: {file_format}")
                return False
            
            # Update metadata
            self.metadata["bytes_written"] = os.path.getsize(file_path)
            self.metadata["format"] = file_format
            self.metadata["compression"] = compression
            self.metadata["path"] = file_path
            logger.info(f"Successfully saved {self.metadata['bytes_written']} bytes to {self.metadata['path']}")
            return True
        except Exception as e:
            logger.error(f"Error saving data to local file: {str(e)}")
            return False
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the local storage operation.
        Returns:
            Dict[str, Any]: Metadata dictionary
        """
        return self.metadata
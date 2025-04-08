import io
import hashlib
import json
from typing import BinaryIO, Union, List
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


class AzureStorage:
    """Azure Blob Storage implementation"""

    def __init__(self, connection_string: str, container_name: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

        # Ensure container exists
        if not self.container_client.exists():
            self.container_client.create_container()

    @staticmethod
    def hex_to_path(digest: str) -> str:
        """Convert hash to path structure"""
        return f"{digest[0:2]}/{digest[2:4]}/{digest}"

    def save_cas(self, stream: BinaryIO) -> str:
        """Save content-addressable storage and return hash"""
        # Calculate hash
        sha256 = hashlib.sha256()
        buffer = io.BytesIO()

        while True:
            data = stream.read(65536)
            if not data:
                break
            sha256.update(data)
            buffer.write(data)

        digest = sha256.hexdigest()
        blob_path = f"_cas/{self.hex_to_path(digest)}"

        # Check if blob exists
        blob_client = self.container_client.get_blob_client(blob_path)
        if not blob_client.exists():
            # Upload if it doesn't exist
            buffer.seek(0)
            blob_client.upload_blob(buffer, overwrite=False)

        return digest

    def read_cas(self, hash_digest: str) -> BinaryIO:
        """Read content from CAS by hash"""
        blob_path = f"_cas/{self.hex_to_path(hash_digest)}"
        blob_client = self.container_client.get_blob_client(blob_path)

        download_stream = io.BytesIO()
        download_stream.write(blob_client.download_blob().readall())
        download_stream.seek(0)
        return download_stream

    def cas_exists(self, hash_digest: str) -> bool:
        """Check if hash exists in CAS"""
        blob_path = f"_cas/{self.hex_to_path(hash_digest)}"
        return self.container_client.get_blob_client(blob_path).exists()

    def read_mutable_data(self, step_name: str, file_name: str) -> BinaryIO:
        """Read mutable data by step and filename"""
        blob_path = f"data/{step_name}/{file_name}"
        blob_client = self.container_client.get_blob_client(blob_path)

        download_stream = io.BytesIO()
        download_stream.write(blob_client.download_blob().readall())
        download_stream.seek(0)
        return download_stream

    def write_mutable_data(
        self, step_name: str, file_name: str, data: Union[BinaryIO, bytes, str]
    ) -> None:
        """Write mutable data"""
        blob_path = f"data/{step_name}/{file_name}"
        blob_client = self.container_client.get_blob_client(blob_path)

        if isinstance(data, bytes):
            blob_client.upload_blob(data, overwrite=True)
        elif isinstance(data, str):
            blob_client.upload_blob(data.encode("utf-8"), overwrite=True)
        else:
            blob_client.upload_blob(data, overwrite=True)

    def mutable_data_exists(self, step_name: str, file_name: str) -> bool:
        """Check if mutable data exists"""
        blob_path = f"data/{step_name}/{file_name}"
        return self.container_client.get_blob_client(blob_path).exists()

    def list_files(self, step_name: str, prefix: str) -> List[str]:
        """List files with prefix"""
        blob_prefix = f"data/{step_name}/{prefix}"
        blobs = self.container_client.list_blobs(name_starts_with=blob_prefix)

        # Strip prefix from blob names
        prefix_len = len(f"data/{step_name}/")
        return [blob.name[prefix_len:] for blob in blobs]

    # DataFrame helpers
    def save_df(self, step_name: str, file_name: str, df: pd.DataFrame) -> None:
        """Save DataFrame as CSV"""
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.write_mutable_data(step_name, file_name, csv_buffer.getvalue())

    def load_df(self, step_name: str, file_name: str) -> pd.DataFrame:
        """Load DataFrame from CSV"""
        with self.read_mutable_data(step_name, file_name) as f:
            return pd.read_csv(io.BytesIO(f.read()))

    # JSON helpers
    def save_json(self, step_name: str, file_name: str, data: dict) -> None:
        """Save dictionary as JSON"""
        json_str = json.dumps(data)
        self.write_mutable_data(step_name, file_name, json_str)

    def load_json(self, step_name: str, file_name: str) -> dict:
        """Load JSON as dictionary"""
        with self.read_mutable_data(step_name, file_name) as f:
            return json.load(io.BytesIO(f.read()))

    def load_mutable_text(self, step_name: str, file_name: str) -> str:
        """Load text content from a mutable file as a string"""
        with self.read_mutable_data(step_name, file_name) as f:
            return f.read().decode("utf-8")

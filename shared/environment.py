import os
import logging
from typing import Optional, Dict, Any, List

from .storage import AzureStorage


class AzureEnvironment:
    """Environment configuration for Azure Functions"""

    def __init__(
        self,
        storage: Optional[AzureStorage] = None,
        log: Optional[logging.Logger] = None,
        flags: List[str] = None,
    ):
        """Initialize the environment with storage and logging"""
        self.storage = storage or self._create_default_storage()
        self.log = log or logging.getLogger(__name__)
        self.flags = flags or []
        self.context = {}  # For passing data between functions

    def _create_default_storage(self) -> AzureStorage:
        """Create default Azure Storage from environment variables"""
        connection_string = os.environ["AzureWebJobsStorage"]
        container_name = os.environ.get("STORAGE_CONTAINER", "power-converter-data")
        return AzureStorage(connection_string, container_name)

    def get_config(self, key: str, default: str = None) -> str:
        return os.environ.get(key, default or "")

    @classmethod
    def from_context(cls, context: Dict[str, Any]) -> "AzureEnvironment":
        """Create environment from Azure Function context"""
        # Check if context has a log attribute
        log = getattr(context, "log", None)
        if log is None and isinstance(context, dict):
            # Try accessing as dictionary
            log = context.get("log")

        # Fall back to default logger if not found
        if log is None:
            log = logging.getLogger(__name__)

        return cls(log=log)

from typing import Dict, Any
from abc import ABC, abstractmethod

class CleanUper(ABC):
    @abstractmethod
    def clean_up(self, tool_params: Dict[str, Any]):
        """Clean up resources after a test case."""
        pass
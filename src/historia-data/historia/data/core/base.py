from abc import ABC, abstractmethod
from typing import List, Set, Dict, Any
from dataclasses import dataclass
import yaml
from sentence_transformers import SentenceTransformer
from historia.ml.embedder import Embedder
from historia.data.core.snipper import Snipper

@dataclass
class TextDocument:
    title: str
    content: str
    metadata: Dict[str, Any]
    url: str


@dataclass
class YAMLConfig:
    source_name: str
    base_url: str
    query_params: Dict[str, Any]


class DataSource(ABC):
    """
    Abstract class for Data Sources that generate URLs, convert them to TextDocuments,
    and manage ingestion and indexing of those documents.
    """

    def __init__(self, config_path: str):
        self.config = self.load_config(config_path)

    @staticmethod
    def load_config(config_path: str) -> YAMLConfig:
        with open(config_path, "r") as file:
            data = yaml.safe_load(file)
        return YAMLConfig(**data)

    @abstractmethod
    def generate_urls(self) -> List[str]:
        """Generate a list of URLs to process."""
        pass

    @abstractmethod
    def urls_to_text_documents(self, urls: List[str]) -> Set[TextDocument]:
        """Convert URLs to a set of TextDocuments."""
        pass

    @abstractmethod
    def write_documents_to_database(self, documents: Set[TextDocument]):
        """Write TextDocuments to the database."""
        pass

    @abstractmethod
    def index_documents(self, index_name: str, snipper: Snipper, embedder: Embedder):
        """Embed document snippets and write them back to the database."""
        pass
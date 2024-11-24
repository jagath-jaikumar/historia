from abc import ABC, abstractmethod
from typing import List


class Embedder(ABC):
    """Abstract class for embedding text."""

    @abstractmethod
    def embed(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of texts and return a list of embeddings."""
        pass


class DummyEmbedder(Embedder):
    """Dummy embedder for testing."""

    def embed(self, texts: List[str]) -> List[List[float]]:
        return [[0.0] * 768] * len(texts)

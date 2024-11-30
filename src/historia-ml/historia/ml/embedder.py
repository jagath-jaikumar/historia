from abc import ABC, abstractmethod
from typing import List

import ollama


class Embedder(ABC):
    """Abstract class for embedding text."""

    @abstractmethod
    def embed(self, texts: str | List[str]) -> List[List[float]]:
        """Embed a list of texts and return a list of embeddings."""
        pass


class DummyEmbedder(Embedder):
    """Dummy embedder for testing."""

    def embed(self, texts: str | List[str]) -> List[List[float]]:
        return [[0.0] * 768] * len(texts)


class OllamaEmbedder(Embedder):
    """Ollama embedder."""

    def __init__(self, model: str = "nomic-embed-text"):
        self.model = model

    def embed(self, texts: str | List[str]) -> List[List[float]]:
        if isinstance(texts, str):
            return ollama.embeddings(model=self.model, prompt=texts)

        embeddings = []
        for text in texts:
            embedding = ollama.embeddings(model=self.model, prompt=text)
            embeddings.extend(embedding)
        return embeddings

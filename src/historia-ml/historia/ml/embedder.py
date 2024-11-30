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
        if isinstance(texts, str):
            return [[0.0] * 768]
        return [[0.0] * 768] * len(texts)


class OllamaEmbedder(Embedder):
    """Ollama embedder."""

    def __init__(self, model: str = "nomic-embed-text"):
        self.model = model

    def embed(self, texts: str | List[str]) -> List[List[float]]:
        if isinstance(texts, str):
            response = ollama.embeddings(model=self.model, prompt=texts)
            return [response.embedding]

        embeddings = []
        for text in texts:
            response = ollama.embeddings(model=self.model, prompt=text)
            embeddings.append(response.embedding)
        return embeddings


if __name__ == "__main__":
    embedder = OllamaEmbedder()
    embedding = embedder.embed("Hello, world!")
    print(embedding)
    print(len(embedding[0]))

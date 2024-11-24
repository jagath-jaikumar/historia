from abc import ABC, abstractmethod
from typing import Generator


class Snipper(ABC):
    """Abstract class for document snippet generation."""

    @abstractmethod
    def generate_snippets(self, content: str) -> Generator[str, None, None]:
        """Generate snippets from document content."""
        pass


class SimpleSnipper(Snipper):
    """A simple snipper that generates fixed-length snippets."""

    def __init__(self, snippet_length: int = 200):
        self.snippet_length = snippet_length

    def generate_snippets(self, content: str) -> Generator[str, None, None]:
        """Split the content into snippets of fixed length."""
        for i in range(0, len(content), self.snippet_length):
            yield content[i : i + self.snippet_length]

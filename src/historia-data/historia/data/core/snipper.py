from abc import ABC, abstractmethod
from typing import Generator


class Snipper(ABC):
    """Abstract class for document snippet generation."""

    @abstractmethod
    def generate_snippets(self, content: str) -> Generator[str, None, None]:
        """Generate snippets from document content."""
        pass


class SimpleSnipper(Snipper):
    """A snipper that generates fixed-length snippets based on word count."""

    def __init__(self, snippet_length: int = 300):
        self.snippet_length = snippet_length

    def generate_snippets(self, content: str) -> Generator[str, None, None]:
        """Generate snippets by splitting content into fixed-length word chunks."""
        words = content.split()

        for i in range(0, len(words), self.snippet_length):
            snippet = " ".join(words[i : i + self.snippet_length])
            if snippet:  # Only yield non-empty snippets
                yield snippet


class BasicParagraphSnipper(Snipper):
    """A snipper that generates snippets based on paragraphs and sentences."""

    def __init__(self, max_tokens: int = 200, min_tokens: int | None = None):
        self.max_tokens = max_tokens
        self.min_tokens = min_tokens if min_tokens is not None else max_tokens // 2

    def _find_sentence_boundary(self, text: str, max_pos: int, min_pos: int = 0) -> int:
        """Find the closest sentence boundary before max_pos but after min_pos."""
        sentence_endings = [". ", "! ", "? "]
        best_pos = 0

        # First try to find sentence boundary between min and max
        for i in range(min_pos, min(max_pos, len(text))):
            if text[i : i + 2] in sentence_endings:
                best_pos = i + 2
                if best_pos >= min_pos:
                    return best_pos

        # If we can't find a suitable sentence ending, try space
        if max_pos - 1 >= min_pos:
            space_pos = text[min_pos:max_pos].rfind(" ")
            if space_pos > 0:
                best_pos = min_pos + space_pos + 1

        # If we still haven't found a good boundary, use max_pos
        return best_pos if best_pos >= min_pos else max_pos

    def generate_snippets(self, content: str) -> Generator[str, None, None]:
        """Generate snippets by paragraphs, falling back to sentences if needed."""
        current_pos = 0

        while current_pos < len(content):
            # Try to find next paragraph boundary within max_tokens
            next_para = content.find("\n\n", current_pos, current_pos + self.max_tokens)

            if next_para != -1:
                # Found paragraph boundary within limit
                snippet = content[current_pos:next_para].strip()
                # Only yield if snippet meets min_tokens requirement
                if snippet and len(snippet.split()) >= self.min_tokens:
                    yield snippet
                current_pos = next_para + 2
            else:
                # No paragraph boundary found, try sentence boundary
                next_pos = self._find_sentence_boundary(
                    content[current_pos:], self.max_tokens
                )
                snippet = content[current_pos : current_pos + next_pos].strip()
                # Only yield if snippet meets min_tokens requirement
                if snippet and len(snippet.split()) >= self.min_tokens:
                    yield snippet
                current_pos += next_pos

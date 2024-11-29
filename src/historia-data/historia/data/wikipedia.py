from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Set

import wikipediaapi
from django.db import transaction

from historia.data.core.base import DataSource, TextDocument
from historia.data.core.snipper import Snipper
from historia.indexing.models import (
    Embedding,
    Index,
    IndexedDocumentSnippet,
    WikipediaDocument,
)
from historia.ml.embedder import Embedder

PAGE_ID_URL_BASE = "https://en.wikipedia.org/?curid="

wiki_wiki = wikipediaapi.Wikipedia(
    user_agent="Historia", language="en", extract_format=wikipediaapi.ExtractFormat.WIKI
)

page_url_to_content = {}


class WikipediaDataSource(DataSource):
    """Implementation of DataSource for Wikipedia."""

    def __init__(self, params: Dict[str, Any]):
        """
        Initialize the WikipediaDataSource with parameters.

        Args:
            params (Dict[str, Any]): Dictionary containing configuration parameters.
                - base_url (str): The base URL for Wikipedia (e.g., "https://en.wikipedia.org/wiki").
                - topics (List[str]): A list of topics to fetch.
        """
        self.base_url = params.get("base_url", "https://en.wikipedia.org/wiki")
        self.categories = params.get("categories", [])
        self.depth = params.get("depth", 1)
        if not self.categories:
            raise ValueError("No categories specified for WikipediaDataSource.")

    def generate_urls(self) -> List[str]:
        """Generate a list of URLs for the given topics."""

        def get_categorymembers(categorymembers, level=0, max_level=1, results=None):
            if results is None:
                results = set()

            for c in categorymembers.values():
                if c.ns == wikipediaapi.Namespace.MAIN and c.exists():
                    results.add(c)
                    page_url_to_content[f"{PAGE_ID_URL_BASE}{c.pageid}"] = (
                        c.title,
                        c.text,
                    )
                elif c.ns == wikipediaapi.Namespace.CATEGORY and level < max_level:
                    get_categorymembers(
                        c.categorymembers,
                        level=level + 1,
                        max_level=max_level,
                        results=results,
                    )

            return results

        def process_category(category):
            cat = wiki_wiki.page(category)
            main_pages = get_categorymembers(cat.categorymembers)
            return [f"{PAGE_ID_URL_BASE}{page.pageid}" for page in main_pages]

        with ThreadPoolExecutor() as executor:
            url_lists = list(executor.map(process_category, self.categories))

        urls = [url for sublist in url_lists for url in sublist]
        return urls

    def urls_to_text_documents(self, urls: List[str]) -> Set[TextDocument]:
        """
        Convert URLs to a set of TextDocuments.
        """
        documents = set()

        def create_document(url):
            title, content = page_url_to_content[url]
            return TextDocument(
                title=title, content=content, metadata={"url": url}, url=url
            )

        with ThreadPoolExecutor() as executor:
            documents.update(executor.map(create_document, urls))

        return documents

    def write_documents_to_database(
        self, documents: Set[TextDocument], no_db: bool = False
    ):
        """
        Write TextDocuments to the database or log transactions if `no_db` is True.

        Args:
            documents (Set[TextDocument]): A set of TextDocument objects to store.
            no_db (bool): If True, log database actions instead of executing them.
        """
        if no_db:
            for doc in documents:
                print(f"[NO-DB] Would write to WikipediaDocument: {doc}")
        else:
            with transaction.atomic():
                for doc in documents:
                    WikipediaDocument.objects.update_or_create(
                        url=doc.url,
                        defaults={
                            "title": doc.title,
                            "content": doc.content,
                            "metadata": doc.metadata,
                        },
                    )

    def index_documents(
        self, index_name: str, snipper: Snipper, embedder: Embedder, no_db: bool = False
    ):
        """
        Embed document snippets and write them to the database or log transactions if `no_db` is True.

        Args:
            index_name (str): The name of the index.
            snipper (Snipper): A Snipper object to generate snippets from document content.
            embedder (Embedder): An Embedder object to generate embeddings for snippets.
            no_db (bool): If True, log database actions instead of executing them.
        """
        if no_db:
            print(f"[NO-DB] Would index documents in index: {index_name}")
        else:
            index, _ = Index.objects.get_or_create(
                name=index_name, defaults={"dimensions": 768}
            )
            documents = WikipediaDocument.objects.all()

            with transaction.atomic():
                for doc in documents:
                    snippets = list(snipper.generate_snippets(doc.content))
                    embeddings = embedder.embed(snippets)

                    for snippet, embedding in zip(snippets, embeddings):
                        embedding_instance = Embedding.objects.create(
                            embedding=embedding, dimensions=len(embedding)
                        )
                        IndexedDocumentSnippet.objects.create(
                            index=index,
                            document=doc,
                            snippet=snippet,
                            embedding=embedding_instance,
                        )

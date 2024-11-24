from typing import List, Dict, Any, Set
from historia.data.core.base import DataSource, TextDocument
from django.db import transaction
from historia.indexing.models import WikipediaDocument, Index, Embedding, IndexedDocumentSnippet


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
        self.topics = params.get("topics", [])
        if not self.topics:
            raise ValueError("No topics specified for WikipediaDataSource.")

    def generate_urls(self) -> List[str]:
        """Generate a list of URLs for the given topics."""
        urls = [f"{self.base_url}/{topic}" for topic in self.topics]
        return urls

    def urls_to_text_documents(self, urls: List[str]) -> Set[TextDocument]:
        """
        Convert URLs to a set of TextDocuments.
        
        Simulates content fetching for demonstration purposes.
        """
        documents = set()
        for url in urls:
            # Simulated content fetching for demonstration purposes
            content = f"Content fetched from {url}"
            title = url.split("/")[-1].replace("_", " ").title()
            documents.add(TextDocument(title=title, content=content, metadata={"url": url}, url=url))
        return documents

    def write_documents_to_database(self, documents: Set[TextDocument], no_db: bool = False):
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

    def index_documents(self, index_name: str, snipper: "Snipper", embedder: "Embedder", no_db: bool = False):
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
            index, _ = Index.objects.get_or_create(name=index_name, defaults={"dimensions": 768})
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

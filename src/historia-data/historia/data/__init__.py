import logging
from typing import Dict, Type
import yaml

from historia.data.core.base import DataSource, Snipper
from historia.data.wikipedia import WikipediaDataSource
from historia.ml.embedder import Embedder, DummyEmbedder # type: ignore
from historia.data.core.snipper import SimpleSnipper


class EntryPoint:
    """Entry point to manage DataSource ingestion and indexing pipelines."""

    # Hardcoded registry of data sources, snippers, and embedders
    DATA_SOURCE_REGISTRY: Dict[str, Type["DataSource"]] = {
        "wikipedia": WikipediaDataSource,
        # Add more data sources here as needed
    }

    SNIPPER_REGISTRY: Dict[str, Type["Snipper"]] = {
        "simple": SimpleSnipper,
        # Add more snippers here as needed
    }

    EMBEDDER_REGISTRY: Dict[str, Type["Embedder"]] = {
        "dummy": DummyEmbedder,
        # Add more embedders here as needed
    }

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.logger = logging.getLogger("DataSourcePipeline")
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def load_config(self, config_path: str) -> Dict:
        """Loads the YAML configuration file."""
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        self.logger.info(f"Configuration loaded from {config_path}.")
        return config

    def initialize_data_source(self, config: Dict) -> "DataSource":
        """Initialize the data source based on the configuration."""
        data_source_name = config.get("data_source")
        if not data_source_name or data_source_name not in self.DATA_SOURCE_REGISTRY:
            raise Exception(f"Unknown or missing data source: {data_source_name}")

        data_source_cls = self.DATA_SOURCE_REGISTRY[data_source_name]
        self.logger.info(f"Initializing DataSource: {data_source_name}")
        return data_source_cls(config.get("data_source_params", {}))

    def initialize_snipper(self, config: Dict) -> Snipper:
        """Initialize the snipper based on the configuration."""
        snipper_name = config.get("snipper", {}).get("type")
        if not snipper_name or snipper_name not in self.SNIPPER_REGISTRY:
            raise Exception(f"Unknown or missing snipper: {snipper_name}")

        snipper_cls = self.SNIPPER_REGISTRY[snipper_name]
        snipper_params = config.get("snipper", {}).get("params", {})
        self.logger.info(f"Initializing Snipper: {snipper_name}")
        return snipper_cls(**snipper_params)

    def initialize_embedder(self, config: Dict) -> Embedder:
        """Initialize the embedder based on the configuration."""
        embedder_name = config.get("embedder", {}).get("type")
        if not embedder_name or embedder_name not in self.EMBEDDER_REGISTRY:
            raise Exception(f"Unknown or missing embedder: {embedder_name}")

        embedder_cls = self.EMBEDDER_REGISTRY[embedder_name]
        embedder_params = config.get("embedder", {}).get("params", {})
        self.logger.info(f"Initializing Embedder: {embedder_name}")
        return embedder_cls(**embedder_params)

    def run_pipeline(self, config_path: str, no_db: bool):
        """Runs the ingestion and indexing pipeline using a YAML configuration file."""
        config = self.load_config(config_path)
        data_source = self.initialize_data_source(config)
        snipper = self.initialize_snipper(config)
        embedder = self.initialize_embedder(config)
        index_name = config.get("index_name")

        if not index_name:
            raise Exception("Missing index name in configuration.")

        self.logger.info(f"Starting pipeline for index: {index_name}")
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(
                    f"[Attempt {attempt}/{self.max_retries}] Generating URLs..."
                )
                urls = data_source.generate_urls()
                self.logger.info(f"Generated {len(urls)} URLs.")

                self.logger.info(
                    f"[Attempt {attempt}/{self.max_retries}] Converting URLs to TextDocuments..."
                )
                documents = data_source.urls_to_text_documents(urls)
                self.logger.info(f"Converted {len(documents)} TextDocuments.")

                self.logger.info(
                    f"[Attempt {attempt}/{self.max_retries}] Writing TextDocuments to database..."
                )
                data_source.write_documents_to_database(documents, no_db=no_db)
                self.logger.info(f"Successfully processed {len(documents)} documents.")

                self.logger.info(
                    f"[Attempt {attempt}/{self.max_retries}] Indexing documents..."
                )
                data_source.index_documents(index_name, snipper, embedder, no_db=no_db)
                self.logger.info("Indexing completed successfully.")

                self.logger.info("Pipeline completed successfully.")
                break
            except Exception as e:
                self.logger.error(f"Error during pipeline execution: {e}")
                if attempt == self.max_retries:
                    self.logger.critical("Pipeline failed after maximum retries.")
                    raise Exception("Pipeline execution failed.") from e
                else:
                    self.logger.warning("Retrying pipeline...")

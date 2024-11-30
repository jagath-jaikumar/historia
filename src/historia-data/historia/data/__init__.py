import datetime
import logging
import os
import traceback
from typing import Callable, Dict, List, Optional, Tuple, Type

import apache_beam as beam
import yaml
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from historia.django.utils import initialize_django

initialize_django()

from historia.data.core.base import DataSource, Snipper  # noqa E402
from historia.data.core.snipper import SimpleSnipper  # noqa E402
from historia.data.wikipedia import WikipediaDataSource  # noqa E402
from historia.ml.embedder import DummyEmbedder, Embedder  # noqa E402
from historia.indexing import models  # noqa E402

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_ROOT = os.path.join(HERE, "configs")
YAML_FILE_EXTENSION = ".yaml"


def default_failure_callback(failed_urls: List[str], error: Exception, step: str):
    """Default callback that writes failed URLs to a file."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"failed_urls_{step}_{timestamp}.txt"
    with open(filename, "w") as f:
        f.write(f"Failed during step: {step}\n")
        f.write(f"Error: {str(error)}\n")
        f.write(f"Stacktrace:\n{traceback.format_exc()}\n")
        f.write("\nFailed URLs:\n")
        for url in failed_urls:
            f.write(f"{url}\n")


class GenerateUrlsFn(beam.DoFn):
    def __init__(self, data_source: DataSource, use_all: bool = False):
        self.data_source = data_source
        self.use_all = use_all

    def process(self, _):
        urls = self.data_source.generate_urls(use_all=self.use_all)
        for url in urls:
            yield url


class UrlToDocumentFn(beam.DoFn):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    def process(self, url):
        try:
            docs = self.data_source.urls_to_text_documents([url])
            for doc in docs:
                yield beam.pvalue.TaggedOutput("success", doc)
        except Exception as e:
            yield beam.pvalue.TaggedOutput("failed", (url, str(e)))


class WriteToDBFn(beam.DoFn):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source

    def process(self, doc):
        try:
            self.data_source.write_documents_to_database({doc})
            yield beam.pvalue.TaggedOutput("success", doc)
        except Exception as e:
            yield beam.pvalue.TaggedOutput("failed", (doc.url, str(e)))


class IndexDocumentFn(beam.DoFn):
    def __init__(
        self,
        data_source: DataSource,
        index_name: str,
        snipper: Snipper,
        embedder: Embedder,
    ):
        self.data_source = data_source
        self.index_name = index_name
        self.snipper = snipper
        self.embedder = embedder

    def process(self, doc):
        try:
            self.data_source.index_documents(
                self.index_name, self.snipper, self.embedder
            )
            yield beam.pvalue.TaggedOutput("success", doc.url)
        except Exception as e:
            yield beam.pvalue.TaggedOutput("failed", (doc.url, str(e)))


class PipelineEntryPoint:
    """Base entry point to manage DataSource ingestion and indexing pipelines."""

    DATA_SOURCE_REGISTRY: Dict[str, Type[DataSource]] = {
        "wikipedia": WikipediaDataSource,
    }

    SNIPPER_REGISTRY: Dict[str, Type[Snipper]] = {
        "simple": SimpleSnipper,
    }

    EMBEDDER_REGISTRY: Dict[str, Type[Embedder]] = {
        "dummy": DummyEmbedder,
    }

    def __init__(
        self, max_retries: int = 3, failure_callback: Optional[Callable] = None
    ):
        self.max_retries = max_retries
        self.failure_callback = failure_callback or default_failure_callback
        self.logger = logging.getLogger("DataSourcePipeline")
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def load_config(self, config_path: str) -> Dict:
        """Loads the YAML configuration file."""
        with open(
            os.path.join(CONFIG_ROOT, config_path + YAML_FILE_EXTENSION), "r"
        ) as file:
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

    def handle_failures(self, failures: List[Tuple[str, str]], step: str):
        if failures:
            failed_urls = [f[0] for f in failures]
            error = Exception(f"Failed during {step}: {failures[0][1]}")
            self.failure_callback(failed_urls, error, step)
    
    def get_document_data_model(self, config: Dict) -> Type[models.Document]:
        document_data_model = config.get("document_data_model", "Document")
        return getattr(models, document_data_model)


    def get_documents_to_index(self, urls: List[str], document_data_model: Type[models.Document]) -> set[models.Document]:
        docs = document_data_model.objects.filter(url__in=urls)
        return set(docs)

    def run_pipeline(self, config_path: str, use_all: bool):
        """Abstract method to be implemented by subclasses."""
        raise NotImplementedError


class BeamEntryPoint(PipelineEntryPoint):
    """Apache Beam based parallel pipeline implementation."""

    def run_pipeline(self, config_path: str, use_all: bool):
        """Runs the ingestion and indexing pipeline using Apache Beam."""
        config = self.load_config(config_path)
        data_source = self.initialize_data_source(config)
        snipper = self.initialize_snipper(config)
        embedder = self.initialize_embedder(config)
        index_name = config.get("index_name")
        document_data_model = self.get_document_data_model(config)

        if not index_name:
            raise Exception("Missing index name in configuration.")

        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = False

        with beam.Pipeline(options=pipeline_options) as p:
            # Generate URLs
            urls = (
                p
                | "Create" >> beam.Create([None])
                | "Generate URLs" >> beam.ParDo(GenerateUrlsFn(data_source, use_all))
            )

            # Convert URLs to documents
            url_results = urls | "URLs to Documents" >> beam.ParDo(
                UrlToDocumentFn(data_source)
            ).with_outputs("success", "failed")

            documents = url_results.success
            url_failures = (
                url_results.failed
                | "Collect URL Failures" >> beam.transforms.combiners.ToList()
            )

            # Write documents to database
            db_results = documents | "Write to DB" >> beam.ParDo(
                WriteToDBFn(data_source)
            ).with_outputs("success", "failed")

            successful_docs = db_results.success
            db_failures = (
                db_results.failed
                | "Collect DB Failures" >> beam.transforms.combiners.ToList()
            )

            # Get all documents to index and chunk them
            documents_to_index = (
                successful_docs
                | "Get Documents to Index" >> beam.Map(lambda x: self.get_documents_to_index(x))
                | "Chunk Documents" >> beam.transforms.util.BatchElements(
                    min_batch_size=100, max_batch_size=1000
                )
            )

            # Index document chunks in parallel
            index_results = documents_to_index | "Index Documents" >> beam.ParDo(
                IndexDocumentFn(data_source, index_name, snipper, embedder)
            ).with_outputs("success", "failed")

            index_failures = (
                index_results.failed
                | "Collect Index Failures" >> beam.transforms.combiners.ToList()
            )

            # Handle failures after pipeline completion
            def handle_all_failures(url_fails, db_fails, index_fails):
                self.handle_failures(url_fails, "url_to_documents")
                self.handle_failures(db_fails, "write_documents")
                self.handle_failures(index_fails, "indexing")

                total_failures = len(url_fails) + len(db_fails) + len(index_fails)
                if total_failures > 0:
                    self.logger.info("Pipeline completed with some failures")
                else:
                    self.logger.info("Pipeline completed successfully")

            _ = (
                (url_failures, db_failures, index_failures)
                | "Combine Failures" >> beam.CoGroupByKey()
                | "Handle Failures" >> beam.Map(lambda x: handle_all_failures(*x))
            )


class DirectEntryPoint(PipelineEntryPoint):
    """Direct sequential pipeline implementation for debugging."""
    
    def run_pipeline(self, config_path: str, use_all: bool):
        """Runs the ingestion and indexing pipeline sequentially."""
        self.logger.info(f"Starting pipeline with config from {config_path}")
        config = self.load_config(config_path)
        data_source = self.initialize_data_source(config)
        snipper = self.initialize_snipper(config)
        embedder = self.initialize_embedder(config)
        index_name = config.get("index_name")
        document_data_model = self.get_document_data_model(config)

        if not index_name:
            raise Exception("Missing index name in configuration.")

        url_failures = []
        db_failures = []
        index_failures = []

        # Generate URLs
        self.logger.info("Generating URLs...")
        urls = data_source.generate_urls(use_all=use_all)
        url_count = sum(1 for _ in urls)  # Count URLs without consuming generator
        urls = data_source.generate_urls(use_all=use_all)  # Regenerate
        self.logger.info(f"Found {url_count} URLs to process")

        # Process each URL sequentially
        processed_count = 0
        for url in urls:
            processed_count += 1
            self.logger.debug(f"Processing URL {processed_count}/{url_count}: {url}")
            try:
                # Convert URL to document
                docs = data_source.urls_to_text_documents([url])
                self.logger.debug(f"Successfully extracted {len(docs)} documents from {url}")
                
                for doc in docs:
                    try:
                        # Write to database
                        self.logger.debug(f"Writing document {doc.url} to database")
                        data_source.write_documents_to_database({doc})
                        self.logger.debug(f"Successfully wrote {doc.url} to database")
                        
                        try:
                            # Index document
                            self.logger.debug(f"Indexing document {doc.url}")
                            documents_to_index = self.get_documents_to_index([doc.url], document_data_model)
                            data_source.index_documents(
                                documents_to_index, index_name, snipper, embedder
                            )
                            self.logger.debug(f"Successfully indexed {doc.url}")
                        except Exception as e:
                            self.logger.error(f"Failed to index document {doc.url}: {str(e)}")
                            index_failures.append((doc.url, str(e)))
                            
                    except Exception as e:
                        self.logger.error(f"Failed to write document {doc.url} to database: {str(e)}")
                        db_failures.append((doc.url, str(e)))
                        
            except Exception as e:
                self.logger.error(f"Failed to process URL {url}: {str(e)}")
                url_failures.append((url, str(e)))

        # Handle all failures
        self.logger.info(f"Processing complete. Processing results:")
        self.logger.info(f"Processed {processed_count} URLs total")
        self.handle_failures(url_failures, "url_to_documents")
        self.handle_failures(db_failures, "write_documents") 
        self.handle_failures(index_failures, "indexing")

        total_failures = len(url_failures) + len(db_failures) + len(index_failures)
        if total_failures > 0:
            self.logger.info(f"Pipeline completed with {total_failures} total failures:")
            self.logger.info(f"- URL processing failures: {len(url_failures)}")
            self.logger.info(f"- Database write failures: {len(db_failures)}")
            self.logger.info(f"- Indexing failures: {len(index_failures)}")
        else:
            self.logger.info("Pipeline completed successfully with no failures")

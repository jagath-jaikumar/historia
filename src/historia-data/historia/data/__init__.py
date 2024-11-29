import datetime
import logging
import os
from typing import Dict, Type, List, Callable, Optional, Tuple

import apache_beam as beam
import yaml
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from historia.django.utils import initialize_django

initialize_django()

from historia.data.core.base import DataSource, Snipper  # noqa E402
from historia.data.core.snipper import SimpleSnipper  # noqa E402
from historia.data.wikipedia import WikipediaDataSource  # noqa E402
from historia.ml.embedder import DummyEmbedder, Embedder  # noqa E402

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
        f.write("\nFailed URLs:\n")
        for url in failed_urls:
            f.write(f"{url}\n")


class GenerateUrlsFn(beam.DoFn):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        
    def process(self, _):
        urls = self.data_source.generate_urls()
        for url in urls:
            yield url


class UrlToDocumentFn(beam.DoFn):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        
    def process(self, url):
        try:
            docs = self.data_source.urls_to_text_documents([url])
            for doc in docs:
                yield beam.pvalue.TaggedOutput('success', doc)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('failed', (url, str(e)))


class WriteToDBFn(beam.DoFn):
    def __init__(self, data_source: DataSource, no_db: bool):
        self.data_source = data_source
        self.no_db = no_db
        
    def process(self, doc):
        try:
            self.data_source.write_documents_to_database({doc}, no_db=self.no_db)
            yield beam.pvalue.TaggedOutput('success', doc)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('failed', (doc.url, str(e)))


class IndexDocumentFn(beam.DoFn):
    def __init__(self, data_source: DataSource, index_name: str, 
                 snipper: Snipper, embedder: Embedder, no_db: bool):
        self.data_source = data_source
        self.index_name = index_name
        self.snipper = snipper
        self.embedder = embedder
        self.no_db = no_db
        
    def process(self, doc):
        try:
            self.data_source.index_documents(
                self.index_name, self.snipper, self.embedder, 
                no_db=self.no_db, doc=doc
            )
            yield beam.pvalue.TaggedOutput('success', doc.url)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('failed', (doc.url, str(e)))


class EntryPoint:
    """Entry point to manage DataSource ingestion and indexing pipelines."""

    DATA_SOURCE_REGISTRY: Dict[str, Type[DataSource]] = {
        "wikipedia": WikipediaDataSource,
    }

    SNIPPER_REGISTRY: Dict[str, Type[Snipper]] = {
        "simple": SimpleSnipper,
    }

    EMBEDDER_REGISTRY: Dict[str, Type[Embedder]] = {
        "dummy": DummyEmbedder,
    }

    def __init__(self, max_retries: int = 3, failure_callback: Optional[Callable] = None):
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
        with open(os.path.join(CONFIG_ROOT, config_path + YAML_FILE_EXTENSION), "r") as file:
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

    def run_pipeline(self, config_path: str, no_db: bool):
        """Runs the ingestion and indexing pipeline using Apache Beam."""
        config = self.load_config(config_path)
        data_source = self.initialize_data_source(config)
        snipper = self.initialize_snipper(config)
        embedder = self.initialize_embedder(config)
        index_name = config.get("index_name")

        if not index_name:
            raise Exception("Missing index name in configuration.")

        pipeline_options = PipelineOptions()
        pipeline_options.view_as(StandardOptions).streaming = False

        with beam.Pipeline(options=pipeline_options) as p:
            # Generate URLs
            urls = (p 
                   | 'Create' >> beam.Create([None])
                   | 'Generate URLs' >> beam.ParDo(GenerateUrlsFn(data_source)))

            # Convert URLs to documents
            url_results = urls | 'URLs to Documents' >> beam.ParDo(
                UrlToDocumentFn(data_source)
            ).with_outputs('success', 'failed')
            
            documents = url_results.success
            url_failures = url_results.failed | 'Collect URL Failures' >> beam.transforms.combiners.ToList()

            # Write documents to database
            db_results = documents | 'Write to DB' >> beam.ParDo(
                WriteToDBFn(data_source, no_db)
            ).with_outputs('success', 'failed')
            
            successful_docs = db_results.success
            db_failures = db_results.failed | 'Collect DB Failures' >> beam.transforms.combiners.ToList()

            # Index documents
            index_results = successful_docs | 'Index Documents' >> beam.ParDo(
                IndexDocumentFn(data_source, index_name, snipper, embedder, no_db)
            ).with_outputs('success', 'failed')
            
            index_failures = index_results.failed | 'Collect Index Failures' >> beam.transforms.combiners.ToList()

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

            _ = ((url_failures, db_failures, index_failures)
                 | 'Combine Failures' >> beam.CoGroupByKey()
                 | 'Handle Failures' >> beam.Map(lambda x: handle_all_failures(*x)))

import logging

class EntryPoint:
    """Entry point to manage DataSource ingestion and indexing pipelines."""

    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        self.logger = logging.getLogger("DataSourcePipeline")
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

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
                self.logger.info(f"[Attempt {attempt}/{self.max_retries}] Generating URLs...")
                urls = data_source.generate_urls()
                self.logger.info(f"Generated {len(urls)} URLs.")

                self.logger.info(f"[Attempt {attempt}/{self.max_retries}] Converting URLs to TextDocuments...")
                documents = data_source.urls_to_text_documents(urls)
                self.logger.info(f"Converted {len(documents)} TextDocuments.")

                self.logger.info(f"[Attempt {attempt}/{self.max_retries}] Writing TextDocuments to database...")
                data_source.write_documents_to_database(documents, no_db=no_db)
                self.logger.info(f"Successfully processed {len(documents)} documents.")

                self.logger.info(f"[Attempt {attempt}/{self.max_retries}] Indexing documents...")
                data_source.index_documents(index_name, snipper, embedder, no_db=no_db)
                self.logger.info("Indexing completed successfully.")

                self.logger.info(f"Pipeline completed successfully.")
                break
            except Exception as e:
                self.logger.error(f"Error during pipeline execution: {e}")
                if attempt == self.max_retries:
                    self.logger.critical("Pipeline failed after maximum retries.")
                    raise Exception("Pipeline execution failed.") from e
                else:
                    self.logger.warning("Retrying pipeline...")

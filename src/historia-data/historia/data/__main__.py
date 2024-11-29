import argparse

from .__init__ import EntryPoint


def main():
    parser = argparse.ArgumentParser(
        description="Run a DataSource ingestion and indexing pipeline."
    )
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        required=True,
        help="Path to the YAML configuration file.",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Log database transactions instead of executing them.",
    )
    args = parser.parse_args()

    entry_point = EntryPoint()
    try:
        entry_point.run_pipeline(config_path=args.config, no_db=args.no_db)
    except Exception as e:
        print(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()

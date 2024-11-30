import argparse

from .__init__ import BeamEntryPoint, DirectEntryPoint


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
        "--use-all",
        action="store_true",
        help="Use all URLs instead only new ones.",
    )
    parser.add_argument(
        "--entrypoint",
        "-e",
        type=str,
        choices=["beam", "direct"],
        default="beam",
        help="Entrypoint to use (beam or direct). Defaults to beam.",
    )
    args = parser.parse_args()

    # Select entrypoint based on argument
    EntryPointClass = BeamEntryPoint if args.entrypoint == "beam" else DirectEntryPoint
    entry_point = EntryPointClass()
    
    try:
        entry_point.run_pipeline(config_path=args.config, use_all=args.use_all)
    except Exception as e:
        print(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()

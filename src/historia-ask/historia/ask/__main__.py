import argparse

from .__init__ import FUNCTIONS

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Call Historia Ask functions")
    parser.add_argument("function", choices=FUNCTIONS.keys(), help="Function to call")
    args = parser.parse_args()

    # Call the requested function
    result = FUNCTIONS[args.function]()

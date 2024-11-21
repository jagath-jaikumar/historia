import httpx
import os

HISTORIA_ASK_URL = os.getenv("HISTORIA_ASK_URL", "http://localhost:8000/")


def root():
    response = httpx.get(HISTORIA_ASK_URL)
    response.raise_for_status()

    print(response.json())
    return response.json()


# Dictionary mapping function names to functions
FUNCTIONS = {"root": root}

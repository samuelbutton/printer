# System imports
import sys
import logging

# Printer imports
from cli import run

logging.basicConfig(
    format="%(asctime)-15s:%(name)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

if __name__ == "__main__":
    run(sys.argv[1:])

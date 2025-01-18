"""
Logging configuration for pg-qperf-compare.
Sets up console and file logging with appropriate formatting.
"""
import logging
from pathlib import Path

def setup_logger(output_dir: Path) -> logging.Logger:
    """Configure logging with console and file handlers."""
    logger = logging.getLogger('pg-qperf-compare')
    logger.setLevel(logging.INFO)
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(message)s'))
    
    file_handler = logging.FileHandler(output_dir / 'analysis.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    logger.addHandler(console)
    logger.addHandler(file_handler)
    
    return logger

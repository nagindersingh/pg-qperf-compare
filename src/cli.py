#!/usr/bin/env python3
"""
pg-qperf-compare

A command-line tool for PostgreSQL query performance analysis.
Compares execution plans and metrics between original and optimized queries.
"""

"""
Command line interface for PostgreSQL query performance analyzer
"""
import sys
from pathlib import Path

from .core.analyzer import QueryAnalyzer
from .core.database import DatabaseManager
from .utils import ConfigLoader

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python -m src.cli <config_file>")
        sys.exit(1)
        
    config_file = Path(sys.argv[1])
    if not config_file.exists():
        print(f"Error: Config file not found: {config_file}")
        sys.exit(1)
    
    # Load configuration and analyze queries
    config = ConfigLoader.load_config(config_file)
    db_manager = DatabaseManager(config.database)
    analyzer = QueryAnalyzer(db_manager)
    analyzer.analyze_queries(config_file)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
pg-qperf-compare

A command-line tool for PostgreSQL query performance analysis.
Compares execution plans and metrics between original and optimized queries.
"""

import sys
from pathlib import Path

from .utils.config import ConfigLoader
from .utils.logger import setup_logger
from .core.database import DatabaseManager
from .core.analyzer import QueryAnalyzer
from .reports.generator import ReportGenerator

def main():
    try:
        # Check command line arguments
        if len(sys.argv) != 2:
            print("Usage: python -m src.cli path/to/config.yml")
            sys.exit(1)
        
        config_path = Path(sys.argv[1])
        if not config_path.exists():
            print(f"Error: Config file not found at {config_path}")
            sys.exit(1)
        
        # Initialize components
        config = ConfigLoader.load(config_path)
        logger = setup_logger(config.output_dir)
        
        # Validate query files
        for query_type, path in config.queries.items():
            if not path.exists():
                print(f"Error: {query_type} query file not found at {path}")
                sys.exit(1)
        
        # Run analysis
        logger.info("Starting performance analysis...")
        db_manager = DatabaseManager(config.database)
        analyzer = QueryAnalyzer(db_manager)
        report_gen = ReportGenerator(config.output_dir)
        
        results = analyzer.compare_queries(
            config.queries['original'],
            config.queries['optimized']
        )
        
        # Generate report
        report_path = report_gen.generate_report(results)
        logger.info(f"Report generated: {report_path}")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()

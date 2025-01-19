#!/usr/bin/env python3
"""
pg-qperf-compare

A command-line tool for PostgreSQL query performance analysis.
Compares execution plans and metrics between original and optimized queries.
"""

import sys
import os
import yaml
from .analyzer import QueryAnalyzer

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python -m src.cli <config_file>")
        sys.exit(1)
        
    config_file = sys.argv[1]
    if not os.path.exists(config_file):
        print(f"Error: Config file not found: {config_file}")
        sys.exit(1)
        
    # Read queries from files
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        
    # Read original query
    with open(config['queries']['original'], 'r') as f:
        original_query = f.read().strip()
        
    # Read optimized query
    with open(config['queries']['optimized'], 'r') as f:
        optimized_query = f.read().strip()
        
    # Initialize analyzer with queries and connection info
    analyzer = QueryAnalyzer(original_query, optimized_query)
    analyzer.conn_string = " ".join(f"{k}={v}" for k, v in config['database'].items())
    
    # Generate reports
    text_report, html_report = analyzer.generate_report()
    print(f"Reports generated: {text_report}, {html_report}")

if __name__ == '__main__':
    main()

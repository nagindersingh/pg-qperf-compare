# PostgreSQL Query Performance Comparator

A powerful tool for analyzing and comparing PostgreSQL query performance, helping database developers and DBAs identify optimization opportunities and validate query improvements.

## Features
- Compare execution times between original and optimized queries
- Generate detailed performance reports with execution plans
- Analyze query metrics including planning time, execution time, and row counts
- Track I/O statistics and buffer usage
- Generate beautiful HTML reports with performance visualizations
- Detailed node type statistics for query execution plans
- Smart index recommendations based on query patterns
- Export raw data in JSON format for further analysis

## Performance Analysis
The tool provides comprehensive analysis of:

### Node Type Statistics
- Counts and distribution of different node types (e.g., Seq Scan, Index Scan, Hash Join)
- Cost metrics per node type
- Average rows processed by each node type

### Table Statistics
- Scan types used for each table
- Row counts and widths
- Cost metrics per table access

### I/O Analysis
- Shared/local buffer hits and reads
- Temporary buffer usage
- Buffer efficiency ratios

### Index Recommendations
- Smart suggestions for new indexes based on:
  - Join conditions
  - Filter predicates
  - Sort operations
  - Table access patterns

## Project Structure
```
pg-qperf-compare/
├── src/
│   ├── core/           # Core analysis functionality
│   │   ├── analyzer.py # Query analysis logic
│   │   ├── database.py # Database connection handling
│   │   └── metrics.py  # Metrics extraction and processing
│   ├── utils/          # Utility modules
│   │   ├── config.py   # Configuration management
│   │   └── logger.py   # Logging setup
│   ├── reports/        # Report generation
│   │   ├── generator.py
│   │   └── templates/
│   └── cli.py         # Command-line interface
├── tests/             # Test suite
│   ├── conftest.py    # Test fixtures
│   ├── test_analyzer.py
│   ├── test_config.py
│   └── test_metrics.py
├── examples/          # Example queries and configs
└── requirements.txt   # Project dependencies
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/pg-qperf-compare.git
   cd pg-qperf-compare
   ```

2. Create and activate a virtual environment:
   ```bash
   # For macOS/Linux
   python -m venv venv
   source venv/bin/activate

   # For Windows
   python -m venv venv
   .\venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. Copy the example config:
   ```bash
   cp examples/config.yml config.yml
   ```

2. Edit `config.yml` with your database settings:
   ```yaml
   database:
     host: localhost
     port: 5432
     dbname: your_database
     user: your_username
     password: your_password  # Consider using environment variables
   ```

## Usage

1. Prepare your SQL queries:
   - Create `original.sql` with your current query
   - Create `optimized.sql` with your improved query

2. Run the analysis:
   ```bash
   python -m src.cli config.yml
   ```

3. View the results:
   - Open the generated HTML report in your browser
   - Check the TEXT data export for detailed metrics

## Report Interpretation

The HTML report provides:

1. **Executive Summary**
   - Overall performance comparison
   - Key metrics differences
   - Performance regressions or improvements

2. **Query Statistics**
   - Node type distribution and costs
   - Table access patterns
   - Buffer usage efficiency

3. **Execution Plan Analysis**
   - Detailed plan comparison
   - Node-by-node metrics
   - Cost and row estimates vs actuals

4. **Recommendations**
   - Index suggestions
   - Query structure improvements
   - Configuration adjustments

## Testing

The project uses pytest for testing. The test suite includes:

- Unit tests for all core components
- Integration tests for database operations
- Fixtures for common test scenarios

Run tests with:
```bash
pytest tests/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the test suite
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

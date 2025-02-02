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

## Development Setup

### Database Setup
1. Prerequisites:
   - PostgreSQL installed and running
   - `psql` command-line tool available

2. Create test database and user:
   ```bash
   # Create test user
   createuser -s test

   # Create test database
   createdb test
   ```

3. Load sample data:
   ```bash
   # Load the sample schema and data
   psql -U test -d test -f examples/setup.sql
   ```

4. Configure database connection:
   ```yaml
   # examples/config.yml
   database:
     host: localhost
     port: 5432
     dbname: test
     user: test
     password: test  # Use environment variables in production
   ```

### Git Hooks
The project uses git hooks to ensure code quality. After cloning the repository:

1. The pre-commit hook will automatically run tests before each commit:
   ```bash
   # The hook will:
   - Run pytest
   - Block the commit if any tests fail
   - Allow the commit if all tests pass
   ```

2. If you need to bypass the pre-commit hook in exceptional cases:
   ```bash
   git commit --no-verify -m "Your commit message"
   ```

### Running Tests
Tests can be run manually using:
```bash
python -m pytest
```

This will:
- Run all test cases
- Generate a coverage report
- Show test execution summary

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
│   ├── core/              # Core analysis functionality
│   │   ├── __init__.py
│   │   ├── analyzer.py    # Query analysis logic
│   │   ├── database.py    # Database connection handling
│   │   ├── metrics.py     # Metrics extraction and processing
│   │   └── models.py      # Data models and types
│   ├── utils/             # Utility modules
│   │   ├── __init__.py
│   │   ├── config.py      # Configuration management
│   │   └── report.py      # Report generation
│   └── cli.py            # Command line interface
├── tests/                # Test suite
│   ├── __init__.py
│   ├── test_analyzer.py
│   ├── test_config.py
│   └── test_metrics.py
├── examples/             # Example queries and configurations
│   ├── config.yml        # Sample configuration
│   ├── setup.sql         # Database setup script
│   └── queries/          # Sample queries to analyze
│       ├── original.sql
│       └── optimized.sql
├── reports/             # Generated performance reports
├── requirements.txt     # Project dependencies
└── README.md           # Project documentation
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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run the test suite
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

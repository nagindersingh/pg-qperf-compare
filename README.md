# PostgreSQL Query Performance Comparator

A powerful tool for analyzing and comparing PostgreSQL query performance, helping database developers and DBAs identify optimization opportunities and validate query improvements.

## Features
- Compare execution times between original and optimized queries
- Generate detailed performance reports with execution plans
- Analyze query metrics including planning time, execution time, and row counts
- Track I/O statistics and buffer usage
- Generate beautiful HTML reports with performance visualizations
- Export raw data in JSON format for further analysis

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
   - Check the JSON data export for detailed metrics
   - Review the log file for analysis details

## Testing

The project uses pytest for testing. The test suite includes:

- Unit tests for all core components
- Integration tests for database operations
- Fixtures for common test scenarios

Run the tests:
```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov

# Run specific test file
pytest tests/test_analyzer.py
```

Coverage reports are generated in HTML format in the `htmlcov` directory.

Key test files:
- `test_analyzer.py`: Tests for query analysis logic
- `test_config.py`: Tests for configuration handling
- `test_metrics.py`: Tests for metrics calculations

## Output Files

The tool generates several files in your specified output directory:

- `report_[timestamp].html`: Main performance comparison report
- `data_[timestamp].json`: Raw performance data in JSON format
- `analysis.log`: Detailed analysis log

## Report Contents

The HTML report includes:
- Performance summary with improvement percentages
- Side-by-side query comparison
- Execution plan analysis
- I/O and buffer statistics
- Performance visualizations

## Development

Want to contribute? Great! The project follows a modular architecture:

- `core/`: Contains the main analysis logic
- `utils/`: Houses utility functions and helpers
- `reports/`: Handles report generation and templating

## Requirements

- Python 3.7+
- PostgreSQL 10+
- Required Python packages (see `requirements.txt`)

## License

MIT License - feel free to use this tool for any purpose!

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

Found a bug or need help? Please open an issue on GitHub.

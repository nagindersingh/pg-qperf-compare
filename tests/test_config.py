# Run all tests
pytest

# Run with coverage
pytest --cov

# Run specific tests
pytest .tests/test_analyzer.py
"""Test configuration loading and validation."""

import pytest
from pathlib import Path
from src.utils.config import ConfigLoader

def test_config_loading(tmp_path):
    # Create a test config file
    config_path = tmp_path / 'test_config.yml'
    config_path.write_text("""
database:
    host: localhost
    port: 5432
    dbname: test_db
    user: test_user
    password: test_pass

queries:
    original: ./queries/original.sql
    optimized: ./queries/optimized.sql

output:
    report_dir: ./reports
""")
    
    config = ConfigLoader.load(config_path)
    
    assert config.database.host == 'localhost'
    assert config.database.port == 5432
    assert config.database.dbname == 'test_db'
    assert isinstance(config.queries['original'], Path)
    assert isinstance(config.output_dir, Path)

def test_config_missing_required(tmp_path):
    config_path = tmp_path / 'invalid_config.yml'
    config_path.write_text("""
database:
    host: localhost
""")
    
    with pytest.raises(KeyError):
        ConfigLoader.load(config_path)

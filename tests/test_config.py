"""Test configuration loading and validation."""

import pytest
from pathlib import Path
from src.utils.config import ConfigLoader

def test_config_loading(tmp_path):
    # Create test query files
    queries_dir = tmp_path / 'queries'
    queries_dir.mkdir()
    
    original_query = queries_dir / 'original.sql'
    optimized_query = queries_dir / 'optimized.sql'
    
    original_query.write_text('SELECT * FROM test;')
    optimized_query.write_text('SELECT id FROM test;')
    
    # Create a test config file
    config_path = tmp_path / 'test_config.yml'
    config_path.write_text(f"""
database:
    host: localhost
    port: 5432
    dbname: test_db
    user: test_user
    password: test_pass

original_query: {original_query}
optimized_query: {optimized_query}
""")
    
    config = ConfigLoader.load_config(config_path)
    
    assert config.database.host == 'localhost'
    assert config.database.port == 5432
    assert config.database.dbname == 'test_db'
    assert config.original_query == original_query
    assert config.optimized_query == optimized_query

def test_config_missing_required(tmp_path):
    config_path = tmp_path / 'invalid_config.yml'
    config_path.write_text("""
database:
    host: localhost
""")
    
    with pytest.raises(ValueError):
        ConfigLoader.load_config(config_path)

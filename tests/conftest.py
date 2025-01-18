"""Test configuration and fixtures for pg-qperf-compare."""

import pytest
from pathlib import Path
from src.core.database import DatabaseConfig
from src.utils.config import AppConfig

@pytest.fixture
def sample_db_config():
    return DatabaseConfig(
        host='localhost',
        port=5432,
        dbname='test_db',
        user='test_user',
        password='test_pass'
    )

@pytest.fixture
def sample_queries(tmp_path):
    # Create temporary query files
    orig_query = tmp_path / 'original.sql'
    opt_query = tmp_path / 'optimized.sql'
    
    orig_query.write_text('SELECT * FROM test_table WHERE id = 1;')
    opt_query.write_text('SELECT id FROM test_table WHERE id = 1;')
    
    return {
        'original': orig_query,
        'optimized': opt_query
    }

@pytest.fixture
def sample_config(sample_db_config, sample_queries, tmp_path):
    return AppConfig(
        database=sample_db_config,
        queries=sample_queries,
        output_dir=tmp_path / 'reports'
    )

@pytest.fixture
def sample_plan():
    return {
        'Plan': {
            'Node Type': 'Seq Scan',
            'Relation Name': 'test_table',
            'Actual Rows': 100,
            'Actual Total Time': 1.5,
            'Actual Loops': 1,
        },
        'Planning Time': 0.5,
        'Execution Time': 2.0
    }

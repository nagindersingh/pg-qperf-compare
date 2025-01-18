"""Test query analysis functionality."""

import pytest
from unittest.mock import Mock, patch
from src.core.analyzer import QueryAnalyzer
from src.core.database import DatabaseManager

@pytest.fixture
def mock_db_manager():
    manager = Mock(spec=DatabaseManager)
    manager.execute_explain.return_value = {
        'plan': {
            'Planning Time': 0.5,
            'Execution Time': 2.0,
            'Plan': {
                'Node Type': 'Seq Scan',
                'Actual Rows': 100,
                'Actual Total Time': 1.5,
            }
        },
        'row_count': 100
    }
    return manager

def test_analyze_query(mock_db_manager, tmp_path):
    query_path = tmp_path / 'test.sql'
    query_path.write_text('SELECT * FROM test;')
    
    analyzer = QueryAnalyzer(mock_db_manager)
    result = analyzer.analyze_query(query_path)
    
    assert result.query_text == 'SELECT * FROM test;'
    assert result.metrics['execution_time'] == 2.0
    assert result.metrics['row_count'] == 100

def test_compare_queries(mock_db_manager, tmp_path):
    # Create test queries
    orig_path = tmp_path / 'original.sql'
    opt_path = tmp_path / 'optimized.sql'
    
    orig_path.write_text('SELECT * FROM test;')
    opt_path.write_text('SELECT id FROM test;')
    
    # Configure mock for different responses
    mock_db_manager.execute_explain.side_effect = [
        {  # Original query result
            'plan': {
                'Planning Time': 1.0,
                'Execution Time': 4.0,
                'Plan': {'Node Type': 'Seq Scan', 'Actual Rows': 100}
            },
            'row_count': 100
        },
        {  # Optimized query result
            'plan': {
                'Planning Time': 0.5,
                'Execution Time': 2.0,
                'Plan': {'Node Type': 'Index Scan', 'Actual Rows': 100}
            },
            'row_count': 100
        }
    ]
    
    analyzer = QueryAnalyzer(mock_db_manager)
    result = analyzer.compare_queries(orig_path, opt_path)
    
    assert result['improvements']['execution_time'] == 50.0  # 50% improvement
    assert result['improvements']['planning_time'] == 50.0
    assert result['original'].metrics['execution_time'] == 4.0
    assert result['optimized'].metrics['execution_time'] == 2.0

"""Test metrics extraction and calculations."""

from src.core.metrics import MetricsExtractor

def test_extract_node_metrics(sample_plan):
    metrics = MetricsExtractor.extract_node_metrics(sample_plan['Plan'])
    
    assert metrics.node_type == 'Seq Scan'
    assert metrics.actual_rows == 100
    assert metrics.actual_time == 1.5
    assert metrics.actual_loops == 1

def test_calculate_performance_metrics(sample_plan):
    metrics = MetricsExtractor.calculate_performance_metrics(sample_plan, 100)
    
    assert metrics['planning_time'] == 0.5
    assert metrics['execution_time'] == 2.0
    assert metrics['row_count'] == 100
    assert metrics['rows_per_second'] == 50  # 100 rows / (2.0 ms / 1000)

def test_nested_plan_extraction():
    nested_plan = {
        'Node Type': 'Hash Join',
        'Actual Rows': 50,
        'Actual Total Time': 3.0,
        'Plans': [
            {
                'Node Type': 'Seq Scan',
                'Actual Rows': 100,
                'Actual Total Time': 1.0
            },
            {
                'Node Type': 'Hash',
                'Actual Rows': 10,
                'Actual Total Time': 0.5
            }
        ]
    }
    
    metrics = MetricsExtractor.extract_node_metrics(nested_plan)
    
    assert metrics.node_type == 'Hash Join'
    assert len(metrics.children) == 2
    assert metrics.children[0].node_type == 'Seq Scan'
    assert metrics.children[1].node_type == 'Hash'

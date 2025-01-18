"""
Query execution plan metrics extraction and analysis.
Handles parsing and calculation of performance metrics from EXPLAIN ANALYZE output.
"""
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class NodeMetrics:
    node_type: str
    actual_rows: int
    actual_time: float
    actual_loops: int
    planned_rows: int
    shared_hit_blocks: int
    shared_read_blocks: int
    shared_dirtied_blocks: int
    shared_written_blocks: int
    temp_read_blocks: int
    temp_written_blocks: int
    io_read_time: float
    io_write_time: float
    filter: str
    rows_removed_by_filter: int
    scan_direction: str
    index_name: str
    index_cond: str
    join_type: str
    hash_condition: str
    relation_name: str
    children: List['NodeMetrics']

class MetricsExtractor:
    @staticmethod
    def extract_node_metrics(node: Dict[str, Any]) -> NodeMetrics:
        """Extract metrics from a query plan node."""
        metrics = NodeMetrics(
            node_type=node['Node Type'],
            actual_rows=node.get('Actual Rows', 0),
            actual_time=node.get('Actual Total Time', 0),
            actual_loops=node.get('Actual Loops', 1),
            planned_rows=node.get('Plan Rows', 0),
            shared_hit_blocks=node.get('Shared Hit Blocks', 0),
            shared_read_blocks=node.get('Shared Read Blocks', 0),
            shared_dirtied_blocks=node.get('Shared Dirtied Blocks', 0),
            shared_written_blocks=node.get('Shared Written Blocks', 0),
            temp_read_blocks=node.get('Temp Read Blocks', 0),
            temp_written_blocks=node.get('Temp Written Blocks', 0),
            io_read_time=node.get('I/O Read Time', 0),
            io_write_time=node.get('I/O Write Time', 0),
            filter=node.get('Filter', ''),
            rows_removed_by_filter=node.get('Rows Removed by Filter', 0),
            scan_direction=node.get('Scan Direction', ''),
            index_name=node.get('Index Name', ''),
            index_cond=node.get('Index Cond', ''),
            join_type=node.get('Join Type', ''),
            hash_condition=node.get('Hash Cond', ''),
            relation_name=node.get('Relation Name', ''),
            children=[]
        )
        
        if 'Plans' in node:
            for child in node['Plans']:
                metrics.children.append(MetricsExtractor.extract_node_metrics(child))
        
        return metrics

    @staticmethod
    def calculate_performance_metrics(plan: Dict[str, Any], row_count: int) -> Dict[str, Any]:
        """Calculate high-level performance metrics from execution plan."""
        return {
            'planning_time': plan['Planning Time'],
            'execution_time': plan['Execution Time'],
            'row_count': row_count,
            'rows_per_second': row_count / (plan['Execution Time'] / 1000) if plan['Execution Time'] > 0 else 0,
            'plan_nodes': MetricsExtractor.extract_node_metrics(plan['Plan'])
        }

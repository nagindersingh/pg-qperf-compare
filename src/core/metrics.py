"""
Query execution plan metrics extraction and analysis.
Handles parsing and calculation of performance metrics from EXPLAIN ANALYZE output.
"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

@dataclass
class NodeMetrics:
    node_type: str
    actual_rows: int
    actual_time: float
    actual_loops: int
    planned_rows: int
    total_cost: float
    startup_cost: float
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

@dataclass
class PlanMetrics:
    planning_time: float
    execution_time: float
    total_time: float
    row_count: int
    node_metrics: NodeMetrics
    io_metrics: Dict[str, Any]
    node_type_stats: Dict[str, Any]
    table_stats: Dict[str, Any]
    buffer_stats: Dict[str, Any]

class MetricsExtractor:
    @staticmethod
    def extract_node_metrics(node: Dict[str, Any]) -> NodeMetrics:
        """Extract metrics from a query plan node."""
        children = []
        if 'Plans' in node:
            for child in node['Plans']:
                children.append(MetricsExtractor.extract_node_metrics(child))

        return NodeMetrics(
            node_type=node.get('Node Type', ''),
            actual_rows=node.get('Actual Rows', 0),
            actual_time=node.get('Actual Total Time', 0),
            actual_loops=node.get('Actual Loops', 1),
            planned_rows=node.get('Plan Rows', 0),
            total_cost=node.get('Total Cost', 0),
            startup_cost=node.get('Startup Cost', 0),
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
            children=children
        )

    @staticmethod
    def extract_io_metrics(node: NodeMetrics) -> Dict[str, int]:
        """Extract I/O metrics from a node and its children."""
        metrics = {
            'shared_hit_blocks': node.shared_hit_blocks,
            'shared_read_blocks': node.shared_read_blocks,
            'shared_dirtied_blocks': node.shared_dirtied_blocks,
            'shared_written_blocks': node.shared_written_blocks,
            'temp_read_blocks': node.temp_read_blocks,
            'temp_written_blocks': node.temp_written_blocks,
        }
        
        for child in node.children:
            child_metrics = MetricsExtractor.extract_io_metrics(child)
            for key, value in child_metrics.items():
                metrics[key] += value
        
        return metrics

    @staticmethod
    def extract_buffer_stats(node: NodeMetrics) -> Dict[str, Any]:
        """Extract buffer statistics from a node and its children."""
        stats = {
            'buffers_hit': 0,
            'buffers_read': 0,
            'buffers_dirtied': 0,
            'buffers_written': 0,
            'buffers_hit_rate': 0.0
        }
        
        def extract_recursive(node: NodeMetrics):
            stats['buffers_hit'] += node.shared_hit_blocks
            stats['buffers_read'] += node.shared_read_blocks
            stats['buffers_dirtied'] += node.shared_dirtied_blocks
            stats['buffers_written'] += node.shared_written_blocks
            
            for child in node.children:
                extract_recursive(child)
        
        extract_recursive(node)
        
        # Calculate buffer hit rate
        total_buffers = stats['buffers_hit'] + stats['buffers_read']
        if total_buffers > 0:
            stats['buffers_hit_rate'] = (stats['buffers_hit'] / total_buffers) * 100
        
        return stats

    @staticmethod
    def analyze_node_type_stats(node: NodeMetrics, stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Analyze statistics per node type."""
        if stats is None:
            stats = {}
        
        node_type = node.node_type
        if node_type not in stats:
            stats[node_type] = {
                'count': 0,
                'total_time': 0,
                'total_rows': 0,
                'total_cost': 0
            }
        
        stats[node_type]['count'] += 1
        stats[node_type]['total_time'] += node.actual_time
        stats[node_type]['total_rows'] += node.actual_rows
        stats[node_type]['total_cost'] += node.total_cost
        
        for child in node.children:
            MetricsExtractor.analyze_node_type_stats(child, stats)
        
        return stats

    @staticmethod
    def analyze_table_stats(node: NodeMetrics, stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Analyze statistics per table."""
        if stats is None:
            stats = {}
        
        if node.relation_name:
            if node.relation_name not in stats:
                stats[node.relation_name] = {
                    'scan_type': node.node_type,
                    'rows': node.actual_rows,
                    'width': 0,  # Width information not available in current plan format
                    'cost': node.total_cost
                }
            else:
                # Update existing stats if we find a better scan type
                if 'Index' in node.node_type and 'Seq' in stats[node.relation_name]['scan_type']:
                    stats[node.relation_name]['scan_type'] = node.node_type
                stats[node.relation_name]['rows'] += node.actual_rows
                stats[node.relation_name]['cost'] += node.total_cost
        
        for child in node.children:
            MetricsExtractor.analyze_table_stats(child, stats)
        
        return stats

    @staticmethod
    def calculate_performance_metrics(plan: Dict[str, Any]) -> PlanMetrics:
        """Calculate high-level performance metrics from execution plan."""
        if isinstance(plan, list):
            plan = plan[0]
        
        planning_time = plan.get('Planning Time', 0)
        execution_time = plan.get('Execution Time', 0)
        
        node_metrics = MetricsExtractor.extract_node_metrics(plan['Plan'])
        io_metrics = MetricsExtractor.extract_io_metrics(node_metrics)
        buffer_stats = MetricsExtractor.extract_buffer_stats(node_metrics)
        node_type_stats = MetricsExtractor.analyze_node_type_stats(node_metrics)
        table_stats = MetricsExtractor.analyze_table_stats(node_metrics)
        
        return PlanMetrics(
            planning_time=planning_time,
            execution_time=execution_time,
            total_time=planning_time + execution_time,
            row_count=node_metrics.actual_rows,
            node_metrics=node_metrics,
            io_metrics=io_metrics,
            node_type_stats=node_type_stats,
            table_stats=table_stats,
            buffer_stats=buffer_stats
        )

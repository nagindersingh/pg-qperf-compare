"""
Core query analysis functionality.
Handles query execution plan comparison and performance metrics calculation.
"""
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path

from .database import DatabaseManager, DatabaseConfig
from .metrics import MetricsExtractor

@dataclass
class QueryAnalysis:
    query_path: Path
    metrics: Dict[str, Any]
    query_text: str

class QueryAnalyzer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metrics_extractor = MetricsExtractor()

    def analyze_query(self, query_path: Path) -> QueryAnalysis:
        """Run EXPLAIN ANALYZE and collect performance metrics."""
        query_text = query_path.read_text()
        result = self.db_manager.execute_explain(query_text)
        
        metrics = self.metrics_extractor.calculate_performance_metrics(
            result['plan'],
            result['row_count']
        )
        
        return QueryAnalysis(
            query_path=query_path,
            metrics=metrics,
            query_text=query_text
        )

    def compare_queries(self, original_path: Path, optimized_path: Path) -> Dict[str, Any]:
        """Compare metrics between original and optimized queries."""
        original = self.analyze_query(original_path)
        optimized = self.analyze_query(optimized_path)
        
        # Calculate improvements
        exec_time_diff = original.metrics['execution_time'] - optimized.metrics['execution_time']
        exec_time_improvement = (exec_time_diff / original.metrics['execution_time']) * 100
        
        planning_time_diff = original.metrics['planning_time'] - optimized.metrics['planning_time']
        planning_time_improvement = (planning_time_diff / original.metrics['planning_time']) * 100
        
        return {
            'original': original,
            'optimized': optimized,
            'improvements': {
                'execution_time': exec_time_improvement,
                'planning_time': planning_time_improvement,
                'execution_time_diff': exec_time_diff,
                'planning_time_diff': planning_time_diff
            }
        }

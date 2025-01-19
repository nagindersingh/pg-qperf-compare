"""
Core query analysis functionality.
Handles query execution plan comparison and performance metrics calculation.
"""
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from .database import DatabaseManager
from .metrics import MetricsExtractor, PlanMetrics, NodeMetrics
from .models import Problem, IndexRecommendation
from ..utils.config import ConfigLoader
from ..utils.report import ReportGenerator

class QueryAnalyzer:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metrics_extractor = MetricsExtractor()

    def analyze_query(self, query_path: Path) -> Dict[str, Any]:
        """Run EXPLAIN ANALYZE and collect performance metrics."""
        query_text = query_path.read_text()
        result = self.db_manager.execute_explain(query_text)
        
        metrics = self.metrics_extractor.calculate_performance_metrics(result['plan'])
        
        problems = self.analyze_node_problems(metrics.node_metrics)
        recommendations = self.analyze_index_recommendations(metrics.node_metrics)
        
        return {
            'query_path': query_path,
            'metrics': metrics,
            'query_text': query_text,
            'problems': problems,
            'recommendations': recommendations,
            'raw_plan': result  # Changed from result['plan'] to result
        }

    def analyze_node_problems(self, node: NodeMetrics) -> List[Problem]:
        """Analyze potential problems in a plan node."""
        problems = []
        
        def analyze_node_recursive(node: NodeMetrics):
            # Check for sequential scans on large tables
            if node.node_type == 'Seq Scan' and node.actual_rows > 1000:
                problems.append(Problem(
                    severity='HIGH',
                    description=f"Sequential scan on large table ({node.actual_rows:,} rows)"
                ))
            
            # Check for poor row estimates
            if node.planned_rows > 0:
                estimate_ratio = node.actual_rows / node.planned_rows
                if estimate_ratio > 10 or estimate_ratio < 0.1:
                    problems.append(Problem(
                        severity='MEDIUM',
                        description=f"Poor row estimation in {node.node_type}: expected {node.planned_rows:,}, got {node.actual_rows:,} ({estimate_ratio:.1f}x off)"
                    ))
            
            # Check for expensive sorts
            if node.node_type == 'Sort' and node.actual_rows > 1000:
                problems.append(Problem(
                    severity='MEDIUM',
                    description=f"Expensive sort operation on {node.actual_rows:,} rows"
                ))
            
            # Check for nested loops with many rows
            if node.node_type == 'Nested Loop' and node.actual_rows > 1000:
                problems.append(Problem(
                    severity='HIGH',
                    description=f"Nested loop join with large number of rows ({node.actual_rows:,})"
                ))
            
            # Check for high filter removal
            if node.rows_removed_by_filter > 1000:
                problems.append(Problem(
                    severity='MEDIUM',
                    description=f"Large number of rows removed by filter ({node.rows_removed_by_filter:,})"
                ))
            
            # Recursively check child nodes
            for child in node.children:
                analyze_node_recursive(child)
        
        analyze_node_recursive(node)
        return problems

    def analyze_index_recommendations(self, node: NodeMetrics) -> List[IndexRecommendation]:
        """Analyze execution plan and provide index recommendations."""
        recommendations = []
        
        def analyze_node_recursive(node: NodeMetrics, parent: Optional[NodeMetrics] = None):
            # Recommend index for sequential scans with high row counts
            if node.node_type == 'Seq Scan' and node.actual_rows > 1000:
                if node.filter:
                    columns = self._extract_columns_from_condition(node.filter)
                    if columns:
                        recommendations.append(IndexRecommendation(
                            table=node.relation_name,
                            columns=columns,
                            reason=f"High row count sequential scan with filter on {', '.join(columns)}"
                        ))
            
            # Recommend index for hash joins
            if node.node_type == 'Hash Join' and node.hash_condition:
                columns = self._extract_join_columns(node.hash_condition)
                if columns:
                    for child in node.children:
                        if child.relation_name:
                            recommendations.append(IndexRecommendation(
                                table=child.relation_name,
                                columns=columns,
                                reason=f"Hash join condition on {', '.join(columns)}"
                            ))
            
            # Recursively check child nodes
            for child in node.children:
                analyze_node_recursive(child, node)
        
        analyze_node_recursive(node)
        return recommendations

    def _extract_columns_from_condition(self, condition: str) -> List[str]:
        """Extract column names from a filter condition."""
        # Simple extraction - could be enhanced with SQL parsing
        import re
        columns = []
        parts = re.split(r'\s+AND\s+|\s+OR\s+', condition)
        for part in parts:
            match = re.search(r'(\w+)\s*[=<>]', part)
            if match:
                columns.append(match.group(1))
        return columns

    def _extract_join_columns(self, condition: str) -> List[str]:
        """Extract table and column names from a join condition."""
        # Simple extraction - could be enhanced with SQL parsing
        import re
        columns = []
        parts = condition.split(' = ')
        for part in parts:
            match = re.search(r'(\w+)\.(\w+)', part)
            if match:
                columns.append(match.group(2))
        return columns

    def compare_queries(self, original_path: Path, optimized_path: Path) -> Dict[str, Any]:
        """Compare metrics between original and optimized queries."""
        original = self.analyze_query(original_path)
        optimized = self.analyze_query(optimized_path)
        
        # Calculate improvements
        # Negative improvement means optimized is worse (takes more time)
        # Positive improvement means optimized is better (takes less time)
        exec_time_diff = original['metrics'].execution_time - optimized['metrics'].execution_time
        exec_time_improvement = (exec_time_diff / original['metrics'].execution_time) * 100 if original['metrics'].execution_time > 0 else 0
        
        planning_time_diff = original['metrics'].planning_time - optimized['metrics'].planning_time
        planning_time_improvement = (planning_time_diff / original['metrics'].planning_time) * 100 if original['metrics'].planning_time > 0 else 0
        
        return {
            'original': {
                'metrics': original['metrics'],
                'problems': original['problems'],
                'recommendations': original['recommendations'],
                'query': original['query_text'],
                'raw_plan': original['raw_plan']  # Changed from original['raw_plan']['plan'] to original['raw_plan']
            },
            'optimized': {
                'metrics': optimized['metrics'],
                'problems': optimized['problems'],
                'recommendations': optimized['recommendations'],
                'query': optimized['query_text'],
                'raw_plan': optimized['raw_plan']  # Changed from optimized['raw_plan']['plan'] to optimized['raw_plan']
            },
            'improvements': {
                'execution_time': exec_time_improvement,  # Positive means optimized is better
                'planning_time': planning_time_improvement
            }
        }

    def analyze_queries(self, config_path: Path) -> None:
        """Analyze queries using configuration from file."""
        print("Starting performance analysis...")
        
        # Load configuration
        config = ConfigLoader.load_config(config_path)
        
        # Initialize components
        db_manager = DatabaseManager(config.database)
        analyzer = QueryAnalyzer(db_manager)
        
        # Analyze queries
        metrics_data = analyzer.compare_queries(config.original_query, config.optimized_query)
        
        # Generate reports
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        html_path = Path('reports') / f'report_{timestamp}.html'
        text_path = Path('reports') / f'report_{timestamp}.txt'
        
        # Ensure reports directory exists
        html_path.parent.mkdir(exist_ok=True)
        
        # Generate reports
        html_report = ReportGenerator.generate_html_report(
            metrics_data,
            metrics_data['original'].get('problems', []),
            metrics_data['optimized'].get('problems', []),
            metrics_data.get('index_recommendations', [])
        )
        text_report = ReportGenerator.generate_text_report(
            metrics_data,
            metrics_data['original'].get('problems', []),
            metrics_data['optimized'].get('problems', []),
            metrics_data.get('index_recommendations', [])
        )
        
        # Write reports
        html_path.write_text(html_report)
        text_path.write_text(text_report)
        
        print("\nReports generated:")
        print(f"- HTML report: {html_path}")
        print(f"- Text report: {text_path}")

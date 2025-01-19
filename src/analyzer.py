"""
PostgreSQL Query Analyzer
Analyzes and compares query performance using EXPLAIN ANALYZE
"""

import os
import psycopg2
import yaml
from datetime import datetime
import json

class QueryAnalyzer:
    """Analyzes and compares query performance"""
    
    def __init__(self, original_query, optimized_query):
        """Initialize analyzer with original and optimized queries"""
        self.original_query = original_query
        self.optimized_query = optimized_query
        self.conn_string = "dbname=postgres"
        
    def connect_db(self):
        """Create database connection"""
        return psycopg2.connect(self.conn_string)

    def format_time(self, ms):
        """Format milliseconds into human readable string"""
        if ms < 1:
            return f"{ms * 1000:.2f} μs"
        elif ms < 1000:
            return f"{ms:.2f} ms"
        else:
            return f"{ms / 1000:.2f} s"

    def analyze_query(self, query):
        """Execute EXPLAIN ANALYZE and collect detailed metrics"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        try:
            # Execute EXPLAIN ANALYZE
            explain_query = f"""
            EXPLAIN (
                ANALYZE true,
                BUFFERS true,
                TIMING true,
                COSTS true,
                VERBOSE true,
                FORMAT JSON
            ) {query}"""
            cursor.execute(explain_query)
            plan_data = cursor.fetchall()[0][0]  # Get the JSON plan
            
            # Execute actual query to get row count
            cursor.execute(query)
            row_count = len(cursor.fetchall())
            
            # Extract key metrics from plan
            planning_time = plan_data[0]['Planning Time']
            execution_time = plan_data[0]['Execution Time']
            
            return {
                'planning_time': planning_time,
                'execution_time': execution_time,
                'row_count': row_count,
                'raw_plan': plan_data[0]  # Keep raw plan for detailed analysis
            }
        finally:
            cursor.close()
            conn.close()

    def analyze_node_problems(self, plan):
        """Analyze potential problems in a plan node"""
        problems = []
        
        def analyze_node(node):
            if isinstance(node, list):
                node = node[0]
            if 'Plan' in node:
                node = node['Plan']
                
            node_type = node.get('Node Type', '')
            
            # Check for sequential scans on large tables
            if node_type == 'Seq Scan' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'high',
                    'description': f"Sequential scan on large table ({node.get('Actual Rows', 0):,} rows)"
                })
            
            # Check for poor row estimates
            if 'Plan Rows' in node and 'Actual Rows' in node:
                plan_rows = node['Plan Rows']
                actual_rows = node['Actual Rows']
                if plan_rows > 0:
                    estimate_ratio = actual_rows / plan_rows
                    if estimate_ratio > 10 or estimate_ratio < 0.1:
                        problems.append({
                            'severity': 'medium',
                            'description': f"Poor row estimation in {node_type}: expected {plan_rows:,}, got {actual_rows:,} ({estimate_ratio:.1f}x off)"
                        })
            
            # Check for expensive sorts
            if node_type == 'Sort' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'medium',
                    'description': f"Expensive sort operation on {node.get('Actual Rows', 0):,} rows"
                })
            
            # Check for nested loops with many rows
            if node_type == 'Nested Loop' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'high',
                    'description': f"Nested loop join with large number of rows ({node.get('Actual Rows', 0):,})"
                })
            
            # Check for high filter removal
            if node.get('Rows Removed by Filter', 0) > 1000:
                problems.append({
                    'severity': 'medium',
                    'description': f"Large number of rows removed by filter ({node.get('Rows Removed by Filter', 0):,})"
                })
            
            # Recursively check child nodes
            if 'Plans' in node:
                for child in node['Plans']:
                    analyze_node(child)
        
        analyze_node(plan)
        return problems

    def analyze_query_metrics(self, original_metrics, optimized_metrics):
        """Analyze and format query metrics for comparison"""
        metrics = {}
        
        # Extract I/O metrics from plan
        def extract_io_metrics(plan):
            if isinstance(plan, list):
                plan = plan[0]
            if 'Plan' in plan:
                plan = plan['Plan']
                
            io_metrics = {
                'shared_hit_blocks': 0,
                'shared_read_blocks': 0,
                'shared_written_blocks': 0,
                'local_hit_blocks': 0,
                'local_read_blocks': 0,
                'local_written_blocks': 0,
                'temp_read_blocks': 0,
                'temp_written_blocks': 0
            }
            
            # Parse buffer info string (e.g. "H:2 R:0")
            if 'Shared Hit Blocks' in plan:
                io_metrics['shared_hit_blocks'] = plan['Shared Hit Blocks']
            elif 'Buffers' in plan:
                buffers = plan['Buffers']
                if isinstance(buffers, str):
                    parts = buffers.split()
                    for part in parts:
                        if ':' in part:
                            type_val = part.split(':')
                            if len(type_val) == 2:
                                type_char, val = type_val
                                try:
                                    val = int(val)
                                    if type_char == 'H':
                                        io_metrics['shared_hit_blocks'] = val
                                    elif type_char == 'R':
                                        io_metrics['shared_read_blocks'] = val
                                    elif type_char == 'W':
                                        io_metrics['shared_written_blocks'] = val
                                except ValueError:
                                    pass
            
            if 'Plans' in plan:
                for child in plan['Plans']:
                    child_metrics = extract_io_metrics(child)
                    for key in io_metrics:
                        io_metrics[key] += child_metrics[key]
                        
            return io_metrics
        
        # Extract I/O metrics
        original_metrics.update(extract_io_metrics(original_metrics['raw_plan']))
        optimized_metrics.update(extract_io_metrics(optimized_metrics['raw_plan']))
        
        # Timing metrics
        timing_metrics = [
            ('Planning Time', 'planning_time', 'μs'),
            ('Execution Time', 'execution_time', 'μs'),
            ('Total Time', 'total_time', 'μs')
        ]
        metrics['timing'] = {
            'name': 'Timing Analysis',
            'metrics': self._format_metric_comparison(original_metrics, optimized_metrics, timing_metrics)
        }
        
        # Row metrics
        row_metrics = [
            ('Rows Returned', 'row_count', '')
        ]
        metrics['rows'] = {
            'name': 'Row Analysis',
            'metrics': self._format_metric_comparison(original_metrics, optimized_metrics, row_metrics)
        }
        
        # I/O metrics
        io_metrics = [
            ('Shared Hit Blocks', 'shared_hit_blocks', ''),
            ('Shared Read Blocks', 'shared_read_blocks', ''),
            ('Shared Written Blocks', 'shared_written_blocks', ''),
            ('Local Hit Blocks', 'local_hit_blocks', ''),
            ('Local Read Blocks', 'local_read_blocks', ''),
            ('Local Written Blocks', 'local_written_blocks', ''),
            ('Temp Read Blocks', 'temp_read_blocks', ''),
            ('Temp Written Blocks', 'temp_written_blocks', '')
        ]
        metrics['io'] = {
            'name': 'I/O Analysis',
            'metrics': self._format_metric_comparison(original_metrics, optimized_metrics, io_metrics)
        }
        
        # Buffer usage metrics
        buffer_metrics = [
            ('Shared Hit Ratio', 'shared_hit_ratio', '%'),
            ('Shared Read Ratio', 'shared_read_ratio', '%'),
            ('Temp Block Usage', 'temp_blocks', '')
        ]
        metrics['buffers'] = {
            'name': 'Buffer Usage',
            'metrics': self._format_metric_comparison(original_metrics, optimized_metrics, buffer_metrics)
        }
        
        return metrics

    def _format_metric_comparison(self, original, optimized, metrics):
        """Format metric comparison"""
        formatted_metrics = []
        
        for metric in metrics:
            name, key, unit = metric
            
            # Extract values
            orig_value = original.get(key, 0)
            opt_value = optimized.get(key, 0)
            
            # Special handling for total time
            if name == 'Total Time':
                orig_value = original.get('planning_time', 0) + original.get('execution_time', 0)
                opt_value = optimized.get('planning_time', 0) + optimized.get('execution_time', 0)
            
            # Special handling for rows returned
            if name == 'Rows Returned':
                orig_value = original.get('row_count', 0)
                opt_value = optimized.get('row_count', 0)
            
            # Special handling for buffer ratios
            if name == 'Shared Hit Ratio':
                shared_hit = original.get('shared_hit_blocks', 0)
                shared_read = original.get('shared_read_blocks', 0)
                total = shared_hit + shared_read
                orig_value = (shared_hit / total * 100) if total > 0 else 0
                
                shared_hit = optimized.get('shared_hit_blocks', 0)
                shared_read = optimized.get('shared_read_blocks', 0)
                total = shared_hit + shared_read
                opt_value = (shared_hit / total * 100) if total > 0 else 0
            
            if name == 'Shared Read Ratio':
                shared_hit = original.get('shared_hit_blocks', 0)
                shared_read = original.get('shared_read_blocks', 0)
                total = shared_hit + shared_read
                orig_value = (shared_read / total * 100) if total > 0 else 0
                
                shared_hit = optimized.get('shared_hit_blocks', 0)
                shared_read = optimized.get('shared_read_blocks', 0)
                total = shared_hit + shared_read
                opt_value = (shared_read / total * 100) if total > 0 else 0
            
            # Format values
            orig_str = f"{orig_value:,}{unit}"
            opt_str = f"{opt_value:,}{unit}"
            
            # Calculate improvement
            if orig_value == 0:
                improvement = "N/A" if opt_value == 0 else "∞"
                css_class = ""
            else:
                change = ((opt_value - orig_value) / orig_value) * 100
                if change < 0:
                    improvement = f" 🟢 {abs(change):.1f}% faster"
                    css_class = "improvement"
                elif change > 0:
                    improvement = f" 🔴 {change:.1f}% slower"
                    css_class = "warning"
                else:
                    improvement = "No change"
                    css_class = ""
            
            formatted_metrics.append({
                'name': name,
                'original': orig_str,
                'optimized': opt_str,
                'improvement': improvement,
                'css_class': css_class
            })
        
        return formatted_metrics

    def _format_metrics_section_html(self, section_name, metrics):
        """Format metrics section as HTML table"""
        html = [
            "<table class='metric-table'>",
            "<tr>",
            "<th>Metric</th>",
            "<th>Original</th>",
            "<th>Optimized</th>",
            "<th>Change</th>",
            "</tr>"
        ]
        
        for metric in metrics:
            html.append(
                f"<tr><td>{metric['name']}</td>"
                f"<td>{metric['original']}</td>"
                f"<td>{metric['optimized']}</td>"
                f"<td class='{metric['css_class']}'>{metric['improvement']}</td></tr>"
            )
        
        html.append("</table>")
        return '\n'.join(html)

    def analyze_node_problems(self, plan):
        """Analyze potential problems in a plan node"""
        problems = []
        
        def analyze_node(node):
            if isinstance(node, list):
                node = node[0]
            if 'Plan' in node:
                node = node['Plan']
                
            node_type = node.get('Node Type', '')
            
            # Check for sequential scans on large tables
            if node_type == 'Seq Scan' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'high',
                    'description': f"Sequential scan on large table ({node.get('Actual Rows', 0):,} rows)"
                })
            
            # Check for poor row estimates
            if 'Plan Rows' in node and 'Actual Rows' in node:
                plan_rows = node['Plan Rows']
                actual_rows = node['Actual Rows']
                if plan_rows > 0:
                    estimate_ratio = actual_rows / plan_rows
                    if estimate_ratio > 10 or estimate_ratio < 0.1:
                        problems.append({
                            'severity': 'medium',
                            'description': f"Poor row estimation in {node_type}: expected {plan_rows:,}, got {actual_rows:,} ({estimate_ratio:.1f}x off)"
                        })
            
            # Check for expensive sorts
            if node_type == 'Sort' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'medium',
                    'description': f"Expensive sort operation on {node.get('Actual Rows', 0):,} rows"
                })
            
            # Check for nested loops with many rows
            if node_type == 'Nested Loop' and node.get('Actual Rows', 0) > 1000:
                problems.append({
                    'severity': 'high',
                    'description': f"Nested loop join with large number of rows ({node.get('Actual Rows', 0):,})"
                })
            
            # Check for high filter removal
            if node.get('Rows Removed by Filter', 0) > 1000:
                problems.append({
                    'severity': 'medium',
                    'description': f"Large number of rows removed by filter ({node.get('Rows Removed by Filter', 0):,})"
                })
            
            # Recursively check child nodes
            if 'Plans' in node:
                for child in node['Plans']:
                    analyze_node(child)
        
        analyze_node(plan)
        return problems

    def analyze_plan_differences(self, original_plan, optimized_plan):
        """Analyze differences between original and optimized plans"""
        differences = []
        
        # Helper function to get plan node types
        def get_node_types(plan):
            if isinstance(plan, list):
                plan = plan[0]
            if 'Plan' in plan:
                plan = plan['Plan']
                
            types = [plan.get('Node Type', '')]
            if 'Plans' in plan:
                for child in plan['Plans']:
                    types.extend(get_node_types(child))
            return types
        
        # Compare node types
        original_types = get_node_types(original_plan)
        optimized_types = get_node_types(optimized_plan)
        
        # Check for sequential scans replaced by index scans
        orig_seqscans = original_types.count('Seq Scan')
        opt_seqscans = optimized_types.count('Seq Scan')
        if opt_seqscans < orig_seqscans:
            differences.append({
                'type': 'improvement',
                'description': f"Reduced sequential scans from {orig_seqscans} to {opt_seqscans}"
            })
        elif opt_seqscans > orig_seqscans:
            differences.append({
                'type': 'warning',
                'description': f"Increased sequential scans from {orig_seqscans} to {opt_seqscans}"
            })
        
        # Compare index usage
        orig_indexscans = sum(1 for t in original_types if 'Index' in t)
        opt_indexscans = sum(1 for t in optimized_types if 'Index' in t)
        if opt_indexscans > orig_indexscans:
            differences.append({
                'type': 'improvement',
                'description': f"Increased index usage from {orig_indexscans} to {opt_indexscans} scans"
            })
        
        return differences

    def format_metrics_section(self, section_name, metrics):
        """Format a metrics section for the report"""
        lines = [
            f"\n{section_name}",
            "-" * len(section_name)
        ]
        
        # Calculate column widths
        name_width = max(len(m['name']) for m in metrics)
        orig_width = max(len(m['original']) for m in metrics)
        opt_width = max(len(m['optimized']) for m in metrics)
        
        # Header
        lines.append(
            f"{'Metric':<{name_width}} | {'Original':<{orig_width}} | {'Optimized':<{opt_width}} | Change"
        )
        lines.append("-" * (name_width + orig_width + opt_width + 30))
        
        # Metrics
        for metric in metrics:
            lines.append(
                f"{metric['name']:<{name_width}} | "
                f"{metric['original']:<{orig_width}} | "
                f"{metric['optimized']:<{opt_width}} | "
                f"{metric['improvement']}"
            )
            
        return lines

    def format_improvement(self, original, optimized):
        """Format improvement percentage"""
        # Handle time strings
        if isinstance(original, str) and isinstance(optimized, str):
            if any(unit in original + optimized for unit in ['ms', 's']):
                original = self._convert_to_ms(original)
                optimized = self._convert_to_ms(optimized)
        
        if original == 0:
            return "N/A" if optimized == 0 else "∞"
            
        change = ((original - optimized) / original) * 100
        
        if optimized < original:
            return f" 🟢 {change:.1f}% faster"
        elif optimized > original:
            return f" 🔴 {abs(change):.1f}% slower"
        else:
            return "No change"

    def analyze_plan_differences(self, original_plan, optimized_plan):
        """Analyze differences between original and optimized plans"""
        differences = []
        
        # Helper function to get plan node types
        def get_node_types(plan):
            if isinstance(plan, list):
                plan = plan[0]
            if 'Plan' in plan:
                plan = plan['Plan']
                
            types = [plan.get('Node Type', '')]
            if 'Plans' in plan:
                for child in plan['Plans']:
                    types.extend(get_node_types(child))
            return types
        
        # Compare node types
        original_types = get_node_types(original_plan)
        optimized_types = get_node_types(optimized_plan)
        
        # Check for sequential scans replaced by index scans
        orig_seqscans = original_types.count('Seq Scan')
        opt_seqscans = optimized_types.count('Seq Scan')
        if opt_seqscans < orig_seqscans:
            differences.append({
                'type': 'improvement',
                'description': f"Reduced sequential scans from {orig_seqscans} to {opt_seqscans}"
            })
        elif opt_seqscans > orig_seqscans:
            differences.append({
                'type': 'warning',
                'description': f"Increased sequential scans from {orig_seqscans} to {opt_seqscans}"
            })
        
        # Compare index usage
        orig_indexscans = sum(1 for t in original_types if 'Index' in t)
        opt_indexscans = sum(1 for t in optimized_types if 'Index' in t)
        if opt_indexscans > orig_indexscans:
            differences.append({
                'type': 'improvement',
                'description': f"Increased index usage from {orig_indexscans} to {opt_indexscans} scans"
            })
        
        return differences

    def format_plan_text(self, plan, level=0):
        """Format a plan node for text display"""
        if isinstance(plan, list):
            plan = plan[0]
        if 'Plan' in plan:
            plan = plan['Plan']
            
        lines = []
        indent = "  " * level
        
        # Node type and basic info
        node_type = plan.get('Node Type', 'Unknown')
        relation = f" on {plan['Relation Name']}" if 'Relation Name' in plan else ""
        lines.append(f"{indent}→ {node_type}{relation}")
        
        # Detailed metrics
        details = []
        
        # Rows
        if 'Actual Rows' in plan:
            details.append(f"Rows: {plan['Actual Rows']:,}")
            if 'Plan Rows' in plan:
                estimate_ratio = plan['Actual Rows'] / plan['Plan Rows'] if plan['Plan Rows'] > 0 else float('inf')
                details.append(f"Row Estimate: {plan['Plan Rows']:,} ({estimate_ratio:.1f}x)")
        
        # Time
        if 'Actual Total Time' in plan:
            details.append(f"Time: {self.format_time(plan['Actual Total Time'])}")
            if 'Actual Startup Time' in plan:
                details.append(f"Startup: {self.format_time(plan['Actual Startup Time'])}")
        
        # Cost
        if 'Total Cost' in plan:
            details.append(f"Cost: {plan['Total Cost']:.1f}")
        
        # Operation details
        if plan.get('Index Name'):
            details.append(f"Index: {plan['Index Name']}")
        if plan.get('Hash Cond'):
            details.append(f"Hash Cond: {plan['Hash Cond']}")
        if plan.get('Index Cond'):
            details.append(f"Index Cond: {plan['Index Cond']}")
        if plan.get('Filter'):
            details.append(f"Filter: {plan['Filter']}")
            if plan.get('Rows Removed by Filter', 0) > 0:
                details.append(f"Rows Filtered: {plan['Rows Removed by Filter']:,}")
        
        # Add details with proper indentation
        for detail in details:
            lines.append(f"{indent}  {detail}")
        
        # Process child nodes
        if 'Plans' in plan:
            for child in plan['Plans']:
                lines.append("")  # Add spacing between nodes
                lines.extend(self.format_plan_text(child, level + 1).split('\n'))
        
        return '\n'.join(lines)
        
    def format_problems_text(self, problems):
        """Format problems for text display"""
        if not problems:
            return "No issues found."
            
        lines = []
        for severity in ['high', 'medium', 'low']:
            severity_problems = [p for p in problems if p['severity'] == severity]
            if severity_problems:
                lines.extend([
                    f"\n{severity.upper()} Severity Issues:",
                    *[f"  - {p['description']}" for p in severity_problems]
                ])
        return '\n'.join(lines)

    def analyze_queries(self):
        """Run and analyze both queries"""
        # Analyze original query
        original_metrics = self.analyze_query(self.original_query)
        
        # Analyze optimized query
        optimized_metrics = self.analyze_query(self.optimized_query)
        
        return {
            'original': original_metrics,
            'optimized': optimized_metrics
        }
        
    def generate_text_report(self, metrics_data=None):
        """Generate a comprehensive text report comparing the queries"""
        if metrics_data is None:
            metrics_data = self.analyze_queries()
            
        # Get all analysis data
        query_metrics = self.analyze_query_metrics(metrics_data['original'], metrics_data['optimized'])
        original_problems = self.analyze_node_problems(metrics_data['original']['raw_plan'])
        optimized_problems = self.analyze_node_problems(metrics_data['optimized']['raw_plan'])
        plan_differences = self.analyze_plan_differences(metrics_data['original']['raw_plan'], metrics_data['optimized']['raw_plan'])
        original_index_recommendations = self.analyze_index_recommendations(metrics_data['original']['raw_plan'])
        optimized_index_recommendations = self.analyze_index_recommendations(metrics_data['optimized']['raw_plan'])
        
        # Get node timing analysis
        original_timings = self._analyze_node_timings(metrics_data['original']['raw_plan'])
        optimized_timings = self._analyze_node_timings(metrics_data['optimized']['raw_plan'])
        
        # Get statistical analysis
        original_node_stats = self._analyze_node_type_stats(original_timings)
        optimized_node_stats = self._analyze_node_type_stats(optimized_timings)
        original_table_stats = self._analyze_table_stats(original_timings)
        optimized_table_stats = self._analyze_table_stats(optimized_timings)

        lines = []
        
        # Header
        lines.extend([
            "PostgreSQL Query Performance Analysis Report",
            "========================================",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Executive Summary",
            "-----------------",
            self._generate_executive_summary(query_metrics, plan_differences),
            ""
        ])
        
        # Performance Metrics
        lines.append("\nPerformance Metrics")
        lines.append("==================")
        
        for section in query_metrics.values():
            lines.extend(self.format_metrics_section(section['name'], section['metrics']))
        
        # Statistical Analysis
        lines.extend([
            "\nOriginal Query Statistics",
            "=======================",
            "\nPer Node Type Statistics:",
            self.format_node_type_stats(original_node_stats),
            "\nPer Table Statistics:",
            self.format_table_stats(original_table_stats),
            "\nOptimized Query Statistics",
            "========================",
            "\nPer Node Type Statistics:",
            self.format_node_type_stats(optimized_node_stats),
            "\nPer Table Statistics:",
            self.format_table_stats(optimized_table_stats)
        ])
        
        # Node Timing Analysis
        lines.extend([
            "\nDetailed Node-Level Analysis",
            "==========================",
            "\nOriginal Query Node Timings:",
            "--------------------------",
            self.format_node_timings(original_timings),
            "\nOptimized Query Node Timings:",
            "---------------------------",
            self.format_node_timings(optimized_timings)
        ])
        
        # Plan Analysis
        lines.extend([
            "\nQuery Plan Analysis",
            "==================",
            "\nPlan Changes and Optimizations:",
        ])
        
        improvements = [d for d in plan_differences if d['type'] == 'improvement']
        warnings = [d for d in plan_differences if d['type'] == 'warning']
        changes = [d for d in plan_differences if d['type'] == 'info']
        
        if improvements:
            lines.extend([
                "\nImprovements:",
                *[f"  - {d['description']}" for d in improvements]
            ])
        if warnings:
            lines.extend([
                "\nConcerns:",
                *[f"  - {d['description']}" for d in warnings]
            ])
        if changes:
            lines.extend([
                "\nOther Changes:",
                *[f"  - {d['description']}" for d in changes]
            ])
        
        # Original Query Analysis
        lines.extend([
            "\nOriginal Query Analysis",
            "=====================",
            "\nExecution Plan:",
            self.format_plan_text(metrics_data['original']['raw_plan']),
            "\nIdentified Issues:",
            self.format_problems_text(original_problems)
        ])
        
        if original_index_recommendations:
            lines.extend([
                "\nIndex Recommendations:",
                self.format_index_recommendations(original_index_recommendations)
            ])
        
        # Optimized Query Analysis
        lines.extend([
            "\nOptimized Query Analysis",
            "======================",
            "\nExecution Plan:",
            self.format_plan_text(metrics_data['optimized']['raw_plan']),
            "\nIdentified Issues:",
            self.format_problems_text(optimized_problems)
        ])
        
        if optimized_index_recommendations:
            lines.extend([
                "\nIndex Recommendations:",
                self.format_index_recommendations(optimized_index_recommendations)
            ])
        
        return '\n'.join(lines)

    def _convert_to_ms(self, time_str):
        """Convert time string to milliseconds"""
        parts = time_str.split()
        value = float(''.join(c for c in parts[0] if c.isdigit() or c == '.'))
        unit = parts[0].strip('0123456789.')
        if not unit:
            unit = parts[1] if len(parts) > 1 else 'ms'
        
        # Convert to milliseconds
        if unit == 's':
            return value * 1000
        elif unit == 'μs':
            return value / 1000
        return value

    def _generate_executive_summary(self, metrics, differences):
        """Generate an executive summary of the analysis"""
        try:
            # Convert times to milliseconds for comparison
            orig_time = self._convert_to_ms(metrics['timing']['metrics'][2]['original'])
            opt_time = self._convert_to_ms(metrics['timing']['metrics'][2]['optimized'])
            
            time_change = ((orig_time - opt_time) / orig_time) * 100
            
            improvements = [d for d in differences if d['type'] == 'improvement']
            warnings = [d for d in differences if d['type'] == 'warning']
            
            summary = []
            
            # Overall assessment with more context
            if opt_time < orig_time:
                summary.append(f"✅ Performance Improvement")
                summary.append(f"Total time reduced by {time_change:.1f}% ({metrics['timing']['metrics'][2]['original']} → {metrics['timing']['metrics'][2]['optimized']})")
                if time_change > 90:
                    summary.append("🚀 This is a significant optimization!")
            else:
                summary.append(f"⚠️ Performance Regression")
                summary.append(f"Total time increased by {abs(time_change):.1f}% ({metrics['timing']['metrics'][2]['original']} → {metrics['timing']['metrics'][2]['optimized']})")
                summary.append("❗ The optimized version is taking longer to execute")
            
            # Execution time breakdown
            summary.append("\nTime Breakdown:")
            summary.append(f"• Planning:  {metrics['timing']['metrics'][0]['original']} → {metrics['timing']['metrics'][0]['optimized']}")
            summary.append(f"• Execution: {metrics['timing']['metrics'][1]['original']} → {metrics['timing']['metrics'][1]['optimized']}")
            
            # Key improvements
            if improvements:
                summary.append("\nKey Improvements:")
                summary.extend([f"• {d['description']}" for d in improvements[:3]])
                
            # Key concerns
            if warnings:
                summary.append("\nKey Concerns:")
                summary.extend([f"• {d['description']}" for d in warnings[:3]])
                
            # Buffer efficiency
            shared_hit_ratio = float(metrics['buffers']['metrics'][0]['optimized'].rstrip('%'))
            if shared_hit_ratio > 90:
                summary.append(f"\n✅ Buffer Efficiency: Excellent ({shared_hit_ratio:.1f}% hit ratio)")
            elif shared_hit_ratio > 70:
                summary.append(f"\n📊 Buffer Efficiency: Good ({shared_hit_ratio:.1f}% hit ratio)")
            else:
                summary.append(f"\n⚠️ Buffer Efficiency: Poor ({shared_hit_ratio:.1f}% hit ratio)")
            
            return '\n'.join(summary)
        except Exception as e:
            return f"Error generating summary: {str(e)}\nPlease check the detailed metrics below."

    def _generate_executive_summary_html(self, metrics_data, differences):
        """Generate HTML executive summary"""
        # Get total time difference
        total_time_diff = next(
            (d for d in differences if d['type'] == 'total_time'),
            None
        )
        
        # Get row count difference
        row_diff = next(
            (d for d in differences if d['type'] == 'rows'),
            None
        )
        
        # Generate summary text
        summary_text = []
        
        if total_time_diff:
            change = float(total_time_diff['change'].rstrip('%'))
            if change < 0:
                summary_text.append(
                    f"<div class='improvement'><h3>Performance Improvement</h3>"
                    f"<p>The optimized query is {abs(change):.1f}% faster than the original query.</p></div>"
                )
            else:
                summary_text.append(
                    f"<div class='warning'><h3>Performance Regression</h3>"
                    f"<p>The optimized query is {change:.1f}% slower than the original query.</p></div>"
                )
        
        if row_diff:
            orig_rows = row_diff['original']
            opt_rows = row_diff['optimized']
            change = float(row_diff['change'].rstrip('%'))
            
            if orig_rows != opt_rows:
                summary_text.append(
                    f"<div class='warning'><h3>Row Count Difference</h3>"
                    f"<p>The optimized query returns {opt_rows} rows, while the original returns {orig_rows} rows "
                    f"({row_diff['change']} difference). This may indicate a semantic change in the query.</p></div>"
                )
        
        # Add query text
        summary_text.extend([
            "<h3>Original Query</h3>",
            f"<pre><code>{self.original_query}</code></pre>",
            "<h3>Optimized Query</h3>",
            f"<pre><code>{self.optimized_query}</code></pre>"
        ])
        
        return '\n'.join(summary_text)
        
    def _format_change(self, original, optimized):
        """Format the change between original and optimized values as a percentage"""
        if original == 0:
            if optimized == 0:
                return "0%"
            return "+∞%"
        change = ((optimized - original) / original) * 100
        if change > 0:
            return f"+{change:.1f}%"
        return f"{change:.1f}%"

    def generate_html_report(self, metrics_data=None):
        """Generate HTML report"""
        if metrics_data is None:
            metrics_data = self.analyze_queries()
            
        # Get problems and recommendations
        original_problems = self.analyze_plan_problems(metrics_data['original']['raw_plan'])
        optimized_problems = self.analyze_plan_problems(metrics_data['optimized']['raw_plan'])
        index_recommendations = self.get_index_recommendations(
            metrics_data['original']['raw_plan'],
            metrics_data['optimized']['raw_plan']
        )
        
        # Calculate differences
        differences = []
        
        # Compare planning time
        orig_planning = metrics_data['original'].get('planning_time', 0)
        opt_planning = metrics_data['optimized'].get('planning_time', 0)
        if orig_planning != opt_planning:
            differences.append({
                'type': 'planning_time',
                'original': orig_planning,
                'optimized': opt_planning,
                'change': self._format_change(orig_planning, opt_planning)
            })
        
        # Compare execution time
        orig_execution = metrics_data['original'].get('execution_time', 0)
        opt_execution = metrics_data['optimized'].get('execution_time', 0)
        if orig_execution != opt_execution:
            differences.append({
                'type': 'execution_time',
                'original': orig_execution,
                'optimized': opt_execution,
                'change': self._format_change(orig_execution, opt_execution)
            })
        
        # Compare total time
        orig_total = orig_planning + orig_execution
        opt_total = opt_planning + opt_execution
        if orig_total != opt_total:
            differences.append({
                'type': 'total_time',
                'original': orig_total,
                'optimized': opt_total,
                'change': self._format_change(orig_total, opt_total)
            })
        
        # Compare row counts
        orig_rows = metrics_data['original'].get('row_count', 0)
        opt_rows = metrics_data['optimized'].get('row_count', 0)
        if orig_rows != opt_rows:
            differences.append({
                'type': 'rows',
                'original': orig_rows,
                'optimized': opt_rows,
                'change': self._format_change(orig_rows, opt_rows)
            })
        
        # Generate sections
        exec_summary = self._generate_executive_summary_html(metrics_data, differences)
        perf_metrics = self._format_performance_metrics_html(metrics_data)
        query_stats = self._format_query_stats_html(metrics_data)
        plan_analysis = self._format_plan_analysis_html(metrics_data)
        problems = self._format_problems_section_html(original_problems, optimized_problems, index_recommendations)
        
        # Combine all sections
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='utf-8'>",
            "<title>PostgreSQL Query Performance Analysis Report</title>",
            "<style>",
            "body { font-family: monospace; line-height: 1.4; margin: 20px; }",
            "h1, h2, h3, h4 { color: #333; margin: 1em 0 0.5em 0; }",
            "h1 { border-bottom: 2px solid #333; padding-bottom: 0.2em; }",
            "h2 { border-bottom: 1px solid #666; }",
            "table { border-collapse: collapse; width: 100%; margin: 1em 0; }",
            "th, td { text-align: left; padding: 0.3em 1em; font-family: monospace; }",
            "th { border-bottom: 1px solid #666; }",
            ".metric-table td:first-child { width: 200px; }",
            ".metric-table td:nth-child(2), .metric-table td:nth-child(3) { text-align: right; width: 100px; }",
            ".metric-table td:nth-child(4) { text-align: left; padding-left: 2em; }",
            ".improvement { color: #28a745; }",
            ".warning { color: #dc3545; }",
            "div.improvement, div.warning { padding: 0.5em; margin: 0.5em 0; }",
            "div.improvement h3, div.warning h3 { color: inherit; margin: 0; }",
            "ul { list-style-type: none; padding-left: 0; margin: 0.5em 0; }",
            "li { margin: 0.2em 0; }",
            ".plan-table { font-family: monospace; }",
            ".plan-table td:first-child { white-space: pre; }",
            "pre { background-color: #f8f9fa; padding: 1em; border-radius: 4px; overflow-x: auto; }",
            "code { font-family: monospace; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>PostgreSQL Query Performance Analysis Report</h1>",
            f"<p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
            "<h2>Executive Summary</h2>",
            exec_summary,
            "<h2>Performance Metrics</h2>",
            perf_metrics,
            "<h2>Query Statistics</h2>",
            query_stats,
            "<h2>Execution Plan Analysis</h2>",
            plan_analysis,
            "<h2>Problems and Recommendations</h2>",
            problems,
            "</body>",
            "</html>"
        ]
        
        return '\n'.join(html)

    def _format_metrics_section_html(self, section_name, metrics):
        """Format metrics section as HTML table"""
        html = [
            "<table class='metric-table'>",
            "<tr>",
            "<th>Metric</th>",
            "<th>Original</th>",
            "<th>Optimized</th>",
            "<th>Change</th>",
            "</tr>"
        ]
        
        for metric in metrics:
            html.append(
                f"<tr><td>{metric['name']}</td>"
                f"<td>{metric['original']}</td>"
                f"<td>{metric['optimized']}</td>"
                f"<td class='{metric['css_class']}'>{metric['improvement']}</td></tr>"
            )
        
        html.append("</table>")
        return '\n'.join(html)
        
    def _format_node_type_stats_html(self, stats):
        """Format node type statistics as HTML table"""
        html = [
            "<h4>Node Type Statistics</h4>",
            "<table>",
            "<tr>",
            "<th>Node Type</th>",
            "<th>Count</th>",
            "<th>Total Time</th>",
            "<th>% of Query</th>",
            "</tr>"
        ]
        
        for stat in stats:
            # Format cells to align with text report
            node_type = stat['node_type'].ljust(30)
            count = str(stat['count']).rjust(7)
            total_time = f"{stat['total_time']:.3f} ms".rjust(12)
            percentage = f"{stat['percentage']:.1f} %".rjust(10)
            
            html.append(
                f"<tr>"
                f"<td>{node_type}</td>"
                f"<td>{count}</td>"
                f"<td>{total_time}</td>"
                f"<td>{percentage}</td>"
                f"</tr>"
            )
            
        html.append("</table>")
        return '\n'.join(html)
        
    def _format_table_stats_html(self, stats):
        """Format table statistics as HTML"""
        html = ["<h4>Table Statistics</h4>"]
        
        for table_stat in stats:
            # Table header
            html.extend([
                f"<h5>{table_stat['table']}</h5>",
                f"<p>Total Time: {table_stat['total_time']:.3f} ms ({table_stat['percentage']:.1f}% of query)</p>",
                "<table>",
                "<tr>",
                "<th>Scan Type</th>",
                "<th>Count</th>",
                "<th>Total Time</th>",
                "<th>% of Table</th>",
                "</tr>"
            ])
            
            for scan in table_stat['scan_stats']:
                # Format cells to align with text report
                scan_type = scan['scan_type'].ljust(30)
                count = str(scan['count']).rjust(7)
                total_time = f"{scan['total_time']:.3f} ms".rjust(12)
                percentage = f"{scan['percentage']:.1f} %".rjust(10)
                
                html.append(
                    f"<tr>"
                    f"<td>{scan_type}</td>"
                    f"<td>{count}</td>"
                    f"<td>{total_time}</td>"
                    f"<td>{percentage}</td>"
                    f"</tr>"
                )
            
            html.extend(["</table>"])
        
        return '\n'.join(html)

    def format_plan_text(self, plan, level=0):
        """Format a plan node for text display"""
        if isinstance(plan, list):
            plan = plan[0]
        if 'Plan' in plan:
            plan = plan['Plan']
            
        lines = []
        indent = "  " * level
        
        # Node type and basic info
        node_type = plan.get('Node Type', 'Unknown')
        relation = f" on {plan['Relation Name']}" if 'Relation Name' in plan else ""
        lines.append(f"{indent}→ {node_type}{relation}")
        
        # Detailed metrics
        details = []
        
        # Rows
        if 'Actual Rows' in plan:
            details.append(f"Rows: {plan['Actual Rows']:,}")
            if 'Plan Rows' in plan:
                estimate_ratio = plan['Actual Rows'] / plan['Plan Rows'] if plan['Plan Rows'] > 0 else float('inf')
                details.append(f"Row Estimate: {plan['Plan Rows']:,} ({estimate_ratio:.1f}x)")
        
        # Time
        if 'Actual Total Time' in plan:
            details.append(f"Time: {self.format_time(plan['Actual Total Time'])}")
            if 'Actual Startup Time' in plan:
                details.append(f"Startup: {self.format_time(plan['Actual Startup Time'])}")
        
        # Cost
        if 'Total Cost' in plan:
            details.append(f"Cost: {plan['Total Cost']:.1f}")
        
        # Operation details
        if plan.get('Index Name'):
            details.append(f"Index: {plan['Index Name']}")
        if plan.get('Hash Cond'):
            details.append(f"Hash Cond: {plan['Hash Cond']}")
        if plan.get('Index Cond'):
            details.append(f"Index Cond: {plan['Index Cond']}")
        if plan.get('Filter'):
            details.append(f"Filter: {plan['Filter']}")
            if plan.get('Rows Removed by Filter', 0) > 0:
                details.append(f"Rows Filtered: {plan['Rows Removed by Filter']:,}")
        
        # Add details with proper indentation
        for detail in details:
            lines.append(f"{indent}  {detail}")
        
        # Process child nodes
        if 'Plans' in plan:
            for child in plan['Plans']:
                lines.append("")  # Add spacing between nodes
                lines.extend(self.format_plan_text(child, level + 1).split('\n'))
        
        return '\n'.join(lines)
        
    def format_problems_text(self, problems):
        """Format problems for text display"""
        if not problems:
            return "No issues found."
            
        lines = []
        for severity in ['high', 'medium', 'low']:
            severity_problems = [p for p in problems if p['severity'] == severity]
            if severity_problems:
                lines.extend([
                    f"\n{severity.upper()} Severity Issues:",
                    *[f"  - {p['description']}" for p in severity_problems]
                ])
        return '\n'.join(lines)

    def analyze_queries(self):
        """Run and analyze both queries"""
        # Analyze original query
        original_metrics = self.analyze_query(self.original_query)
        
        # Analyze optimized query
        optimized_metrics = self.analyze_query(self.optimized_query)
        
        return {
            'original': original_metrics,
            'optimized': optimized_metrics
        }
        
    def generate_text_report(self, metrics_data=None):
        """Generate a comprehensive text report comparing the queries"""
        if metrics_data is None:
            metrics_data = self.analyze_queries()
            
        # Get all analysis data
        query_metrics = self.analyze_query_metrics(metrics_data['original'], metrics_data['optimized'])
        original_problems = self.analyze_node_problems(metrics_data['original']['raw_plan'])
        optimized_problems = self.analyze_node_problems(metrics_data['optimized']['raw_plan'])
        plan_differences = self.analyze_plan_differences(metrics_data['original']['raw_plan'], metrics_data['optimized']['raw_plan'])
        original_index_recommendations = self.analyze_index_recommendations(metrics_data['original']['raw_plan'])
        optimized_index_recommendations = self.analyze_index_recommendations(metrics_data['optimized']['raw_plan'])
        
        # Get node timing analysis
        original_timings = self._analyze_node_timings(metrics_data['original']['raw_plan'])
        optimized_timings = self._analyze_node_timings(metrics_data['optimized']['raw_plan'])
        
        # Get statistical analysis
        original_node_stats = self._analyze_node_type_stats(original_timings)
        optimized_node_stats = self._analyze_node_type_stats(optimized_timings)
        original_table_stats = self._analyze_table_stats(original_timings)
        optimized_table_stats = self._analyze_table_stats(optimized_timings)

        lines = []
        
        # Header
        lines.extend([
            "PostgreSQL Query Performance Analysis Report",
            "========================================",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Executive Summary",
            "-----------------",
            self._generate_executive_summary(query_metrics, plan_differences),
            ""
        ])
        
        # Performance Metrics
        lines.append("\nPerformance Metrics")
        lines.append("==================")
        
        for section in query_metrics.values():
            lines.extend(self.format_metrics_section(section['name'], section['metrics']))
        
        # Statistical Analysis
        lines.extend([
            "\nOriginal Query Statistics",
            "=======================",
            "\nPer Node Type Statistics:",
            self.format_node_type_stats(original_node_stats),
            "\nPer Table Statistics:",
            self.format_table_stats(original_table_stats),
            "\nOptimized Query Statistics",
            "========================",
            "\nPer Node Type Statistics:",
            self.format_node_type_stats(optimized_node_stats),
            "\nPer Table Statistics:",
            self.format_table_stats(optimized_table_stats)
        ])
        
        # Node Timing Analysis
        lines.extend([
            "\nDetailed Node-Level Analysis",
            "==========================",
            "\nOriginal Query Node Timings:",
            "--------------------------",
            self.format_node_timings(original_timings),
            "\nOptimized Query Node Timings:",
            "---------------------------",
            self.format_node_timings(optimized_timings)
        ])
        
        # Plan Analysis
        lines.extend([
            "\nQuery Plan Analysis",
            "==================",
            "\nPlan Changes and Optimizations:",
        ])
        
        improvements = [d for d in plan_differences if d['type'] == 'improvement']
        warnings = [d for d in plan_differences if d['type'] == 'warning']
        changes = [d for d in plan_differences if d['type'] == 'info']
        
        if improvements:
            lines.extend([
                "\nImprovements:",
                *[f"  - {d['description']}" for d in improvements]
            ])
        if warnings:
            lines.extend([
                "\nConcerns:",
                *[f"  - {d['description']}" for d in warnings]
            ])
        if changes:
            lines.extend([
                "\nOther Changes:",
                *[f"  - {d['description']}" for d in changes]
            ])
        
        # Original Query Analysis
        lines.extend([
            "\nOriginal Query Analysis",
            "=====================",
            "\nExecution Plan:",
            self.format_plan_text(metrics_data['original']['raw_plan']),
            "\nIdentified Issues:",
            self.format_problems_text(original_problems)
        ])
        
        if original_index_recommendations:
            lines.extend([
                "\nIndex Recommendations:",
                self.format_index_recommendations(original_index_recommendations)
            ])
        
        # Optimized Query Analysis
        lines.extend([
            "\nOptimized Query Analysis",
            "======================",
            "\nExecution Plan:",
            self.format_plan_text(metrics_data['optimized']['raw_plan']),
            "\nIdentified Issues:",
            self.format_problems_text(optimized_problems)
        ])
        
        if optimized_index_recommendations:
            lines.extend([
                "\nIndex Recommendations:",
                self.format_index_recommendations(optimized_index_recommendations)
            ])
        
        return '\n'.join(lines)

    def _convert_to_ms(self, time_str):
        """Convert time string to milliseconds"""
        parts = time_str.split()
        value = float(''.join(c for c in parts[0] if c.isdigit() or c == '.'))
        unit = parts[0].strip('0123456789.')
        if not unit:
            unit = parts[1] if len(parts) > 1 else 'ms'
        
        # Convert to milliseconds
        if unit == 's':
            return value * 1000
        elif unit == 'μs':
            return value / 1000
        return value

    def _generate_executive_summary(self, metrics, differences):
        """Generate an executive summary of the analysis"""
        try:
            # Convert times to milliseconds for comparison
            orig_time = self._convert_to_ms(metrics['timing']['metrics'][2]['original'])
            opt_time = self._convert_to_ms(metrics['timing']['metrics'][2]['optimized'])
            
            time_change = ((orig_time - opt_time) / orig_time) * 100
            
            improvements = [d for d in differences if d['type'] == 'improvement']
            warnings = [d for d in differences if d['type'] == 'warning']
            
            summary = []
            
            # Overall assessment with more context
            if opt_time < orig_time:
                summary.append(f"✅ Performance Improvement")
                summary.append(f"Total time reduced by {time_change:.1f}% ({metrics['timing']['metrics'][2]['original']} → {metrics['timing']['metrics'][2]['optimized']})")
                if time_change > 90:
                    summary.append("🚀 This is a significant optimization!")
            else:
                summary.append(f"⚠️ Performance Regression")
                summary.append(f"Total time increased by {abs(time_change):.1f}% ({metrics['timing']['metrics'][2]['original']} → {metrics['timing']['metrics'][2]['optimized']})")
                summary.append("❗ The optimized version is taking longer to execute")
            
            # Execution time breakdown
            summary.append("\nTime Breakdown:")
            summary.append(f"• Planning:  {metrics['timing']['metrics'][0]['original']} → {metrics['timing']['metrics'][0]['optimized']}")
            summary.append(f"• Execution: {metrics['timing']['metrics'][1]['original']} → {metrics['timing']['metrics'][1]['optimized']}")
            
            # Key improvements
            if improvements:
                summary.append("\nKey Improvements:")
                summary.extend([f"• {d['description']}" for d in improvements[:3]])
                
            # Key concerns
            if warnings:
                summary.append("\nKey Concerns:")
                summary.extend([f"• {d['description']}" for d in warnings[:3]])
                
            # Buffer efficiency
            shared_hit_ratio = float(metrics['buffers']['metrics'][0]['optimized'].rstrip('%'))
            if shared_hit_ratio > 90:
                summary.append(f"\n✅ Buffer Efficiency: Excellent ({shared_hit_ratio:.1f}% hit ratio)")
            elif shared_hit_ratio > 70:
                summary.append(f"\n📊 Buffer Efficiency: Good ({shared_hit_ratio:.1f}% hit ratio)")
            else:
                summary.append(f"\n⚠️ Buffer Efficiency: Poor ({shared_hit_ratio:.1f}% hit ratio)")
            
            return '\n'.join(summary)
        except Exception as e:
            return f"Error generating summary: {str(e)}\nPlease check the detailed metrics below."

    def _generate_executive_summary_html(self, metrics_data, differences):
        """Generate HTML executive summary"""
        # Get total time difference
        total_time_diff = next(
            (d for d in differences if d['type'] == 'total_time'),
            None
        )
        
        # Get row count difference
        row_diff = next(
            (d for d in differences if d['type'] == 'rows'),
            None
        )
        
        # Generate summary text
        summary_text = []
        
        if total_time_diff:
            change = float(total_time_diff['change'].rstrip('%'))
            if change < 0:
                summary_text.append(
                    f"<div class='improvement'><h3>Performance Improvement</h3>"
                    f"<p>The optimized query is {abs(change):.1f}% faster than the original query.</p></div>"
                )
            else:
                summary_text.append(
                    f"<div class='warning'><h3>Performance Regression</h3>"
                    f"<p>The optimized query is {change:.1f}% slower than the original query.</p></div>"
                )
        
        if row_diff:
            orig_rows = row_diff['original']
            opt_rows = row_diff['optimized']
            change = float(row_diff['change'].rstrip('%'))
            
            if orig_rows != opt_rows:
                summary_text.append(
                    f"<div class='warning'><h3>Row Count Difference</h3>"
                    f"<p>The optimized query returns {opt_rows} rows, while the original returns {orig_rows} rows "
                    f"({row_diff['change']} difference). This may indicate a semantic change in the query.</p></div>"
                )
        
        # Add query text
        summary_text.extend([
            "<h3>Original Query</h3>",
            f"<pre><code>{self.original_query}</code></pre>",
            "<h3>Optimized Query</h3>",
            f"<pre><code>{self.optimized_query}</code></pre>"
        ])
        
        return '\n'.join(summary_text)
        
    def _format_change(self, original, optimized):
        """Format the change between original and optimized values as a percentage"""
        if original == 0:
            if optimized == 0:
                return "0%"
            return "+∞%"
        change = ((optimized - original) / original) * 100
        if change > 0:
            return f"+{change:.1f}%"
        return f"{change:.1f}%"

    def generate_html_report(self, metrics_data=None):
        """Generate HTML report"""
        if metrics_data is None:
            metrics_data = self.analyze_queries()
            
        # Get problems and recommendations
        original_problems = self.analyze_plan_problems(metrics_data['original']['raw_plan'])
        optimized_problems = self.analyze_plan_problems(metrics_data['optimized']['raw_plan'])
        index_recommendations = self.get_index_recommendations(
            metrics_data['original']['raw_plan'],
            metrics_data['optimized']['raw_plan']
        )
        
        # Calculate differences
        differences = []
        
        # Compare planning time
        orig_planning = metrics_data['original'].get('planning_time', 0)
        opt_planning = metrics_data['optimized'].get('planning_time', 0)
        if orig_planning != opt_planning:
            differences.append({
                'type': 'planning_time',
                'original': orig_planning,
                'optimized': opt_planning,
                'change': self._format_change(orig_planning, opt_planning)
            })
        
        # Compare execution time
        orig_execution = metrics_data['original'].get('execution_time', 0)
        opt_execution = metrics_data['optimized'].get('execution_time', 0)
        if orig_execution != opt_execution:
            differences.append({
                'type': 'execution_time',
                'original': orig_execution,
                'optimized': opt_execution,
                'change': self._format_change(orig_execution, opt_execution)
            })
        
        # Compare total time
        orig_total = orig_planning + orig_execution
        opt_total = opt_planning + opt_execution
        if orig_total != opt_total:
            differences.append({
                'type': 'total_time',
                'original': orig_total,
                'optimized': opt_total,
                'change': self._format_change(orig_total, opt_total)
            })
        
        # Compare row counts
        orig_rows = metrics_data['original'].get('row_count', 0)
        opt_rows = metrics_data['optimized'].get('row_count', 0)
        if orig_rows != opt_rows:
            differences.append({
                'type': 'rows',
                'original': orig_rows,
                'optimized': opt_rows,
                'change': self._format_change(orig_rows, opt_rows)
            })
        
        # Generate sections
        exec_summary = self._generate_executive_summary_html(metrics_data, differences)
        perf_metrics = self._format_performance_metrics_html(metrics_data)
        query_stats = self._format_query_stats_html(metrics_data)
        plan_analysis = self._format_plan_analysis_html(metrics_data)
        problems = self._format_problems_section_html(original_problems, optimized_problems, index_recommendations)
        
        # Combine all sections
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='utf-8'>",
            "<title>PostgreSQL Query Performance Analysis Report</title>",
            "<style>",
            "body { font-family: monospace; line-height: 1.4; margin: 20px; }",
            "h1, h2, h3, h4 { color: #333; margin: 1em 0 0.5em 0; }",
            "h1 { border-bottom: 2px solid #333; padding-bottom: 0.2em; }",
            "h2 { border-bottom: 1px solid #666; }",
            "table { border-collapse: collapse; width: 100%; margin: 1em 0; }",
            "th, td { text-align: left; padding: 0.3em 1em; font-family: monospace; }",
            "th { border-bottom: 1px solid #666; }",
            ".metric-table td:first-child { width: 200px; }",
            ".metric-table td:nth-child(2), .metric-table td:nth-child(3) { text-align: right; width: 100px; }",
            ".metric-table td:nth-child(4) { text-align: left; padding-left: 2em; }",
            ".improvement { color: #28a745; }",
            ".warning { color: #dc3545; }",
            "div.improvement, div.warning { padding: 0.5em; margin: 0.5em 0; }",
            "div.improvement h3, div.warning h3 { color: inherit; margin: 0; }",
            "ul { list-style-type: none; padding-left: 0; margin: 0.5em 0; }",
            "li { margin: 0.2em 0; }",
            ".plan-table { font-family: monospace; }",
            ".plan-table td:first-child { white-space: pre; }",
            "pre { background-color: #f8f9fa; padding: 1em; border-radius: 4px; overflow-x: auto; }",
            "code { font-family: monospace; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>PostgreSQL Query Performance Analysis Report</h1>",
            f"<p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
            "<h2>Executive Summary</h2>",
            exec_summary,
            "<h2>Performance Metrics</h2>",
            perf_metrics,
            "<h2>Query Statistics</h2>",
            query_stats,
            "<h2>Execution Plan Analysis</h2>",
            plan_analysis,
            "<h2>Problems and Recommendations</h2>",
            problems,
            "</body>",
            "</html>"
        ]
        
        return '\n'.join(html)

    def _format_metrics_section_html(self, section_name, metrics):
        """Format metrics section as HTML table"""
        html = [
            "<table class='metric-table'>",
            "<tr>",
            "<th>Metric</th>",
            "<th>Original</th>",
            "<th>Optimized</th>",
            "<th>Change</th>",
            "</tr>"
        ]
        
        for metric in metrics:
            html.append(
                f"<tr><td>{metric['name']}</td>"
                f"<td>{metric['original']}</td>"
                f"<td>{metric['optimized']}</td>"
                f"<td class='{metric['css_class']}'>{metric['improvement']}</td></tr>"
            )
        
        html.append("</table>")
        return '\n'.join(html)
        
    def _format_node_type_stats_html(self, stats):
        """Format node type statistics as HTML table"""
        html = [
            "<h4>Node Type Statistics</h4>",
            "<table>",
            "<tr>",
            "<th>Node Type</th>",
            "<th>Count</th>",
            "<th>Total Time</th>",
            "<th>% of Query</th>",
            "</tr>"
        ]
        
        for stat in stats:
            # Format cells to align with text report
            node_type = stat['node_type'].ljust(30)
            count = str(stat['count']).rjust(7)
            total_time = f"{stat['total_time']:.3f} ms".rjust(12)
            percentage = f"{stat['percentage']:.1f} %".rjust(10)
            
            html.append(
                f"<tr>"
                f"<td>{node_type}</td>"
                f"<td>{count}</td>"
                f"<td>{total_time}</td>"
                f"<td>{percentage}</td>"
                f"</tr>"
            )
            
        html.append("</table>")
        return '\n'.join(html)
        
    def _format_table_stats_html(self, stats):
        """Format table statistics as HTML"""
        html = ["<h4>Table Statistics</h4>"]
        
        for table_stat in stats:
            # Table header
            html.extend([
                f"<h5>{table_stat['table']}</h5>",
                f"<p>Total Time: {table_stat['total_time']:.3f} ms ({table_stat['percentage']:.1f}% of query)</p>",
                "<table>",
                "<tr>",
                "<th>Scan Type</th>",
                "<th>Count</th>",
                "<th>Total Time</th>",
                "<th>% of Table</th>",
                "</tr>"
            ])
            
            for scan in table_stat['scan_stats']:
                # Format cells to align with text report
                scan_type = scan['scan_type'].ljust(30)
                count = str(scan['count']).rjust(7)
                total_time = f"{scan['total_time']:.3f} ms".rjust(12)
                percentage = f"{scan['percentage']:.1f} %".rjust(10)
                
                html.append(
                    f"<tr>"
                    f"<td>{scan_type}</td>"
                    f"<td>{count}</td>"
                    f"<td>{total_time}</td>"
                    f"<td>{percentage}</td>"
                    f"</tr>"
                )
            
            html.extend(["</table>"])
        
        return '\n'.join(html)

    def _format_plan_html(self, plan):
        """Format execution plan as HTML table"""
        html = [
            "<table class='plan-table'>",
            "<tr>",
            "<th>Operation</th>",
            "<th>Est. Cost</th>",
            "<th>Est. Rows</th>",
            "<th>Act. Rows</th>",
            "<th>Loops</th>",
            "<th>Time/Loop</th>",
            "<th>Total Time</th>",
            "<th>Buffers</th>",
            "</tr>"
        ]
        
        def format_node(node, depth=0):
            if isinstance(node, list):
                node = node[0]
            if 'Plan' in node:
                node = node['Plan']
                
            indent = "&nbsp;" * (depth * 2)
            node_type = node.get('Node Type', '')
            relation = node.get('Relation Name', '')
            index = node.get('Index Name', '')
            
            operation = f"{indent}{node_type}"
            if relation:
                operation += f" on {relation}"
            if index:
                operation += f" using {index}"
                
            startup_time = float(node.get('Actual Startup Time', 0))
            total_time = float(node.get('Actual Total Time', 0))
            loops = int(node.get('Actual Loops', 1))
            time_per_loop = total_time / loops if loops > 0 else 0
            
            shared_hit = int(node.get('Shared Hit Blocks', 0))
            shared_read = int(node.get('Shared Read Blocks', 0))
            shared_written = int(node.get('Shared Written Blocks', 0))
            temp_read = int(node.get('Temp Read Blocks', 0))
            temp_written = int(node.get('Temp Written Blocks', 0))
            
            buffer_info = f"H:{shared_hit} R:{shared_read}"
            if shared_written > 0:
                buffer_info += f" W:{shared_written}"
            if temp_read > 0 or temp_written > 0:
                buffer_info += f" T:{temp_read}/{temp_written}"
                
            html.append(
                f"<tr>"
                f"<td>{operation}</td>"
                f"<td>{float(node.get('Total Cost', 0)):.2f}</td>"
                f"<td>{int(node.get('Plan Rows', 0)):,}</td>"
                f"<td>{int(node.get('Actual Rows', 0)):,}</td>"
                f"<td>{loops:,}</td>"
                f"<td>{time_per_loop:.2f}</td>"
                f"<td>{total_time:.2f}</td>"
                f"<td>{buffer_info}</td>"
                f"</tr>"
            )
            
            if node.get('Filter'):
                html.append(f"<tr><td colspan='8'>{indent}&nbsp;&nbsp;Filter: {node['Filter']}</td></tr>")
            if node.get('Index Cond'):
                html.append(f"<tr><td colspan='8'>{indent}&nbsp;&nbsp;Index Cond: {node['Index Cond']}</td></tr>")
            if node.get('Hash Cond'):
                html.append(f"<tr><td colspan='8'>{indent}&nbsp;&nbsp;Hash Cond: {node['Hash Cond']}</td></tr>")
                
            if 'Plans' in node:
                for child in node['Plans']:
                    format_node(child, depth + 1)
                    
        format_node(plan)
        html.append("</table>")
        return '\n'.join(html)
        
    def _format_problems_section_html(self, original_problems, optimized_problems, index_recommendations):
        """Format problems section as HTML"""
        html = []
        
        # Original Query Issues
        html.append("<h3>Original Query Issues</h3>")
        if not original_problems:
            html.append("<p>No issues found.</p>")
        else:
            # High Severity Issues
            high_severity = [p for p in original_problems if p['severity'] == 'HIGH']
            if high_severity:
                html.append("<h4>HIGH Severity Issues</h4>")
                html.append("<ul>")
                for problem in high_severity:
                    html.append(f"<li>⚠️ {problem['description']}</li>")
                html.append("</ul>")
            
            # Medium Severity Issues
            medium_severity = [p for p in original_problems if p['severity'] == 'MEDIUM']
            if medium_severity:
                html.append("<h4>MEDIUM Severity Issues</h4>")
                html.append("<ul>")
                for problem in medium_severity:
                    html.append(f"<li>⚠️ {problem['description']}</li>")
                html.append("</ul>")
        
        # Optimized Query Issues
        html.append("<h3>Optimized Query Issues</h3>")
        if not optimized_problems:
            html.append("<p>No issues found.</p>")
        else:
            # High Severity Issues
            high_severity = [p for p in optimized_problems if p['severity'] == 'HIGH']
            if high_severity:
                html.append("<h4>HIGH Severity Issues</h4>")
                html.append("<ul>")
                for problem in high_severity:
                    html.append(f"<li>⚠️ {problem['description']}</li>")
                html.append("</ul>")
            
            # Medium Severity Issues
            medium_severity = [p for p in optimized_problems if p['severity'] == 'MEDIUM']
            if medium_severity:
                html.append("<h4>MEDIUM Severity Issues</h4>")
                html.append("<ul>")
                for problem in medium_severity:
                    html.append(f"<li>⚠️ {problem['description']}</li>")
                html.append("</ul>")
        
        # Index Recommendations
        html.append("<h3>Index Recommendations</h3>")
        if not index_recommendations:
            html.append("<p>No index recommendations.</p>")
        else:
            html.append("<ul>")
            for recommendation in index_recommendations:
                html.append(f"<li>💡 {recommendation['description']}</li>")
                if 'sql' in recommendation:
                    html.append(f"<pre><code>{recommendation['sql']}</code></pre>")
            html.append("</ul>")
        
        return '\n'.join(html)

    def get_index_recommendations(self, original_plan, optimized_plan):
        """Generate index recommendations based on plan analysis"""
        recommendations = []
        
        # Analyze both plans
        original_problems = self.analyze_plan_problems(original_plan)
        optimized_problems = self.analyze_plan_problems(optimized_plan)
        
        # Helper to extract table and column info from problems
        def extract_table_info(problems):
            tables = {}  # Map table name to its row count
            for problem in problems:
                if 'Sequential scan on large table' in problem['description']:
                    desc = problem['description']
                    table_name = desc.split()[5]  # Extract table name
                    # Extract row count
                    row_count = int(''.join(c for c in desc.split('with')[1].split('rows')[0] if c.isdigit()))
                    tables[table_name] = row_count
            return tables
        
        # Get tables that need indexes
        original_tables = extract_table_info(original_problems)
        optimized_tables = extract_table_info(optimized_problems)
        
        # Combine tables and use max row count
        all_tables = {}
        for table, rows in original_tables.items():
            all_tables[table] = max(rows, optimized_tables.get(table, 0))
        for table, rows in optimized_tables.items():
            if table not in all_tables:
                all_tables[table] = rows
        
        # Generate recommendations for tables that appear in either query
        for table, row_count in all_tables.items():
            # Extract filter conditions from the plan
            filter_cols = self._extract_filter_columns(original_plan, table)
            filter_cols.update(self._extract_filter_columns(optimized_plan, table))
            
            # Generate CREATE INDEX statements
            if filter_cols:
                cols_list = ', '.join(sorted(filter_cols))
                idx_name = f"idx_{table.lower()}_{'_'.join(sorted(filter_cols))}"
                recommendations.append({
                    'description': f"Consider adding an index on {table}({cols_list}) to improve query performance",
                    'sql': f"CREATE INDEX {idx_name} ON {table} ({cols_list});"
                })
            
            # If no specific columns found but table is large, recommend index on commonly used columns
            elif row_count > 1000:
                recommendations.append({
                    'description': f"Consider adding an index on commonly accessed columns of {table} to improve query performance",
                    'sql': None
                })
        
        return recommendations
    
    def _extract_filter_columns(self, plan, target_table):
        """Extract columns used in filter conditions for a specific table"""
        columns = set()
        
        if not plan:
            return columns
        
        # Extract the actual plan node
        if isinstance(plan, list):
            plan = plan[0]
        if 'Plan' in plan:
            plan = plan['Plan']
            
        # Check if this node is for our target table
        if plan.get('Relation Name') == target_table:
            # Extract filter conditions
            filter_cond = plan.get('Filter', '')
            if filter_cond:
                # Simple parsing of filter condition to extract column names
                # This is a basic implementation and might need to be enhanced
                parts = filter_cond.split()
                for part in parts:
                    if '.' in part:
                        col = part.split('.')[-1].strip('()"\'')
                        if col.isidentifier():
                            columns.add(col)
        
        # Recursively check child nodes
        if 'Plans' in plan:
            for child in plan['Plans']:
                columns.update(self._extract_filter_columns({'Plan': child}, target_table))
        
        return columns

    def analyze_plan_problems(self, plan):
        """Analyze plan for potential problems"""
        problems = []
        
        if not plan:
            return problems
        
        # Extract the actual plan node
        if isinstance(plan, list):
            plan = plan[0]
        if 'Plan' in plan:
            plan = plan['Plan']
        
        # Check for sequential scans on large tables
        if plan['Node Type'] == 'Seq Scan':
            if plan.get('Plan Rows', 0) > 1000:
                problems.append({
                    'severity': 'HIGH',
                    'description': f"Sequential scan on large table {plan.get('Relation Name', 'unknown')} with {plan.get('Plan Rows', 0):,} rows. Consider adding an index."
                })
        
        # Check for poor row estimates
        actual_rows = plan.get('Actual Rows', 0)
        plan_rows = plan.get('Plan Rows', 0)
        if plan_rows > 0 and actual_rows > 0:
            estimate_ratio = actual_rows / plan_rows
            if estimate_ratio > 10 or estimate_ratio < 0.1:
                problems.append({
                    'severity': 'MEDIUM',
                    'description': f"Poor row estimate in {plan['Node Type']}: expected {plan_rows:,} rows but got {actual_rows:,} rows. Consider running ANALYZE."
                })
        
        # Check for expensive sorts
        if plan['Node Type'] == 'Sort':
            if plan.get('Sort Method', '') == 'External Merge':
                problems.append({
                    'severity': 'HIGH',
                    'description': f"External merge sort detected. Query is using disk for sorting, which is much slower than in-memory sorts."
                })
            elif plan.get('Sort Space Used', 0) > 1000000:  # More than 1MB
                problems.append({
                    'severity': 'MEDIUM',
                    'description': f"Large in-memory sort using {plan.get('Sort Space Used', 0):,} bytes. Consider adding indexes to avoid sorting."
                })
        
        # Check for hash joins with large tables
        if plan['Node Type'] == 'Hash Join':
            hash_table_size = plan.get('Hash Table Size', 0)
            if hash_table_size > 1000000:  # More than 1MB
                problems.append({
                    'severity': 'MEDIUM',
                    'description': f"Large hash table size ({hash_table_size:,} bytes) in Hash Join. Consider using indexes for join conditions."
                })
        
        # Recursively check child nodes
        if 'Plans' in plan:
            for child in plan['Plans']:
                problems.extend(self.analyze_plan_problems({'Plan': child}))
        
        return problems
    
    def generate_html_report(self, metrics_data=None):
        """Generate an HTML report"""
        if metrics_data is None:
            metrics_data = self.analyze_queries()
            
        # Get problems and recommendations
        original_problems = self.analyze_plan_problems(metrics_data['original']['raw_plan'])
        optimized_problems = self.analyze_plan_problems(metrics_data['optimized']['raw_plan'])
        index_recommendations = self.get_index_recommendations(
            metrics_data['original']['raw_plan'],
            metrics_data['optimized']['raw_plan']
        )
        
        # Calculate differences
        differences = []
        
        # Compare planning time
        orig_planning = metrics_data['original'].get('planning_time', 0)
        opt_planning = metrics_data['optimized'].get('planning_time', 0)
        if orig_planning != opt_planning:
            differences.append({
                'type': 'planning_time',
                'original': orig_planning,
                'optimized': opt_planning,
                'change': self._format_change(orig_planning, opt_planning)
            })
        
        # Compare execution time
        orig_execution = metrics_data['original'].get('execution_time', 0)
        opt_execution = metrics_data['optimized'].get('execution_time', 0)
        if orig_execution != opt_execution:
            differences.append({
                'type': 'execution_time',
                'original': orig_execution,
                'optimized': opt_execution,
                'change': self._format_change(orig_execution, opt_execution)
            })
        
        # Compare total time
        orig_total = orig_planning + orig_execution
        opt_total = opt_planning + opt_execution
        if orig_total != opt_total:
            differences.append({
                'type': 'total_time',
                'original': orig_total,
                'optimized': opt_total,
                'change': self._format_change(orig_total, opt_total)
            })
        
        # Compare row counts
        orig_rows = metrics_data['original'].get('row_count', 0)
        opt_rows = metrics_data['optimized'].get('row_count', 0)
        if orig_rows != opt_rows:
            differences.append({
                'type': 'rows',
                'original': orig_rows,
                'optimized': opt_rows,
                'change': self._format_change(orig_rows, opt_rows)
            })
        
        # Generate sections
        exec_summary = self._generate_executive_summary_html(metrics_data, differences)
        perf_metrics = self._format_performance_metrics_html(metrics_data)
        query_stats = self._format_query_stats_html(metrics_data)
        plan_analysis = self._format_plan_analysis_html(metrics_data)
        problems = self._format_problems_section_html(original_problems, optimized_problems, index_recommendations)
        
        # Combine all sections
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='utf-8'>",
            "<title>PostgreSQL Query Performance Analysis Report</title>",
            "<style>",
            "body { font-family: monospace; line-height: 1.4; margin: 20px; }",
            "h1, h2, h3, h4 { color: #333; margin: 1em 0 0.5em 0; }",
            "h1 { border-bottom: 2px solid #333; padding-bottom: 0.2em; }",
            "h2 { border-bottom: 1px solid #666; }",
            "table { border-collapse: collapse; width: 100%; margin: 1em 0; }",
            "th, td { text-align: left; padding: 0.3em 1em; font-family: monospace; }",
            "th { border-bottom: 1px solid #666; }",
            ".metric-table td:first-child { width: 200px; }",
            ".metric-table td:nth-child(2), .metric-table td:nth-child(3) { text-align: right; width: 100px; }",
            ".metric-table td:nth-child(4) { text-align: left; padding-left: 2em; }",
            ".improvement { color: #28a745; }",
            ".warning { color: #dc3545; }",
            "div.improvement, div.warning { padding: 0.5em; margin: 0.5em 0; }",
            "div.improvement h3, div.warning h3 { color: inherit; margin: 0; }",
            "ul { list-style-type: none; padding-left: 0; margin: 0.5em 0; }",
            "li { margin: 0.2em 0; }",
            ".plan-table { font-family: monospace; }",
            ".plan-table td:first-child { white-space: pre; }",
            "pre { background-color: #f8f9fa; padding: 1em; border-radius: 4px; overflow-x: auto; }",
            "code { font-family: monospace; }",
            "</style>",
            "</head>",
            "<body>",
            "<h1>PostgreSQL Query Performance Analysis Report</h1>",
            f"<p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>",
            "<h2>Executive Summary</h2>",
            exec_summary,
            "<h2>Performance Metrics</h2>",
            perf_metrics,
            "<h2>Query Statistics</h2>",
            query_stats,
            "<h2>Execution Plan Analysis</h2>",
            plan_analysis,
            "<h2>Problems and Recommendations</h2>",
            problems,
            "</body>",
            "</html>"
        ]
        
        return '\n'.join(html)

    def _format_change(self, original, optimized):
        """Format the change between original and optimized values as a percentage"""
        if original == 0:
            if optimized == 0:
                return "0%"
            return "+∞%"
        change = ((optimized - original) / original) * 100
        if change > 0:
            return f"+{change:.1f}%"
        return f"{change:.1f}%"

    def _format_query_stats_html(self, metrics_data):
        """Format query statistics section of HTML report"""
        html = []
        
        # Format original query stats
        html.append("<h3>Original Query</h3>")
        html.append(self._format_plan_stats(metrics_data['original']['raw_plan']))
        
        # Format optimized query stats
        html.append("<h3>Optimized Query</h3>")
        html.append(self._format_plan_stats(metrics_data['optimized']['raw_plan']))
        
        return '\n'.join(html)

    def _format_plan_stats(self, plan):
        """Format statistics for a single query plan"""
        if not plan:
            return "<p>No plan data available</p>"
        
        # Collect node type stats
        node_types = {}
        self._collect_node_types(plan, node_types)
        
        # Format node type table
        html = ["<h4>Node Types</h4>"]
        html.append("<table class='metric-table'>")
        html.append("<tr><th>Node Type</th><th>Count</th><th>Total Cost</th><th>Avg. Rows</th></tr>")
        
        for node_type, stats in sorted(node_types.items()):
            avg_rows = stats['rows'] / stats['count'] if stats['count'] > 0 else 0
            html.append(
                f"<tr>"
                f"<td>{node_type}</td>"
                f"<td>{stats['count']}</td>"
                f"<td>{stats['cost']:.1f}</td>"
                f"<td>{avg_rows:.1f}</td>"
                f"</tr>"
            )
        
        html.append("</table>")
        
        # Collect table stats
        table_stats = {}
        self._collect_table_stats(plan, table_stats)
        
        # Format table stats
        if table_stats:
            html.append("<h4>Table Statistics</h4>")
            html.append("<table class='metric-table'>")
            html.append("<tr><th>Table</th><th>Scan Type</th><th>Rows</th><th>Width</th><th>Cost</th></tr>")
            
            for table, stats in sorted(table_stats.items()):
                html.append(
                    f"<tr>"
                    f"<td>{table}</td>"
                    f"<td>{stats['scan_type']}</td>"
                    f"<td>{stats['rows']}</td>"
                    f"<td>{stats['width']}</td>"
                    f"<td>{stats['cost']:.1f}</td>"
                    f"</tr>"
                )
            
            html.append("</table>")
        
        return '\n'.join(html)
    
    def _collect_node_types(self, node, stats):
        """Recursively collect statistics about node types in the plan"""
        if not node:
            return
        
        node_type = node.get('Node Type', '')
        if node_type not in stats:
            stats[node_type] = {
                'count': 0,
                'cost': 0,
                'rows': 0
            }
        stats[node_type]['count'] += 1
        stats[node_type]['cost'] += float(node.get('Total Cost', 0))
        stats[node_type]['rows'] += int(node.get('Plan Rows', 0))
        
        # Recurse into child plans
        if 'Plans' in node:
            for child in node['Plans']:
                self._collect_node_types(child, stats)
    
    def _collect_table_stats(self, node, stats):
        """Recursively collect statistics about tables in the plan"""
        if not node:
            return
        
        # Check if this is a scan node
        node_type = node.get('Node Type', '')
        if 'Scan' in node_type:
            table_name = node.get('Relation Name')
            stats[table_name] = {
                'scan_type': node_type,
                'rows': node.get('Plan Rows', 0),
                'width': node.get('Plan Width', 0),
                'cost': float(node.get('Total Cost', 0))
            }
        
        # Recursively check child nodes
        if 'Plans' in node:
            for child in node['Plans']:
                self._collect_table_stats(child, stats)
        
    def _format_plan_analysis_html(self, metrics_data):
        """Format plan analysis section of HTML report"""
        html = []
        
        # Format original plan
        html.append("<h3>Original Query Plan</h3>")
        if 'raw_plan' in metrics_data['original']:
            html.append("<pre>")
            html.append(json.dumps(metrics_data['original']['raw_plan'], indent=2))
            html.append("</pre>")
        else:
            html.append("<p>No plan data available</p>")
        
        # Format optimized plan
        html.append("<h3>Optimized Query Plan</h3>")
        if 'raw_plan' in metrics_data['optimized']:
            html.append("<pre>")
            html.append(json.dumps(metrics_data['optimized']['raw_plan'], indent=2))
            html.append("</pre>")
        else:
            html.append("<p>No plan data available</p>")
        
        return '\n'.join(html)
        
    def _format_performance_metrics_html(self, metrics_data):
        """Format performance metrics section of HTML report"""
        # Get timing metrics
        timing_metrics = [
            ('Planning Time', metrics_data['original'].get('planning_time', 0), metrics_data['optimized'].get('planning_time', 0)),
            ('Execution Time', metrics_data['original'].get('execution_time', 0), metrics_data['optimized'].get('execution_time', 0)),
            ('Total Time', 
             metrics_data['original'].get('planning_time', 0) + metrics_data['original'].get('execution_time', 0),
             metrics_data['optimized'].get('planning_time', 0) + metrics_data['optimized'].get('execution_time', 0))
        ]
        
        # Get I/O metrics
        io_metrics = [
            ('Shared Hit Blocks', metrics_data['original'].get('shared_hit_blocks', 0), metrics_data['optimized'].get('shared_hit_blocks', 0)),
            ('Shared Read Blocks', metrics_data['original'].get('shared_read_blocks', 0), metrics_data['optimized'].get('shared_read_blocks', 0)),
            ('Shared Dirtied Blocks', metrics_data['original'].get('shared_dirtied_blocks', 0), metrics_data['optimized'].get('shared_dirtied_blocks', 0)),
            ('Shared Written Blocks', metrics_data['original'].get('shared_written_blocks', 0), metrics_data['optimized'].get('shared_written_blocks', 0)),
            ('Local Hit Blocks', metrics_data['original'].get('local_hit_blocks', 0), metrics_data['optimized'].get('local_hit_blocks', 0)),
            ('Local Read Blocks', metrics_data['original'].get('local_read_blocks', 0), metrics_data['optimized'].get('local_read_blocks', 0)),
            ('Local Dirtied Blocks', metrics_data['original'].get('local_dirtied_blocks', 0), metrics_data['optimized'].get('local_dirtied_blocks', 0)),
            ('Local Written Blocks', metrics_data['original'].get('local_written_blocks', 0), metrics_data['optimized'].get('local_written_blocks', 0)),
            ('Temp Read Blocks', metrics_data['original'].get('temp_read_blocks', 0), metrics_data['optimized'].get('temp_read_blocks', 0)),
            ('Temp Written Blocks', metrics_data['original'].get('temp_written_blocks', 0), metrics_data['optimized'].get('temp_written_blocks', 0))
        ]
        
        # Get buffer metrics
        buffer_metrics = [
            ('Shared Hit Ratio', 
             self._calculate_hit_ratio(metrics_data['original'].get('shared_hit_blocks', 0), metrics_data['original'].get('shared_read_blocks', 0)),
             self._calculate_hit_ratio(metrics_data['optimized'].get('shared_hit_blocks', 0), metrics_data['optimized'].get('shared_read_blocks', 0))),
            ('Local Hit Ratio',
             self._calculate_hit_ratio(metrics_data['original'].get('local_hit_blocks', 0), metrics_data['original'].get('local_read_blocks', 0)),
             self._calculate_hit_ratio(metrics_data['optimized'].get('local_hit_blocks', 0), metrics_data['optimized'].get('local_read_blocks', 0)))
        ]
        
        # Format sections
        html = []
        
        # Timing metrics
        html.append("<h3>Timing Analysis</h3>")
        html.append(self._format_metrics_table(timing_metrics))
        
        # I/O metrics
        html.append("<h3>I/O Analysis</h3>")
        html.append(self._format_metrics_table(io_metrics))
        
        # Buffer metrics
        html.append("<h3>Buffer Efficiency</h3>")
        html.append(self._format_metrics_table(buffer_metrics))
        
        return '\n'.join(html)
    
    def _calculate_hit_ratio(self, hits, reads):
        """Calculate buffer hit ratio"""
        total = hits + reads
        if total == 0:
            return 0
        return (hits / total) * 100
    
    def _format_metrics_table(self, metrics):
        """Format a metrics table in HTML"""
        rows = []
        rows.append("<table class='metric-table'>")
        rows.append("<tr><th>Metric</th><th>Original</th><th>Optimized</th><th>Change</th></tr>")
        
        for name, orig, opt in metrics:
            change = self._format_change(orig, opt)
            change_class = 'improvement' if '-' in change else 'warning' if '+' in change else ''
            rows.append(
                f"<tr>"
                f"<td>{name}</td>"
                f"<td>{orig:.1f}</td>"
                f"<td>{opt:.1f}</td>"
                f"<td class='{change_class}'>{change}</td>"
                f"</tr>"
            )
        
        rows.append("</table>")
        return '\n'.join(rows)

    def write_report(self, report_path, content):
        """Write report content to file"""
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        
    def generate_report(self):
        """Generate both text and HTML reports comparing the queries"""
        print("Starting performance analysis...")
        metrics_data = self.analyze_queries()
        
        # Generate and write text report
        os.makedirs('reports', exist_ok=True)
        report_path = os.path.join(
            'reports',
            f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        )
        text_report = self.generate_text_report(metrics_data)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
            f.flush()
            os.fsync(f.fileno())
        
        # Generate and write HTML report
        html_report_path = os.path.join(
            'reports',
            f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
        )
        html_report = self.generate_html_report(metrics_data)
        with open(html_report_path, 'w', encoding='utf-8') as f:
            f.write(html_report)
            f.flush()
            os.fsync(f.fileno())
        
        print("\nReports generated:")
        print(f"- Text report: {report_path}")
        print(f"- HTML report: {html_report_path}")
        
        return report_path, html_report_path

    def analyze_index_recommendations(self, plan):
        """Analyze execution plan and provide index recommendations"""
        recommendations = []
        
        def analyze_node(node):
            if isinstance(node, list):
                node = node[0]
            if 'Plan' in node:
                node = node['Plan']
                
            node_type = node.get('Node Type', '')
            
            # Check for sequential scans
            if node_type == 'Seq Scan':
                relation = node.get('Relation Name')
                filter_cond = node.get('Filter', '')
                if relation and filter_cond:
                    columns = self._extract_columns_from_condition(filter_cond)
                    if columns:
                        recommendations.append({
                            'type': 'index',
                            'table': relation,
                            'columns': columns,
                            'reason': f"Sequential scan with filter: {filter_cond}"
                        })
            
            # Check for inefficient joins
            elif node_type in ['Hash Join', 'Nested Loop', 'Merge Join']:
                # Check hash conditions
                hash_cond = node.get('Hash Cond', '')
                if hash_cond:
                    tables_cols = self._extract_join_columns(hash_cond)
                    for table, cols in tables_cols.items():
                        recommendations.append({
                            'type': 'index',
                            'table': table,
                            'columns': cols,
                            'reason': f"Join condition: {hash_cond}"
                        })
                
                # Check merge conditions
                merge_cond = node.get('Merge Cond', '')
                if merge_cond:
                    tables_cols = self._extract_join_columns(merge_cond)
                    for table, cols in tables_cols.items():
                        recommendations.append({
                            'type': 'index',
                            'table': table,
                            'columns': cols,
                            'reason': f"Merge condition: {merge_cond}"
                        })
                
                # Check join filters
                join_filter = node.get('Join Filter', '')
                if join_filter:
                    tables_cols = self._extract_join_columns(join_filter)
                    for table, cols in tables_cols.items():
                        recommendations.append({
                            'type': 'index',
                            'table': table,
                            'columns': cols,
                            'reason': f"Join filter: {join_filter}"
                        })
            
            # Check for sorting operations
            elif node_type == 'Sort':
                sort_key = node.get('Sort Key', [])
                if sort_key:
                    # Try to determine the table from parent nodes
                    table = self._find_parent_relation(node)
                    if table:
                        columns = []
                        for key in sort_key:
                            if '.' in key:
                                col = key.split('.')[-1]
                            else:
                                col = key
                            columns.append(col)
                        if columns:
                            recommendations.append({
                                'type': 'index',
                                'table': table,
                                'columns': columns,
                                'reason': f"Sort operation on columns: {', '.join(sort_key)}"
                            })
            
            # Recursively check child nodes
            if 'Plans' in node:
                for child in node['Plans']:
                    analyze_node(child)
                    
        try:
            analyze_node(plan)
        except Exception as e:
            print(f"Warning: Error analyzing plan for index recommendations: {str(e)}")
        
        return recommendations
        
    def _find_parent_relation(self, node):
        """Try to find the relation name from parent nodes"""
        if isinstance(node, list):
            node = node[0]
        if 'Plan' in node:
            node = node['Plan']
            
        if 'Relation Name' in node:
            return node['Relation Name']
            
        if 'Plans' in node:
            for child in node['Plans']:
                result = self._find_parent_relation(child)
                if result:
                    return result
        return None

    def _extract_columns_from_condition(self, condition):
        """Extract column names from a filter condition"""
        if not condition:
            return []
            
        columns = []
        try:
            # Look for common patterns in conditions
            parts = condition.replace('(', ' ').replace(')', ' ').split(' AND ')
            for part in parts:
                part = part.strip()
                # Skip if empty
                if not part:
                    continue
                    
                # Extract column name from various condition types
                if any(op in part.upper() for op in [' = ', ' LIKE ', ' > ', ' < ', ' >= ', ' <= ', ' IN ']):
                    words = part.split()
                    if words:
                        col = words[0].strip()
                        if '.' in col:
                            # Handle schema.table.column format
                            col = col.split('.')[-1]  # Take the last part as column name
                        columns.append(col)
                        
                # Handle special functions like POSITION, COALESCE
                elif 'POSITION(' in part.upper():
                    # Extract column from POSITION(col IN ...)
                    try:
                        col_part = part[part.upper().index('POSITION(') + 9:]
                        col = col_part[:col_part.upper().index(' IN ')].strip()
                        if '.' in col:
                            col = col.split('.')[-1]
                        columns.append(col)
                    except (ValueError, IndexError):
                        pass
                        
                # Handle COALESCE
                elif 'COALESCE(' in part.upper():
                    try:
                        col_part = part[part.upper().index('COALESCE(') + 9:]
                        col = col_part.split(',')[0].strip()
                        if '.' in col:
                            col = col.split('.')[-1]
                        columns.append(col)
                    except (ValueError, IndexError):
                        pass
            
        except Exception as e:
            print(f"Warning: Could not parse condition: {condition}")
            return []
            
        return list(set(columns))  # Remove duplicates

    def _extract_join_columns(self, condition):
        """Extract table and column names from join conditions"""
        if not condition:
            return {}
            
        tables_columns = {}
        try:
            parts = condition.replace('(', '').replace(')', '').split(' = ')
            for part in parts:
                if '.' in part:
                    try:
                        table, col = part.strip().split('.')[-2:]  # Handle cases with schema.table.column
                        if table not in tables_columns:
                            tables_columns[table] = []
                        tables_columns[table].append(col)
                    except ValueError:
                        continue  # Skip if we can't parse this part
        except Exception as e:
            print(f"Warning: Could not parse join condition: {condition}")
            return {}
            
        return tables_columns
        
    def format_index_recommendations(self, recommendations):
        """Format index recommendations for text display"""
        if not recommendations:
            return "No index recommendations."
            
        lines = []
        seen_recommendations = set()  # To avoid duplicates
        
        for rec in recommendations:
            if rec['type'] == 'index':
                # Create a unique key for this recommendation
                key = f"{rec['table']}:{','.join(sorted(rec['columns']))}"
                if key not in seen_recommendations:
                    seen_recommendations.add(key)
                    lines.append(f"\n• Table: {rec['table']}")
                    lines.append(f"  Columns: {', '.join(rec['columns'])}")
                    lines.append(f"  Reason: {rec['reason']}")
                    lines.append(f"  Suggested Index:")
                    lines.append(f"    CREATE INDEX idx_{rec['table']}_{'_'.join(rec['columns'])} ")
                    lines.append(f"    ON {rec['table']} ({', '.join(rec['columns'])});")
        
        return '\n'.join(lines) if lines else "No index recommendations."

    def _analyze_node_timings(self, plan):
        """Analyze execution time for each node in the plan"""
        node_timings = []
        
        def analyze_node(node, depth=0):
            if isinstance(node, list):
                node = node[0]
            if 'Plan' in node:
                node = node['Plan']
                
            # Get node info
            node_type = node.get('Node Type', '')
            actual_time = node.get('Actual Total Time', 0)
            actual_loops = node.get('Actual Loops', 1)
            startup_time = node.get('Actual Startup Time', 0)
            workers_time = sum(w.get('Actual Total Time', 0) for w in node.get('Workers', []))
            
            # Calculate total time including workers
            total_time = actual_time * actual_loops + workers_time
            
            # Get CPU and I/O info
            cpu_info = {
                'CPU Cost': node.get('Total Cost', 0),
                'CPU Used (%)': node.get('Workers Planned', 0) * 100 if 'Workers Planned' in node else None,
                'Parallel Aware': node.get('Parallel Aware', False),
                'Workers Planned': node.get('Workers Planned', 0),
                'Workers Launched': node.get('Workers Launched', 0)
            }
            
            io_info = {
                'Shared Hit Blocks': node.get('Shared Hit Blocks', 0),
                'Shared Read Blocks': node.get('Shared Read Blocks', 0),
                'Shared Written Blocks': node.get('Shared Written Blocks', 0),
                'Temp Read Blocks': node.get('Temp Read Blocks', 0),
                'Temp Written Blocks': node.get('Temp Written Blocks', 0)
            }
            
            # Get index info if present
            index_info = {
                'Index Name': node.get('Index Name'),
                'Index Cond': node.get('Index Cond'),
                'Index Only Scan': node_type == 'Index Only Scan'
            } if 'Index Name' in node else None
            
            node_timings.append({
                'node_type': node_type,
                'depth': depth,
                'relation': node.get('Relation Name'),
                'startup_time': startup_time,
                'total_time': total_time,
                'actual_loops': actual_loops,
                'cpu_info': cpu_info,
                'io_info': io_info,
                'index_info': index_info,
                'filter': node.get('Filter'),
                'rows': node.get('Actual Rows', 0),
                'plan_rows': node.get('Plan Rows', 0),
                'workers': len(node.get('Workers', []))
            })
            
            # Recursively analyze child nodes
            if 'Plans' in node:
                for child in node['Plans']:
                    analyze_node(child, depth + 1)
                    
        analyze_node(plan)
        return node_timings
        
    def _analyze_node_type_stats(self, node_timings):
        """Analyze statistics per node type"""
        node_type_stats = {}
        total_time = sum(node['total_time'] for node in node_timings)
        
        # Group by node type
        for node in node_timings:
            node_type = node['node_type']
            if node_type not in node_type_stats:
                node_type_stats[node_type] = {
                    'count': 0,
                    'total_time': 0.0
                }
            node_type_stats[node_type]['count'] += 1
            node_type_stats[node_type]['total_time'] += node['total_time']
        
        # Calculate percentages and format
        stats = []
        for node_type, data in node_type_stats.items():
            percentage = (data['total_time'] / total_time * 100) if total_time > 0 else 0
            stats.append({
                'node_type': node_type,
                'count': data['count'],
                'total_time': data['total_time'],
                'percentage': percentage
            })
        
        return sorted(stats, key=lambda x: x['total_time'], reverse=True)
        
    def _analyze_table_stats(self, node_timings):
        """Analyze statistics per table"""
        table_stats = {}
        total_query_time = sum(node['total_time'] for node in node_timings)
        
        # Group by table and scan type
        for node in node_timings:
            relation = node.get('relation')
            if not relation:
                continue
                
            if relation not in table_stats:
                table_stats[relation] = {
                    'total_time': 0.0,
                    'scan_types': {}
                }
            
            node_type = node['node_type']
            if node_type not in table_stats[relation]['scan_types']:
                table_stats[relation]['scan_types'][node_type] = {
                    'count': 0,
                    'total_time': 0.0
                }
            
            table_stats[relation]['scan_types'][node_type]['count'] += 1
            table_stats[relation]['scan_types'][node_type]['total_time'] += node['total_time']
            table_stats[relation]['total_time'] += node['total_time']
        
        # Format the statistics
        stats = []
        for table, data in table_stats.items():
            table_percentage = (data['total_time'] / total_query_time * 100) if total_query_time > 0 else 0
            scan_stats = []
            
            for scan_type, scan_data in data['scan_types'].items():
                scan_percentage = (scan_data['total_time'] / data['total_time'] * 100) if data['total_time'] > 0 else 0
                scan_stats.append({
                    'scan_type': scan_type,
                    'count': scan_data['count'],
                    'total_time': scan_data['total_time'],
                    'percentage': scan_percentage
                })
            
            stats.append({
                'table': table,
                'total_time': data['total_time'],
                'percentage': table_percentage,
                'scan_stats': sorted(scan_stats, key=lambda x: x['total_time'], reverse=True)
            })
        
        return sorted(stats, key=lambda x: x['total_time'], reverse=True)
        
    def format_node_type_stats(self, stats):
        """Format node type statistics table"""
        if not stats:
            return "No node statistics available."
            
        lines = [
            "Node Type Statistics",
            "------------------",
            f"{'Node Type':<30} | {'Count':>5} | {'Total Time':>12} | {'% of Query':>10}"
        ]
        
        # Add separator line
        separator = "-" * 30 + "-+-" + "-" * 5 + "-+-" + "-" * 12 + "-+-" + "-" * 10
        lines.append(separator)
        
        # Add data rows
        for stat in stats:
            lines.append(
                f"{stat['node_type']:<30} | "
                f"{stat['count']:>5} | "
                f"{stat['total_time']:>9.3f} ms | "
                f"{stat['percentage']:>8.1f} %"
            )
        
        return "\n".join(lines)
        
    def format_table_stats(self, stats):
        """Format table statistics"""
        if not stats:
            return "No table statistics available."
            
        lines = []
        
        for table_stat in stats:
            # Table header
            lines.extend([
                f"\nTable: {table_stat['table']}",
                f"Total Time: {table_stat['total_time']:.3f} ms ({table_stat['percentage']:.1f}% of query)",
                "-" * 60,
                f"{'Scan Type':<30} | {'Count':>5} | {'Total Time':>12} | {'% of Table':>10}"
            ])
            
            # Add separator
            separator = "-" * 30 + "-+-" + "-" * 5 + "-+-" + "-" * 12 + "-+-" + "-" * 10
            lines.append(separator)
            
            # Add scan type rows
            for scan in table_stat['scan_stats']:
                lines.append(
                    f"{scan['scan_type']:<30} | "
                    f"{scan['count']:>5} | "
                    f"{scan['total_time']:>9.3f} ms | "
                    f"{scan['percentage']:>8.1f} %"
                )
        
        return "\n".join(lines)

    def format_node_timings(self, node_timings):
        """Format node timing analysis for the report"""
        lines = []
        
        # Sort nodes by total time
        sorted_nodes = sorted(node_timings, key=lambda x: x['total_time'], reverse=True)
        
        for node in sorted_nodes:
            indent = "  " * node['depth']
            node_info = [
                f"{indent}→ {node['node_type']}",
                f"{indent}  Time: {node['total_time']:.2f}ms "
                f"(startup: {node['startup_time']:.2f}ms, loops: {node['actual_loops']})"
            ]
            
            if node['relation']:
                node_info.append(f"{indent}  Table: {node['relation']}")
            
            if node['workers'] > 0:
                node_info.append(
                    f"{indent}  Workers: {node['workers']} "
                    f"(CPU: {node['cpu_info']['CPU Used (%)']}% utilized)"
                )
            
            if node['index_info'] and node['index_info']['Index Name']:
                node_info.append(
                    f"{indent}  Index: {node['index_info']['Index Name']}"
                    + (f" (condition: {node['index_info']['Index Cond']})" 
                       if node['index_info']['Index Cond'] else "")
                )
            
            if node['filter']:
                node_info.append(f"{indent}  Filter: {node['filter']}")
            
            if node['rows'] != node['plan_rows']:
                node_info.append(
                    f"{indent}  Rows: {node['rows']} "
                    f"(estimated: {node['plan_rows']}, "
                    f"error: {((node['rows'] - node['plan_rows']) / node['plan_rows'] * 100):.1f}%)"
                )
            
            # Add I/O info if significant
            io = node['io_info']
            if any(v > 0 for v in io.values()):
                io_lines = []
                if io['Shared Hit Blocks'] > 0 or io['Shared Read Blocks'] > 0:
                    total_shared = io['Shared Hit Blocks'] + io['Shared Read Blocks']
                    hit_ratio = (io['Shared Hit Blocks'] / total_shared * 100) if total_shared > 0 else 0
                    io_lines.append(f"buffer hit ratio: {hit_ratio:.1f}%")
                if io['Temp Written Blocks'] > 0:
                    io_lines.append(f"temp blocks: {io['Temp Written Blocks']}")
                if io_lines:
                    node_info.append(f"{indent}  I/O: {', '.join(io_lines)}")
            
            lines.extend(node_info)
            lines.append("")  # Empty line between nodes
        
        return "\n".join(lines)

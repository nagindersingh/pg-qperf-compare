"""
PostgreSQL Query Analyzer
Analyzes and compares query performance using EXPLAIN ANALYZE
"""

import os
import psycopg2
from datetime import datetime

class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.output_config = config.get('output', {'report_dir': './reports'})
        self.db_config = config['database']
        
    def connect_db(self):
        """Create database connection"""
        return psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            dbname=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

    def analyze_query(self, query_path):
        """Execute EXPLAIN ANALYZE and collect detailed metrics"""
        # Read query from file
        with open(query_path, 'r') as f:
            query = f.read()

        conn = self.connect_db()
        cursor = conn.cursor()
        
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
        plan = cursor.fetchall()[0][0]  # Get the JSON plan
        
        # Execute actual query to get row count
        cursor.execute(query)
        row_count = len(cursor.fetchall())
        
        cursor.close()
        conn.close()
        
        # Extract key metrics from plan
        planning_time = plan[0]['Planning Time']
        execution_time = plan[0]['Execution Time']
        
        # Extract node-specific metrics
        nodes_info = self.extract_node_metrics(plan[0]['Plan'])
        
        return {
            'planning_time': planning_time,
            'execution_time': execution_time,
            'row_count': row_count,
            'nodes': nodes_info,
            'raw_plan': plan[0]  # Keep raw plan for detailed analysis
        }
    
    def extract_node_metrics(self, node):
        """Recursively extract metrics from plan nodes"""
        metrics = {
            'node_type': node['Node Type'],
            'actual_rows': node.get('Actual Rows', 0),
            'actual_time': node.get('Actual Total Time', 0),
            'actual_loops': node.get('Actual Loops', 1),
            'planned_rows': node.get('Plan Rows', 0),
            'shared_hit_blocks': node.get('Shared Hit Blocks', 0),
            'shared_read_blocks': node.get('Shared Read Blocks', 0),
            'shared_dirtied_blocks': node.get('Shared Dirtied Blocks', 0),
            'shared_written_blocks': node.get('Shared Written Blocks', 0),
            'temp_read_blocks': node.get('Temp Read Blocks', 0),
            'temp_written_blocks': node.get('Temp Written Blocks', 0),
            'io_read_time': node.get('I/O Read Time', 0),
            'io_write_time': node.get('I/O Write Time', 0),
            'filter': node.get('Filter', ''),
            'rows_removed_by_filter': node.get('Rows Removed by Filter', 0),
            'scan_direction': node.get('Scan Direction', ''),
            'index_name': node.get('Index Name', ''),
            'index_cond': node.get('Index Cond', ''),
            'join_type': node.get('Join Type', ''),
            'hash_condition': node.get('Hash Cond', ''),
            'relation_name': node.get('Relation Name', '')
        }
        
        # Process child nodes
        children = []
        if 'Plans' in node:
            for child in node['Plans']:
                children.append(self.extract_node_metrics(child))
        
        metrics['children'] = children
        return metrics

    def analyze_node_problems(self, node, parent_rows=None):
        """Analyze potential problems in a plan node"""
        problems = []
        
        # Helper function to add problems with severity
        def add_problem(description, severity):
            problems.append({
                'description': description,
                'severity': severity,  # 'high', 'medium', 'low'
                'node_type': node['node_type']
            })

        # Check row estimate accuracy
        if node['planned_rows'] > 0:
            estimate_ratio = node['actual_rows'] / node['planned_rows']
            if estimate_ratio > 10:
                add_problem(
                    f"Row estimation is off by {estimate_ratio:.1f}x (expected {node['planned_rows']}, got {node['actual_rows']}). "
                    "Consider updating table statistics with ANALYZE.",
                    'high'
                )
            elif estimate_ratio > 3:
                add_problem(
                    f"Row estimation is off by {estimate_ratio:.1f}x. May need table statistics update.",
                    'medium'
                )

        # Check for sequential scans on large results
        if node['node_type'] == 'Seq Scan' and node['actual_rows'] > 10000:
            add_problem(
                f"Sequential scan retrieving {node['actual_rows']} rows. Consider adding an index.",
                'high' if node['actual_rows'] > 100000 else 'medium'
            )

        # Check for inefficient joins
        if node['node_type'] == 'Nested Loop' and node['actual_rows'] > 10000:
            add_problem(
                f"Nested loop join with {node['actual_rows']} rows. Consider using Hash Join or Merge Join.",
                'high'
            )

        # Check for spilled hash joins
        if node['node_type'] == 'Hash Join' and node['temp_written_blocks'] > 0:
            add_problem(
                f"Hash join spilled {self.format_blocks(node['temp_written_blocks'])} to disk. Consider increasing work_mem.",
                'high'
            )

        # Check for filter efficiency
        if node['rows_removed_by_filter'] > 0:
            removal_ratio = node['rows_removed_by_filter'] / (node['actual_rows'] + node['rows_removed_by_filter'])
            if removal_ratio > 0.8:
                add_problem(
                    f"Filter removed {removal_ratio:.1%} of rows. Consider adding an index or modifying WHERE clause.",
                    'high' if removal_ratio > 0.9 else 'medium'
                )

        # Check for multiple node executions
        if node['actual_loops'] > 100:
            add_problem(
                f"Node executed {node['actual_loops']} times. Consider restructuring the query.",
                'medium'
            )

        # Check I/O time ratio
        total_time = node['actual_time']
        io_time = node['io_read_time'] + node['io_write_time']
        if total_time > 1000 and io_time/total_time > 0.5:  # More than 50% time spent on I/O
            add_problem(
                f"High I/O time ({io_time/total_time:.1%} of total time). Consider adding indexes or increasing cache.",
                'high'
            )

        # Recursive analysis of child nodes
        for child in node['children']:
            child_problems = self.analyze_node_problems(child, node['actual_rows'])
            problems.extend(child_problems)

        return problems

    def analyze_queries(self):
        """Run and analyze both queries"""
        print("Analyzing original query...")
        original_metrics = self.analyze_query(self.config['queries']['original'])
        
        print("Analyzing optimized query...")
        optimized_metrics = self.analyze_query(self.config['queries']['optimized'])
        
        # Analyze problems in both queries
        original_problems = self.analyze_node_problems(original_metrics['nodes'])
        optimized_problems = self.analyze_node_problems(optimized_metrics['nodes'])
        
        return {
            'original': {**original_metrics, 'problems': original_problems},
            'optimized': {**optimized_metrics, 'problems': optimized_problems}
        }

    def calculate_improvement(self, original, optimized):
        """Calculate improvement percentage with safety checks"""
        if original <= 0:
            return 0.0
        return ((original - optimized) / original * 100)

    def format_time(self, ms):
        """Format time in milliseconds to appropriate unit"""
        if ms < 1:
            return f"{ms * 1000:.2f} µs"
        elif ms < 1000:
            return f"{ms:.2f} ms"
        else:
            return f"{ms / 1000:.2f} s"

    def format_blocks(self, blocks):
        """Format blocks to KB/MB/GB"""
        kb = blocks * 8  # Assuming 8KB block size
        if kb < 1024:
            return f"{kb:.2f} KB"
        elif kb < 1024 * 1024:
            return f"{kb / 1024:.2f} MB"
        else:
            return f"{kb / 1024 / 1024:.2f} GB"

    def generate_node_report(self, node, level=0):
        """Generate text report for a plan node"""
        indent = "  " * level
        lines = [
            f"{indent}Node: {node['node_type']}",
            f"{indent}  Rows: {node['actual_rows']} (planned: {node['planned_rows']})",
            f"{indent}  Loops: {node['actual_loops']}",
            f"{indent}  Time: {self.format_time(node['actual_time'])}"
        ]
        
        # Add node-specific details
        if node['relation_name']:
            lines.append(f"{indent}  Relation: {node['relation_name']}")
        if node['index_name']:
            lines.append(f"{indent}  Index: {node['index_name']}")
        if node['index_cond']:
            lines.append(f"{indent}  Index Cond: {node['index_cond']}")
        if node['filter']:
            lines.append(f"{indent}  Filter: {node['filter']}")
            if node['rows_removed_by_filter']:
                lines.append(f"{indent}  Rows Removed by Filter: {node['rows_removed_by_filter']}")
        
        # Add I/O statistics
        lines.extend([
            f"{indent}  Shared Hit Blocks: {self.format_blocks(node['shared_hit_blocks'])}",
            f"{indent}  Shared Read Blocks: {self.format_blocks(node['shared_read_blocks'])}",
            f"{indent}  Temp Read Blocks: {self.format_blocks(node['temp_read_blocks'])}",
            f"{indent}  Temp Written Blocks: {self.format_blocks(node['temp_written_blocks'])}"
        ])
        
        # Process child nodes
        for child in node['children']:
            lines.extend(self.generate_node_report(child, level + 1))
        
        return lines

    def generate_problem_report(self, problems):
        """Generate formatted report of query problems"""
        if not problems:
            return ["No significant problems detected."]
        
        lines = []
        for severity in ['high', 'medium', 'low']:
            severity_problems = [p for p in problems if p['severity'] == severity]
            if severity_problems:
                lines.append(f"\n{severity.upper()} Severity Issues:")
                for problem in severity_problems:
                    lines.append(f"- [{problem['node_type']}] {problem['description']}")
        return lines

    def generate_report(self, metrics_data=None):
        """Generate text report with performance metrics"""
        if metrics_data is None:
            print("Collecting performance metrics...")
            metrics_data = self.analyze_queries()
        
        # Create reports directory if it doesn't exist
        os.makedirs(self.output_config['report_dir'], exist_ok=True)
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = os.path.join(
            self.output_config['report_dir'],
            f'query_analysis_{timestamp}.txt'
        )
        
        # Calculate improvements
        planning_improvement = self.calculate_improvement(
            metrics_data['original']['planning_time'],
            metrics_data['optimized']['planning_time']
        )
        execution_improvement = self.calculate_improvement(
            metrics_data['original']['execution_time'],
            metrics_data['optimized']['execution_time']
        )
        
        # Create report content
        report_content = [
            "PostgreSQL Query Performance Analysis",
            "=" * 35,
            f"Generated at: {timestamp}",
            "",
            "Overall Metrics",
            "-" * 15,
            "",
            "1. Planning Time",
            f"   Original:    {self.format_time(metrics_data['original']['planning_time'])}",
            f"   Optimized:   {self.format_time(metrics_data['optimized']['planning_time'])}",
            f"   Improvement: {planning_improvement:+.2f}%" if planning_improvement else "   Improvement: N/A",
            "",
            "2. Execution Time",
            f"   Original:    {self.format_time(metrics_data['original']['execution_time'])}",
            f"   Optimized:   {self.format_time(metrics_data['optimized']['execution_time'])}",
            f"   Improvement: {execution_improvement:+.2f}%" if execution_improvement else "   Improvement: N/A",
            "",
            "Results Verification",
            "-" * 20,
            f"Original Rows:  {metrics_data['original']['row_count']}",
            f"Optimized Rows: {metrics_data['optimized']['row_count']}",
            f"Status:         {'MATCH' if metrics_data['original']['row_count'] == metrics_data['optimized']['row_count'] else 'MISMATCH'}",
            "",
            "Original Query Analysis",
            "-" * 22
        ]
        
        # Add original query problems
        report_content.extend(self.generate_problem_report(metrics_data['original']['problems']))
        
        report_content.extend([
            "",
            "Original Query Plan",
            "-" * 18
        ])
        
        # Add original query plan details
        report_content.extend(self.generate_node_report(metrics_data['original']['nodes']))
        
        report_content.extend([
            "",
            "Optimized Query Analysis",
            "-" * 23
        ])
        
        # Add optimized query problems
        report_content.extend(self.generate_problem_report(metrics_data['optimized']['problems']))
        
        report_content.extend([
            "",
            "Optimized Query Plan",
            "-" * 19
        ])
        
        # Add optimized query plan details
        report_content.extend(self.generate_node_report(metrics_data['optimized']['nodes']))
        
        report_content.extend([
            "",
            "Overall Performance",
            "-" * 20,
            f"Total Time Improvement: {(planning_improvement + execution_improvement) / 2:+.2f}%",
            f"Status: {'IMPROVED' if execution_improvement > 0 else 'NO CHANGE' if execution_improvement == 0 else 'DEGRADED'}"
        ])
        
        # Write report to file
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_content))
        
        return report_path

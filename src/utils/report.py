"""
Report generation utilities for query performance analysis.
"""
from typing import Dict, Any, List
from datetime import datetime
from pathlib import Path
import json

from ..core.metrics import PlanMetrics
from ..core.models import Problem, IndexRecommendation

class ReportGenerator:
    @staticmethod
    def generate_html_report(
        metrics_data: Dict[str, Any],
        original_problems: List[Problem],
        optimized_problems: List[Problem],
        index_recommendations: List[IndexRecommendation]
    ) -> str:
        """Generate HTML report from analysis results."""
        html = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
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
            ReportGenerator._generate_executive_summary_html(metrics_data),
            "<h2>Performance Metrics</h2>",
            ReportGenerator._format_performance_metrics_html(metrics_data),
            "<h2>Query Statistics</h2>",
            ReportGenerator._format_query_stats_html(metrics_data),
            "<h2>Execution Plan Analysis</h2>",
            ReportGenerator._format_plan_analysis_html(metrics_data),
            "<h2>Problems and Recommendations</h2>",
            ReportGenerator._format_problems_section_html(
                original_problems, 
                optimized_problems, 
                index_recommendations
            ),
            "<h2>Raw Plans</h2>",
            "<h3>Original Query Plan</h3>",
            "<pre>",
            json.dumps(metrics_data['original']['raw_plan'], indent=2),
            "</pre>",
            "<h3>Optimized Query Plan</h3>",
            "<pre>",
            json.dumps(metrics_data['optimized']['raw_plan'], indent=2),
            "</pre>",
            "</body>",
            "</html>"
        ]
        
        return '\n'.join(html)

    @staticmethod
    def generate_text_report(
        metrics_data: Dict[str, Any],
        original_problems: List[Problem],
        optimized_problems: List[Problem],
        index_recommendations: List[IndexRecommendation]
    ) -> str:
        """Generate text report from analysis results."""
        lines = [
            "PostgreSQL Query Performance Analysis Report",
            "========================================",
            f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "Executive Summary",
            "--------------------"
        ]

        # Add executive summary
        exec_improvement = metrics_data['improvements']['execution_time']
        plan_improvement = metrics_data['improvements']['planning_time']
        
        if exec_improvement > 0:
            lines.append(f"✅ Execution time improved by {exec_improvement:.1f}%")
        else:
            lines.append(f"⚠️ Execution time regressed by {abs(exec_improvement):.1f}%")
        
        if plan_improvement > 0:
            lines.append(f"✅ Planning time improved by {plan_improvement:.1f}%")
        else:
            lines.append(f"⚠️ Planning time regressed by {abs(plan_improvement):.1f}%")
        
        # Add performance metrics
        lines.extend([
            "",
            "Performance Metrics",
            "--------------------",
            "Planning Time:",
            f"  Original:  {metrics_data['original']['metrics'].planning_time:.2f} ms",
            f"  Optimized: {metrics_data['optimized']['metrics'].planning_time:.2f} ms",
            f"  Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].planning_time, metrics_data['optimized']['metrics'].planning_time)}",
            "",
            "Execution Time:",
            f"  Original:  {metrics_data['original']['metrics'].execution_time:.2f} ms",
            f"  Optimized: {metrics_data['optimized']['metrics'].execution_time:.2f} ms",
            f"  Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].execution_time, metrics_data['optimized']['metrics'].execution_time)}",
        ])

        # Add buffer statistics
        lines.extend([
            "",
            "Buffer Statistics:",
            "  Buffers Hit:",
            f"    Original:  {metrics_data['original']['metrics'].buffer_stats['buffers_hit']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].buffer_stats['buffers_hit']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].buffer_stats['buffers_hit'], metrics_data['optimized']['metrics'].buffer_stats['buffers_hit'])}",
            "",
            "  Buffers Read:",
            f"    Original:  {metrics_data['original']['metrics'].buffer_stats['buffers_read']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].buffer_stats['buffers_read']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].buffer_stats['buffers_read'], metrics_data['optimized']['metrics'].buffer_stats['buffers_read'])}",
            "",
            "  Buffers Dirtied:",
            f"    Original:  {metrics_data['original']['metrics'].buffer_stats['buffers_dirtied']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].buffer_stats['buffers_dirtied']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].buffer_stats['buffers_dirtied'], metrics_data['optimized']['metrics'].buffer_stats['buffers_dirtied'])}",
            "",
            "  Buffers Written:",
            f"    Original:  {metrics_data['original']['metrics'].buffer_stats['buffers_written']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].buffer_stats['buffers_written']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].buffer_stats['buffers_written'], metrics_data['optimized']['metrics'].buffer_stats['buffers_written'])}",
            "",
            "  Buffer Hit Rate:",
            f"    Original:  {metrics_data['original']['metrics'].buffer_stats['buffers_hit_rate']:.1f}%",
            f"    Optimized: {metrics_data['optimized']['metrics'].buffer_stats['buffers_hit_rate']:.1f}%",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].buffer_stats['buffers_hit_rate'], metrics_data['optimized']['metrics'].buffer_stats['buffers_hit_rate'])}",
        ])

        # Add I/O statistics
        lines.extend([
            "",
            "I/O Statistics:",
            "  Shared Hit Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['shared_hit_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['shared_hit_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['shared_hit_blocks'], metrics_data['optimized']['metrics'].io_metrics['shared_hit_blocks'])}",
            "",
            "  Shared Read Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['shared_read_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['shared_read_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['shared_read_blocks'], metrics_data['optimized']['metrics'].io_metrics['shared_read_blocks'])}",
            "",
            "  Shared Dirtied Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['shared_dirtied_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['shared_dirtied_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['shared_dirtied_blocks'], metrics_data['optimized']['metrics'].io_metrics['shared_dirtied_blocks'])}",
            "",
            "  Shared Written Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['shared_written_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['shared_written_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['shared_written_blocks'], metrics_data['optimized']['metrics'].io_metrics['shared_written_blocks'])}",
            "",
            "  Temp Read Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['temp_read_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['temp_read_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['temp_read_blocks'], metrics_data['optimized']['metrics'].io_metrics['temp_read_blocks'])}",
            "",
            "  Temp Written Blocks:",
            f"    Original:  {metrics_data['original']['metrics'].io_metrics['temp_written_blocks']}",
            f"    Optimized: {metrics_data['optimized']['metrics'].io_metrics['temp_written_blocks']}",
            f"    Change:    {ReportGenerator._format_change_text(metrics_data['original']['metrics'].io_metrics['temp_written_blocks'], metrics_data['optimized']['metrics'].io_metrics['temp_written_blocks'])}",
        ])

        # Add query statistics
        lines.extend([
            "",
            "Query Statistics",
            "--------------------",
            "Original Query:",
            "",
            "Node Types:",
            *ReportGenerator._format_node_type_stats_text(metrics_data['original']['metrics'].node_type_stats),
            "",
            "Table Statistics:",
            *ReportGenerator._format_table_stats_text(metrics_data['original']['metrics'].table_stats),
            "",
            "Optimized Query:",
            "",
            "Node Types:",
            *ReportGenerator._format_node_type_stats_text(metrics_data['optimized']['metrics'].node_type_stats),
            "",
            "Table Statistics:",
            *ReportGenerator._format_table_stats_text(metrics_data['optimized']['metrics'].table_stats),
        ])

        # Add problems and recommendations
        if original_problems:
            lines.extend([
                "",
                "Problems in Original Query:",
                *[f"- {p.description}" for p in original_problems]
            ])

        if optimized_problems:
            lines.extend([
                "",
                "Problems in Optimized Query:",
                *[f"- {p.description}" for p in optimized_problems]
            ])

        if index_recommendations:
            lines.extend([
                "",
                "Index Recommendations:",
                *[f"- {r.description}" for r in index_recommendations]
            ])

        # Add raw plans at the end
        lines.extend([
            "",
            "Raw Plans",
            "--------------------",
            "",
            "Original Query Plan:",
            json.dumps(metrics_data['original']['raw_plan'], indent=2),
            "",
            "Optimized Query Plan:",
            json.dumps(metrics_data['optimized']['raw_plan'], indent=2)
        ])

        return "\n".join(lines)

    @staticmethod
    def _generate_executive_summary_html(metrics_data: Dict[str, Any]) -> str:
        """Generate HTML executive summary."""
        improvements = []
        warnings = []
        
        # Check execution time improvement
        exec_improvement = metrics_data['improvements']['execution_time']
        if exec_improvement > 0:
            improvements.append(
                f"Execution time improved by {exec_improvement:.1f}%"
            )
        elif exec_improvement < 0:
            warnings.append(
                f"Execution time regressed by {abs(exec_improvement):.1f}%"
            )
        
        # Check planning time changes
        plan_improvement = metrics_data['improvements']['planning_time']
        if abs(plan_improvement) > 10:  # Only mention if significant
            if plan_improvement > 0:
                improvements.append(
                    f"Planning time improved by {plan_improvement:.1f}%"
                )
            else:
                warnings.append(
                    f"Planning time increased by {abs(plan_improvement):.1f}%"
                )
        
        html = []
        
        if improvements:
            html.append('<div class="improvement">')
            html.append('<h3>✅ Improvements</h3>')
            html.append('<ul>')
            for improvement in improvements:
                html.append(f'<li>{improvement}</li>')
            html.append('</ul>')
            html.append('</div>')
        
        if warnings:
            html.append('<div class="warning">')
            html.append('<h3>⚠️ Warnings</h3>')
            html.append('<ul>')
            for warning in warnings:
                html.append(f'<li>{warning}</li>')
            html.append('</ul>')
            html.append('</div>')
        
        if not improvements and not warnings:
            html.append('<p>No significant performance changes detected.</p>')
        
        return '\n'.join(html)

    @staticmethod
    def _format_performance_metrics_html(metrics_data: Dict[str, Any]) -> str:
        """Format performance metrics section of HTML report."""
        orig_metrics = metrics_data['original']['metrics']
        opt_metrics = metrics_data['optimized']['metrics']
        
        rows = []
        
        # Planning Time
        rows.append([
            'Planning Time',
            f"{orig_metrics.planning_time:.2f} ms",
            f"{opt_metrics.planning_time:.2f} ms",
            ReportGenerator._format_change(orig_metrics.planning_time, opt_metrics.planning_time)
        ])
        
        # Execution Time
        rows.append([
            'Execution Time',
            f"{orig_metrics.execution_time:.2f} ms",
            f"{opt_metrics.execution_time:.2f} ms",
            ReportGenerator._format_change(orig_metrics.execution_time, opt_metrics.execution_time)
        ])
        
        # Total Time
        rows.append([
            'Total Time',
            f"{orig_metrics.total_time:.2f} ms",
            f"{opt_metrics.total_time:.2f} ms",
            ReportGenerator._format_change(orig_metrics.total_time, opt_metrics.total_time)
        ])
        
        # Rows
        rows.append([
            'Rows',
            str(orig_metrics.row_count),
            str(opt_metrics.row_count),
            ReportGenerator._format_change(orig_metrics.row_count, opt_metrics.row_count)
        ])
        
        # Buffer Statistics
        rows.append(['', '', '', ''])  # Empty row as separator
        rows.append(['Buffer Statistics', '', '', ''])
        
        buffer_metrics = [
            ('Buffers Hit', 'buffers_hit'),
            ('Buffers Read', 'buffers_read'),
            ('Buffers Dirtied', 'buffers_dirtied'),
            ('Buffers Written', 'buffers_written'),
            ('Buffer Hit Rate', 'buffers_hit_rate')
        ]
        
        for label, key in buffer_metrics:
            orig_val = orig_metrics.buffer_stats[key]
            opt_val = opt_metrics.buffer_stats[key]
            
            if key == 'buffers_hit_rate':
                rows.append([
                    label,
                    f"{orig_val:.1f}%",
                    f"{opt_val:.1f}%",
                    ReportGenerator._format_change(orig_val, opt_val)
                ])
            else:
                rows.append([
                    label,
                    str(orig_val),
                    str(opt_val),
                    ReportGenerator._format_change(orig_val, opt_val)
                ])
        
        # I/O Statistics
        rows.append(['', '', '', ''])  # Empty row as separator
        rows.append(['I/O Statistics', '', '', ''])
        
        io_metrics = [
            ('Shared Hit Blocks', 'shared_hit_blocks'),
            ('Shared Read Blocks', 'shared_read_blocks'),
            ('Shared Dirtied Blocks', 'shared_dirtied_blocks'),
            ('Shared Written Blocks', 'shared_written_blocks'),
            ('Temp Read Blocks', 'temp_read_blocks'),
            ('Temp Written Blocks', 'temp_written_blocks')
        ]
        
        for label, key in io_metrics:
            orig_val = orig_metrics.io_metrics[key]
            opt_val = opt_metrics.io_metrics[key]
            rows.append([
                label,
                str(orig_val),
                str(opt_val),
                ReportGenerator._format_change(orig_val, opt_val)
            ])
        
        html = ['<table class="metric-table">']
        html.append('<tr><th>Metric</th><th>Original</th><th>Optimized</th><th>Change</th></tr>')
        
        for row in rows:
            html.append('<tr>')
            if row[0] == '':  # Empty separator row
                html.append('<td colspan="4"><hr></td>')
            elif row[0] in ['Buffer Statistics', 'I/O Statistics']:  # Header row
                html.append(f'<td colspan="4"><strong>{row[0]}</strong></td>')
            else:
                for cell in row:
                    html.append(f'<td>{cell}</td>')
            html.append('</tr>')
        
        html.append('</table>')
        return '\n'.join(html)

    @staticmethod
    def _format_query_stats_html(metrics_data: Dict[str, Any]) -> str:
        """Format query statistics section of HTML report"""
        html = []
        
        # Original Query Stats
        html.append("<h3>Original Query</h3>")
        if 'raw_plan' in metrics_data['original']:
            # Node Type Statistics
            html.append("<h4>Node Types</h4>")
            node_stats = metrics_data['original']['metrics'].node_type_stats
            html.append(ReportGenerator._format_node_type_stats_html(node_stats))
            
            # Table Statistics
            html.append("<h4>Table Statistics</h4>")
            table_stats = metrics_data['original']['metrics'].table_stats
            html.append(ReportGenerator._format_table_stats_html(table_stats))
        else:
            html.append("<p>No plan data available</p>")
        
        # Optimized Query Stats
        html.append("<h3>Optimized Query</h3>")
        if 'raw_plan' in metrics_data['optimized']:
            # Node Type Statistics
            html.append("<h4>Node Types</h4>")
            node_stats = metrics_data['optimized']['metrics'].node_type_stats
            html.append(ReportGenerator._format_node_type_stats_html(node_stats))
            
            # Table Statistics
            html.append("<h4>Table Statistics</h4>")
            table_stats = metrics_data['optimized']['metrics'].table_stats
            html.append(ReportGenerator._format_table_stats_html(table_stats))
        else:
            html.append("<p>No plan data available</p>")
        
        return '\n'.join(html)

    @staticmethod
    def _format_node_type_stats_html(stats: Dict[str, Any]) -> str:
        """Format node type statistics as HTML table."""
        html = ["<table class='metric-table'>"]
        html.append("<tr><th>Node Type</th><th>Count</th><th>Total Cost</th><th>Avg. Rows</th></tr>")
        
        for node_type, node_stats in sorted(stats.items()):
            avg_rows = node_stats['total_rows'] / node_stats['count'] if node_stats['count'] > 0 else 0
            total_cost = sum(node_stats.get('costs', [0]))  # Sum up all costs for this node type
            html.append(
                f"<tr>"
                f"<td>{node_type}</td>"
                f"<td>{node_stats['count']}</td>"
                f"<td>{total_cost:.1f}</td>"
                f"<td>{avg_rows:.1f}</td>"
                f"</tr>"
            )
        
        html.append("</table>")
        return '\n'.join(html)

    @staticmethod
    def _format_table_stats_html(stats: Dict[str, Any]) -> str:
        """Format table statistics as HTML table."""
        html = ["<table class='metric-table'>"]
        html.append("<tr><th>Table</th><th>Scan Type</th><th>Rows</th><th>Width</th><th>Cost</th></tr>")
        
        for table, table_stats in sorted(stats.items()):
            html.append(
                f"<tr>"
                f"<td>{table}</td>"
                f"<td>{table_stats['scan_type']}</td>"
                f"<td>{table_stats.get('rows', 0)}</td>"
                f"<td>{table_stats.get('width', 0)}</td>"
                f"<td>{table_stats.get('cost', 0):.1f}</td>"
                f"</tr>"
            )
        
        html.append("</table>")
        return '\n'.join(html)

    @staticmethod
    def _format_plan_analysis_html(metrics_data: Dict[str, Any]) -> str:
        """Format plan analysis section of HTML report."""
        html = []
        
        # Compare node types between original and optimized
        orig_types = set(metrics_data['original']['metrics'].node_type_stats.keys())
        opt_types = set(metrics_data['optimized']['metrics'].node_type_stats.keys())
        
        added = opt_types - orig_types
        removed = orig_types - opt_types
        
        if added:
            html.append("<h3>New Node Types</h3>")
            html.append("<ul>")
            for node_type in sorted(added):
                html.append(f"<li>Added: {node_type}</li>")
            html.append("</ul>")
        
        if removed:
            html.append("<h3>Removed Node Types</h3>")
            html.append("<ul>")
            for node_type in sorted(removed):
                html.append(f"<li>Removed: {node_type}</li>")
            html.append("</ul>")
        
        if not added and not removed:
            html.append("<p>No changes in plan node types.</p>")
        
        return '\n'.join(html)

    @staticmethod
    def _format_problems_section_html(
        original_problems: List[Problem],
        optimized_problems: List[Problem],
        index_recommendations: List[IndexRecommendation]
    ) -> str:
        """Format problems section as HTML."""
        html = []
        
        # Original Query Issues
        html.append("<h3>Original Query Issues</h3>")
        if not original_problems:
            html.append("<p>No issues found.</p>")
        else:
            # High Severity Issues
            high_severity = [p for p in original_problems if p.severity == 'HIGH']
            if high_severity:
                html.append("<h4>HIGH Severity Issues</h4>")
                html.append("<ul>")
                for problem in high_severity:
                    html.append(f"<li>⚠️ {problem.description}</li>")
                html.append("</ul>")
            
            # Medium Severity Issues
            medium_severity = [p for p in original_problems if p.severity == 'MEDIUM']
            if medium_severity:
                html.append("<h4>MEDIUM Severity Issues</h4>")
                html.append("<ul>")
                for problem in medium_severity:
                    html.append(f"<li>⚠️ {problem.description}</li>")
                html.append("</ul>")
        
        # Optimized Query Issues
        html.append("<h3>Optimized Query Issues</h3>")
        if not optimized_problems:
            html.append("<p>No issues found.</p>")
        else:
            # High Severity Issues
            high_severity = [p for p in optimized_problems if p.severity == 'HIGH']
            if high_severity:
                html.append("<h4>HIGH Severity Issues</h4>")
                html.append("<ul>")
                for problem in high_severity:
                    html.append(f"<li>⚠️ {problem.description}</li>")
                html.append("</ul>")
            
            # Medium Severity Issues
            medium_severity = [p for p in optimized_problems if p.severity == 'MEDIUM']
            if medium_severity:
                html.append("<h4>MEDIUM Severity Issues</h4>")
                html.append("<ul>")
                for problem in medium_severity:
                    html.append(f"<li>⚠️ {problem.description}</li>")
                html.append("</ul>")
        
        # Index Recommendations
        html.append("<h3>Index Recommendations</h3>")
        if not index_recommendations:
            html.append("<p>No index recommendations.</p>")
        else:
            html.append("<ul>")
            for rec in index_recommendations:
                html.append(
                    f"<li>💡 Consider creating an index on {rec.table}({', '.join(rec.columns)})"
                    f"<br>Reason: {rec.reason}</li>"
                )
            html.append("</ul>")
        
        return '\n'.join(html)

    @staticmethod
    def _format_node_type_stats_text(stats: Dict[str, Any]) -> List[str]:
        """Format node type statistics as text."""
        lines = [
            "Node Type               Count   Total Cost    Avg. Rows",
            "------------------------------------------------------"
        ]
        
        for node_type, node_stats in sorted(stats.items()):
            avg_rows = node_stats['total_rows'] / node_stats['count'] if node_stats['count'] > 0 else 0
            total_cost = node_stats['total_cost']  # Use the total_cost from node_stats
            lines.append(
                f"{node_type:<20} {node_stats['count']:>7} {total_cost:>11.1f} {avg_rows:>10.1f}"
            )
        
        return lines

    @staticmethod
    def _format_table_stats_text(stats: Dict[str, Any]) -> List[str]:
        """Format table statistics as text."""
        lines = [
            "Table                Scan Type        Rows    Width     Cost",
            "----------------------------------------------------------"
        ]
        
        for table, table_stats in sorted(stats.items()):
            lines.append(
                f"{table:<20} {table_stats['scan_type']:<15} {table_stats['rows']:>7} {table_stats['width']:>8} {table_stats['cost']:>8.1f}"
            )
        
        return lines

    @staticmethod
    def _format_change_text(original: float, optimized: float) -> str:
        """Format the change between original and optimized values."""
        if original == 0 and optimized == 0:
            return "N/A"
        elif original == 0:
            return "+∞%"
        
        change = ((optimized - original) / original) * 100
        if change > 0:
            return f"-{change:.1f}% (worse)"  # Increase is worse for time/blocks
        elif change < 0:
            return f"+{abs(change):.1f}% (better)"  # Show positive number for improvements
        else:
            return "No change"

    @staticmethod
    def _format_change(original: float, optimized: float) -> str:
        """Format the change between original and optimized values."""
        if original == 0 and optimized == 0:
            return "N/A"
        elif original == 0:
            return '<span class="warning">+∞%</span>'
        
        change = ((optimized - original) / original) * 100
        if change > 0:
            return f'<span class="warning">-{change:.1f}%</span>'  # Increase is worse for time/blocks
        elif change < 0:
            return f'<span class="improvement">+{abs(change):.1f}%</span>'  # Show positive number for improvements
        else:
            return "No change"

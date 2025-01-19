"""
Data models for query analysis.
"""
from typing import List
from dataclasses import dataclass

@dataclass
class Problem:
    """Represents a problem identified in a query plan."""
    description: str
    severity: str = 'warning'

@dataclass
class IndexRecommendation:
    """Represents a recommended index for query optimization."""
    table: str
    columns: List[str]
    reason: str
    
    @property
    def description(self) -> str:
        """Get a human-readable description of the recommendation."""
        return f"Create index on {self.table}({', '.join(self.columns)}) - {self.reason}"
    
    @property
    def sql(self) -> str:
        """Get the SQL command to create the index."""
        idx_name = f"idx_{self.table}_{'_'.join(self.columns)}"
        return f"CREATE INDEX {idx_name} ON {self.table} ({', '.join(self.columns)});"

"""
Configuration loading utilities for pg-qperf-compare.
"""
from typing import Dict, Any
from pathlib import Path
import yaml
from dataclasses import dataclass

from ..core.database import DatabaseConfig

@dataclass
class AppConfig:
    """Application configuration."""
    database: DatabaseConfig
    original_query: Path
    optimized_query: Path

class ConfigLoader:
    """Loads and validates configuration from YAML files."""
    
    @staticmethod
    def load_config(config_path: Path) -> AppConfig:
        """Load configuration from YAML file."""
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        
        # Validate required fields
        required_fields = ['database', 'original_query', 'optimized_query']
        for field in required_fields:
            if field not in config_data:
                raise ValueError(f"Missing required field in config: {field}")
        
        # Load database config
        db_config = DatabaseConfig(
            host=config_data['database'].get('host', 'localhost'),
            port=config_data['database'].get('port', 5432),
            dbname=config_data['database']['dbname'],
            user=config_data['database']['user'],
            password=config_data['database'].get('password', '')
        )
        
        # Convert query paths to Path objects
        original_query = Path(config_data['original_query'])
        optimized_query = Path(config_data['optimized_query'])
        
        # Validate query files exist
        if not original_query.exists():
            raise FileNotFoundError(f"Original query file not found: {original_query}")
        if not optimized_query.exists():
            raise FileNotFoundError(f"Optimized query file not found: {optimized_query}")
        
        return AppConfig(
            database=db_config,
            original_query=original_query,
            optimized_query=optimized_query
        )

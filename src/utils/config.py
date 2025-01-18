"""
Configuration management for pg-qperf-compare.
Handles loading and validation of YAML config files.
"""
from typing import Dict, Any
from pathlib import Path
import yaml
from dataclasses import dataclass

from ..core.database import DatabaseConfig

@dataclass
class AppConfig:
    database: DatabaseConfig
    queries: Dict[str, Path]
    output_dir: Path

class ConfigLoader:
    @staticmethod
    def load(config_path: Path) -> AppConfig:
        """Load and validate config from YAML file."""
        with open(config_path) as f:
            config_data = yaml.safe_load(f)
        
        db_config = DatabaseConfig(
            host=config_data['database']['host'],
            port=config_data['database']['port'],
            dbname=config_data['database']['dbname'],
            user=config_data['database']['user'],
            password=config_data['database']['password']
        )
        
        queries = {
            k: Path(v) for k, v in config_data['queries'].items()
        }
        
        output_dir = Path(config_data.get('output', {}).get('report_dir', './reports'))
        
        return AppConfig(
            database=db_config,
            queries=queries,
            output_dir=output_dir
        )

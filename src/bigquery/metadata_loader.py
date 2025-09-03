"""BigQuery metadata loader for schema validation and descriptions."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from .client import BigQueryClient

logger = logging.getLogger(__name__)


@dataclass
class ColumnMetadata:
    """Metadata for a BigQuery column."""
    table_name: str
    column_name: str
    data_type: str
    description: Optional[str] = None
    field_path: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata for a BigQuery table."""
    table_name: str
    columns: Dict[str, ColumnMetadata]
    row_count: Optional[int] = None


class BigQueryMetadataLoader:
    """Loads and caches BigQuery metadata for validation and descriptions."""
    
    def __init__(self, client: BigQueryClient, dataset: str, cache_dir: Optional[str] = None):
        """Initialize metadata loader."""
        self.client = client
        self.dataset = dataset
        self.cache_dir = Path(cache_dir) if cache_dir else None
        self.metadata_cache: Dict[str, TableMetadata] = {}
        
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def load_metadata_for_tables(self, table_names: Set[str], use_cache: bool = True) -> Dict[str, TableMetadata]:
        """Load metadata for specified tables."""
        logger.info(f"Loading metadata for {len(table_names)} tables")
        
        result = {}
        tables_to_load = set()
        
        # Check cache first
        if use_cache:
            for table_name in table_names:
                cached_metadata = self._load_from_cache(table_name)
                if cached_metadata:
                    result[table_name] = cached_metadata
                    self.metadata_cache[table_name] = cached_metadata
                else:
                    tables_to_load.add(table_name)
        else:
            tables_to_load = table_names
        
        # Load missing tables from BigQuery
        if tables_to_load:
            fresh_metadata = self._load_metadata_from_bigquery(tables_to_load)
            result.update(fresh_metadata)
            
            # Cache the results
            if use_cache:
                for table_name, metadata in fresh_metadata.items():
                    self._save_to_cache(table_name, metadata)
        
        logger.info(f"Loaded metadata for {len(result)} tables")
        return result
    
    def _load_metadata_from_bigquery(self, table_names: Set[str]) -> Dict[str, TableMetadata]:
        """Load metadata directly from BigQuery."""
        if not table_names:
            return {}
        
        # Build table list for query
        table_list = "', '".join(table_names)
        
        # Query for column metadata
        columns_sql = f"""
        SELECT 
            table_name,
            column_name,
            data_type
        FROM `{self.dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name IN ('{table_list}')
        ORDER BY table_name, ordinal_position
        """
        
        # Query for column descriptions (may not exist for all columns)
        descriptions_sql = f"""
        SELECT 
            table_name,
            column_name,
            field_path,
            data_type,
            description
        FROM `{self.dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
        WHERE table_name IN ('{table_list}')
        AND description IS NOT NULL
        """
        
        # Query for table row counts (optional)
        tables_sql = f"""
        SELECT 
            table_name
        FROM `{self.dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name IN ('{table_list}')
        AND table_type = 'BASE_TABLE'
        """
        
        metadata_dict = {}
        
        try:
            # Load basic column info
            logger.info("Querying INFORMATION_SCHEMA.COLUMNS")
            columns_job = self.client.query(columns_sql)
            
            for row in columns_job:
                table_name = row['table_name']
                if table_name not in metadata_dict:
                    metadata_dict[table_name] = TableMetadata(
                        table_name=table_name,
                        columns={}
                    )
                
                column_metadata = ColumnMetadata(
                    table_name=table_name,
                    column_name=row['column_name'],
                    data_type=row['data_type']
                )
                metadata_dict[table_name].columns[row['column_name']] = column_metadata
            
            # Load column descriptions
            logger.info("Querying INFORMATION_SCHEMA.COLUMN_FIELD_PATHS")
            descriptions_job = self.client.query(descriptions_sql)
            
            for row in descriptions_job:
                table_name = row['table_name']
                column_name = row['column_name']
                
                if (table_name in metadata_dict and 
                    column_name in metadata_dict[table_name].columns):
                    
                    metadata_dict[table_name].columns[column_name].description = row['description']
                    metadata_dict[table_name].columns[column_name].field_path = row['field_path']
            
            # Load table row counts
            logger.info("Querying INFORMATION_SCHEMA.TABLES")
            tables_job = self.client.query(tables_sql)
            
            for row in tables_job:
                table_name = row['table_name']
                if table_name in metadata_dict:
                    # Row count not available for public datasets
                    metadata_dict[table_name].row_count = None
            
        except Exception as e:
            logger.error(f"Failed to load BigQuery metadata: {e}")
            raise
        
        return metadata_dict
    
    def _load_from_cache(self, table_name: str) -> Optional[TableMetadata]:
        """Load metadata from cache file."""
        if not self.cache_dir:
            return None
        
        cache_file = self.cache_dir / f"{table_name}_metadata.json"
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
            
            columns = {}
            for col_name, col_data in data['columns'].items():
                columns[col_name] = ColumnMetadata(
                    table_name=col_data['table_name'],
                    column_name=col_data['column_name'],
                    data_type=col_data['data_type'],
                    description=col_data.get('description'),
                    field_path=col_data.get('field_path')
                )
            
            return TableMetadata(
                table_name=data['table_name'],
                columns=columns,
                row_count=data.get('row_count')
            )
        
        except Exception as e:
            logger.warning(f"Failed to load cache for {table_name}: {e}")
            return None
    
    def _save_to_cache(self, table_name: str, metadata: TableMetadata) -> None:
        """Save metadata to cache file."""
        if not self.cache_dir:
            return
        
        cache_file = self.cache_dir / f"{table_name}_metadata.json"
        
        try:
            data = {
                'table_name': metadata.table_name,
                'row_count': metadata.row_count,
                'columns': {}
            }
            
            for col_name, col_metadata in metadata.columns.items():
                data['columns'][col_name] = {
                    'table_name': col_metadata.table_name,
                    'column_name': col_metadata.column_name,
                    'data_type': col_metadata.data_type,
                    'description': col_metadata.description,
                    'field_path': col_metadata.field_path
                }
            
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
            
        except Exception as e:
            logger.warning(f"Failed to save cache for {table_name}: {e}")
    
    def validate_tables_exist(self, table_names: Set[str]) -> Tuple[Set[str], Set[str]]:
        """Validate which tables exist in BigQuery."""
        if not table_names:
            return set(), set()
        
        table_list = "', '".join(table_names)
        sql = f"""
        SELECT table_name
        FROM `{self.dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name IN ('{table_list}')
        """
        
        try:
            job = self.client.query(sql)
            existing_tables = {row['table_name'] for row in job}
            missing_tables = table_names - existing_tables
            
            if missing_tables:
                logger.warning(f"Missing tables in BigQuery: {missing_tables}")
            
            return existing_tables, missing_tables
        
        except Exception as e:
            logger.error(f"Failed to validate table existence: {e}")
            return set(), table_names
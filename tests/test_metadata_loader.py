"""Tests for BigQuery metadata loader."""

import pytest
from unittest.mock import Mock, MagicMock

from src.bigquery.metadata_loader import BigQueryMetadataLoader, ColumnMetadata, TableMetadata


class TestBigQueryMetadataLoader:
    """Test BigQuery metadata loader functionality."""
    
    def test_metadata_structure(self):
        """Test metadata data structures."""
        column = ColumnMetadata(
            table_name="users",
            column_name="id",
            data_type="INT64",
            description="User identifier"
        )
        
        assert column.table_name == "users"
        assert column.column_name == "id"
        assert column.data_type == "INT64"
        assert column.description == "User identifier"
    
    def test_table_metadata(self):
        """Test table metadata structure."""
        columns = {
            "id": ColumnMetadata("users", "id", "INT64", "User ID"),
            "name": ColumnMetadata("users", "name", "STRING", "User name")
        }
        
        table = TableMetadata("users", columns, row_count=1000)
        
        assert table.table_name == "users"
        assert len(table.columns) == 2
        assert table.row_count == 1000
        assert "id" in table.columns
        assert "name" in table.columns
    
    def test_validate_tables_exist(self):
        """Test table existence validation."""
        # Mock BigQuery client
        mock_client = Mock()
        mock_job = Mock()
        mock_job.__iter__ = Mock(return_value=iter([
            {"table_name": "users"},
            {"table_name": "orders"}
        ]))
        mock_client.query.return_value = mock_job
        
        loader = BigQueryMetadataLoader(
            client=mock_client,
            dataset="test_dataset"
        )
        
        table_names = {"users", "orders", "products"}
        existing, missing = loader.validate_tables_exist(table_names)
        
        assert existing == {"users", "orders"}
        assert missing == {"products"}
        
        # Verify SQL query was called
        mock_client.query.assert_called_once()
        call_args = mock_client.query.call_args[0][0]
        assert "INFORMATION_SCHEMA.TABLES" in call_args
        assert "users" in call_args
        assert "orders" in call_args
        assert "products" in call_args
    
    def test_cache_key_generation(self):
        """Test cache functionality."""
        mock_client = Mock()
        
        loader = BigQueryMetadataLoader(
            client=mock_client,
            dataset="test_dataset",
            cache_dir=None  # No file cache for this test
        )
        
        # Test that empty table set returns empty result
        result = loader.load_metadata_for_tables(set())
        assert result == {}
        
        # Verify no BigQuery calls were made
        mock_client.query.assert_not_called()
    
    def test_load_metadata_structure(self):
        """Test metadata loading structure."""
        mock_client = Mock()
        
        # Mock the three queries: columns, descriptions, tables
        columns_result = [
            {"table_name": "users", "column_name": "id", "data_type": "INT64"},
            {"table_name": "users", "column_name": "name", "data_type": "STRING"}
        ]
        
        descriptions_result = [
            {"table_name": "users", "column_name": "id", "field_path": "id", 
             "data_type": "INT64", "description": "User identifier"}
        ]
        
        tables_result = [
            {"table_name": "users", "row_count": 1000}
        ]
        
        # Set up mock to return different results for each query
        mock_client.query.side_effect = [
            iter(columns_result),
            iter(descriptions_result), 
            iter(tables_result)
        ]
        
        loader = BigQueryMetadataLoader(
            client=mock_client,
            dataset="test_dataset"
        )
        
        result = loader._load_metadata_from_bigquery({"users"})
        
        assert "users" in result
        table_metadata = result["users"]
        assert table_metadata.table_name == "users"
        assert table_metadata.row_count == 1000
        assert len(table_metadata.columns) == 2
        
        # Check column metadata
        assert "id" in table_metadata.columns
        id_column = table_metadata.columns["id"]
        assert id_column.data_type == "INT64"
        assert id_column.description == "User identifier"
        
        assert "name" in table_metadata.columns
        name_column = table_metadata.columns["name"]
        assert name_column.data_type == "STRING"
        assert name_column.description is None  # No description provided
"""Tests for grounding index functionality."""

import pytest
from unittest.mock import Mock

from src.grounding.index import GroundingIndex, FieldInfo
from src.grounding.field_mapper import FieldMapper
from src.lookml.models import LookMLProject, LookMLView, LookMLDimension, LookMLMeasure
from src.bigquery.metadata_loader import BigQueryMetadataLoader, TableMetadata, ColumnMetadata


class TestFieldMapper:
    """Test field mapper functionality."""
    
    def test_resolve_table_references(self):
        """Test ${TABLE} reference resolution."""
        mapper = FieldMapper()
        
        # Test simple table reference
        expression = "${TABLE}.id"
        result = mapper.resolve_lookml_expression(expression, "users", "users", {})
        assert result == "users.id"
        
        # Test with table alias
        mapper.set_table_alias("users", "u")
        result = mapper.resolve_lookml_expression(expression, "users", "users", {})
        assert result == "u.id"
    
    def test_extract_referenced_fields(self):
        """Test field reference extraction."""
        mapper = FieldMapper()
        
        expression = "${users.id} = ${orders.user_id}"
        references = mapper.extract_referenced_fields(expression)
        
        assert references == {"users.id", "orders.user_id"}
    
    def test_is_simple_column_reference(self):
        """Test simple column reference detection."""
        mapper = FieldMapper()
        
        assert mapper.is_simple_column_reference("${TABLE}.id")
        assert mapper.is_simple_column_reference("${TABLE}.user_name")
        assert not mapper.is_simple_column_reference("CASE WHEN ${TABLE}.id > 0 THEN 1 ELSE 0 END")
        assert not mapper.is_simple_column_reference("COUNT(${TABLE}.id)")
    
    def test_extract_column_name(self):
        """Test column name extraction."""
        mapper = FieldMapper()
        
        assert mapper.extract_column_name("${TABLE}.id") == "id"
        assert mapper.extract_column_name("${TABLE}.user_name") == "user_name"
        assert mapper.extract_column_name("COUNT(${TABLE}.id)") is None


class TestGroundingIndex:
    """Test grounding index functionality."""
    
    def create_test_project(self):
        """Create a test LookML project."""
        # Create dimensions
        id_dim = LookMLDimension(
            name="id",
            type="number",
            sql="${TABLE}.id",
            description="User ID",
            primary_key=True
        )
        
        name_dim = LookMLDimension(
            name="name", 
            type="string",
            sql="${TABLE}.name",
            description="User name"
        )
        
        # Create measures
        count_measure = LookMLMeasure(
            name="count",
            type="count",
            sql="${TABLE}.id",
            description="Count of users"
        )
        
        # Create view
        users_view = LookMLView(
            name="users",
            sql_table_name="public.users",
            dimensions={"id": id_dim, "name": name_dim},
            measures={"count": count_measure}
        )
        
        # Create project
        project = LookMLProject(views={"users": users_view})
        return project
    
    def create_test_metadata(self):
        """Create test BigQuery metadata."""
        columns = {
            "id": ColumnMetadata("users", "id", "INT64", "User identifier"),
            "name": ColumnMetadata("users", "name", "STRING", "User full name")
        }
        
        table_metadata = {
            "users": TableMetadata("users", columns, row_count=1000)
        }
        
        return table_metadata
    
    def test_field_info_creation(self):
        """Test field info data structure."""
        field = FieldInfo(
            name="id",
            field_type="dimension",
            lookml_type="number",
            sql_expression="${TABLE}.id",
            lookml_description="User ID",
            bigquery_description="User identifier",
            bigquery_data_type="INT64",
            view_name="users",
            table_name="public.users"
        )
        
        assert field.qualified_name == "users.id"
        assert "User ID" in field.combined_description
        assert "User identifier" in field.combined_description
    
    def test_grounding_index_basic(self):
        """Test basic grounding index functionality."""
        project = self.create_test_project()
        
        # Mock metadata loader
        mock_loader = Mock(spec=BigQueryMetadataLoader)
        mock_loader.load_metadata_for_tables.return_value = self.create_test_metadata()
        
        # Create grounding index
        index = GroundingIndex(
            lookml_project=project,
            metadata_loader=mock_loader
        )
        
        # Test field glossary was built
        assert len(index.field_glossary) > 0
        assert "count" in index.field_glossary
        assert "user" in index.field_glossary  # From descriptions
    
    def test_find_relevant_fields(self):
        """Test finding relevant fields by query terms."""
        project = self.create_test_project()
        
        # Mock metadata loader
        mock_loader = Mock(spec=BigQueryMetadataLoader)
        mock_loader.load_metadata_for_tables.return_value = self.create_test_metadata()
        
        # Mock explores (since our test project doesn't have any)
        index = GroundingIndex(
            lookml_project=project,
            metadata_loader=mock_loader
        )
        
        # Manually add an explore for testing
        from src.grounding.index import ExploreInfo
        
        available_fields = {}
        for view in project.views.values():
            for field_name, dimension in view.dimensions.items():
                field_info = FieldInfo(
                    name=field_name,
                    field_type="dimension",
                    lookml_type=dimension.type,
                    sql_expression=dimension.sql,
                    lookml_description=dimension.description,
                    bigquery_description=None,
                    bigquery_data_type=None,
                    view_name=view.name,
                    table_name=view.sql_table_name
                )
                available_fields[field_info.qualified_name] = field_info
        
        explore_info = ExploreInfo(
            name="users_explore",
            base_view="users",
            available_fields=available_fields,
            join_graph={},
            join_conditions={}
        )
        
        index.explores["users_explore"] = explore_info
        
        # Test field search
        relevant_fields = index.find_relevant_fields("users_explore", ["id", "user"])
        
        assert len(relevant_fields) > 0
        field_names = [field.name for field, score in relevant_fields]
        assert "id" in field_names
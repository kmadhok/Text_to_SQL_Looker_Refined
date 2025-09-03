"""Tests for SQL generation components."""

import pytest
from unittest.mock import Mock

from src.generator.planner import QueryPlanner, QueryPlan
from src.generator.sql_builder import SQLBuilder
from src.grounding.index import GroundingIndex, ExploreInfo, FieldInfo


class TestQueryPlanner:
    """Test query planner functionality."""
    
    def create_mock_grounding_index(self):
        """Create a mock grounding index for testing."""
        mock_index = Mock(spec=GroundingIndex)
        
        # Mock field info
        id_field = FieldInfo(
            name="id",
            field_type="dimension",
            lookml_type="number",
            sql_expression="${TABLE}.id",
            lookml_description="User ID",
            bigquery_description=None,
            bigquery_data_type="INT64",
            view_name="users",
            table_name="public.users"
        )
        
        name_field = FieldInfo(
            name="name",
            field_type="dimension", 
            lookml_type="string",
            sql_expression="${TABLE}.name",
            lookml_description="User name",
            bigquery_description=None,
            bigquery_data_type="STRING",
            view_name="users",
            table_name="public.users"
        )
        
        count_field = FieldInfo(
            name="count",
            field_type="measure",
            lookml_type="count",
            sql_expression="COUNT(${TABLE}.id)",
            lookml_description="Count of users",
            bigquery_description=None,
            bigquery_data_type=None,
            view_name="users",
            table_name="public.users"
        )
        
        # Mock explore info
        explore_info = ExploreInfo(
            name="users_explore",
            base_view="users",
            available_fields={
                "users.id": id_field,
                "users.name": name_field,
                "users.count": count_field
            },
            join_graph={},
            join_conditions={}
        )
        
        mock_index.explores = {"users_explore": explore_info}
        mock_index.find_relevant_explores.return_value = [("users_explore", 5.0)]
        mock_index.find_relevant_fields.return_value = [
            (id_field, 3.0),
            (count_field, 2.0)
        ]
        mock_index.get_explore_by_name.return_value = explore_info
        
        return mock_index
    
    def test_extract_query_terms(self):
        """Test query term extraction."""
        mock_index = self.create_mock_grounding_index()
        planner = QueryPlanner(mock_index)
        
        query = "Show me the count of users by name"
        terms = planner._extract_query_terms(query)
        
        # Should extract meaningful terms, filtering out stop words
        expected_terms = ["count", "users", "name"]
        for term in expected_terms:
            assert term in terms
        
        # Should not include stop words
        assert "the" not in terms
        assert "me" not in terms
    
    def test_extract_limit(self):
        """Test limit extraction from queries."""
        mock_index = self.create_mock_grounding_index()
        planner = QueryPlanner(mock_index)
        
        # Test various limit formats
        assert planner._extract_limit("show top 10 users") == 10
        assert planner._extract_limit("limit 25") == 25
        assert planner._extract_limit("first 50 records") == 50
        assert planner._extract_limit("show users") is None
    
    def test_plan_query_basic(self):
        """Test basic query planning."""
        mock_index = self.create_mock_grounding_index()
        planner = QueryPlanner(mock_index)
        
        query = "show count of users"
        plan = planner.plan_query(query)
        
        assert plan is not None
        assert plan.explore_name == "users_explore"
        assert len(plan.selected_fields) > 0
        assert plan.limit == 100  # Default limit
        
        # Should have selected count measure
        field_names = [f.name for f in plan.selected_fields]
        assert "count" in field_names


class TestSQLBuilder:
    """Test SQL builder functionality."""
    
    def create_test_plan(self):
        """Create a test query plan."""
        id_field = FieldInfo(
            name="id",
            field_type="dimension",
            lookml_type="number",
            sql_expression="${TABLE}.id",
            lookml_description="User ID",
            bigquery_description=None,
            bigquery_data_type="INT64",
            view_name="users",
            table_name="public.users"
        )
        
        count_field = FieldInfo(
            name="count",
            field_type="measure",
            lookml_type="count",
            sql_expression="COUNT(${TABLE}.id)",
            lookml_description="Count of users",
            bigquery_description=None,
            bigquery_data_type=None,
            view_name="users",
            table_name="public.users"
        )
        
        return QueryPlan(
            explore_name="users_explore",
            selected_fields=[id_field, count_field],
            required_joins=set(),
            filters=[],
            limit=100,
            has_aggregation=True
        )
    
    def test_enforce_limit(self):
        """Test limit enforcement in SQL."""
        mock_index = Mock(spec=GroundingIndex)
        builder = SQLBuilder(mock_index)
        
        # Test adding limit to SQL without limit
        sql_without_limit = "SELECT id FROM users"
        result = builder.enforce_limit(sql_without_limit, 100)
        assert "LIMIT 100" in result
        
        # Test not modifying SQL that already has limit
        sql_with_limit = "SELECT id FROM users LIMIT 50"
        result = builder.enforce_limit(sql_with_limit, 100)
        assert result == sql_with_limit
    
    def test_clean_table_name(self):
        """Test table name cleaning and formatting."""
        mock_index = Mock(spec=GroundingIndex)
        builder = SQLBuilder(mock_index)
        
        # Test simple table name
        assert builder._clean_table_name("users") == "users"
        
        # Test quoted table name
        assert builder._clean_table_name("`users`") == "users"
        
        # Test fully qualified table name
        result = builder._clean_table_name("project.dataset.users")
        assert result == "`project.dataset.users`"
    
    def test_get_table_alias(self):
        """Test table alias generation."""
        mock_index = Mock(spec=GroundingIndex)
        builder = SQLBuilder(mock_index)
        
        assert builder._get_table_alias("users") == "users"
        assert builder._get_table_alias("user-profiles") == "user_profiles"
    
    def test_convert_join_type(self):
        """Test LookML join type conversion."""
        mock_index = Mock(spec=GroundingIndex)
        builder = SQLBuilder(mock_index)
        
        assert builder._convert_join_type("left_outer") == "LEFT"
        assert builder._convert_join_type("right_outer") == "RIGHT"
        assert builder._convert_join_type("inner") == "INNER"
        assert builder._convert_join_type("full_outer") == "FULL OUTER"
        assert builder._convert_join_type("unknown") == "LEFT"  # Default fallback
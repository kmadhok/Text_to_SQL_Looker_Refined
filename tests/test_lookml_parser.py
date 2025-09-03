"""Tests for LookML parser."""

import pytest
import tempfile
from pathlib import Path

from src.lookml.parser import LookMLParser
from src.lookml.models import LookMLDimension, LookMLMeasure, LookMLView


class TestLookMLParser:
    """Test LookML parser functionality."""
    
    def test_parse_simple_view(self):
        """Test parsing a simple view file."""
        view_content = '''
view: users {
  sql_table_name: public.users ;;
  
  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
    description: "User ID"
  }
  
  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "User name"
  }
  
  measure: count {
    type: count
    sql: ${TABLE}.id ;;
    description: "Count of users"
  }
}
'''
        
        with tempfile.TemporaryDirectory() as temp_dir:
            view_file = Path(temp_dir) / "users.view.lkml"
            view_file.write_text(view_content)
            
            parser = LookMLParser(temp_dir)
            project = parser.parse_project()
            
            assert "users" in project.views
            view = project.views["users"]
            
            assert view.name == "users"
            assert view.sql_table_name == "public.users"
            assert len(view.dimensions) == 2
            assert len(view.measures) == 1
            
            # Check ID dimension
            id_dim = view.dimensions["id"]
            assert id_dim.type == "number"
            assert id_dim.primary_key is True
            assert id_dim.sql == "${TABLE}.id"
            assert id_dim.description == "User ID"
            
            # Check name dimension
            name_dim = view.dimensions["name"]
            assert name_dim.type == "string"
            assert name_dim.sql == "${TABLE}.name"
            
            # Check count measure
            count_measure = view.measures["count"]
            assert count_measure.type == "count"
            assert count_measure.sql == "${TABLE}.id"
    
    def test_parse_model_with_explore(self):
        """Test parsing a model file with explores."""
        model_content = '''
connection: "bigquery"

include: "*.view.lkml"

explore: orders {
  join: users {
    type: left_outer
    sql_on: ${orders.user_id} = ${users.id} ;;
    relationship: many_to_one
  }
  
  join: order_items {
    type: left_outer
    sql_on: ${orders.id} = ${order_items.order_id} ;;
    relationship: one_to_many
  }
}
'''
        
        with tempfile.TemporaryDirectory() as temp_dir:
            model_file = Path(temp_dir) / "ecommerce.model.lkml"
            model_file.write_text(model_content)
            
            parser = LookMLParser(temp_dir)
            project = parser.parse_project()
            
            assert "ecommerce" in project.models
            model = project.models["ecommerce"]
            
            assert model.connection == "bigquery"
            assert len(model.explores) == 1
            
            explore = model.explores["orders"]
            assert explore.name == "orders"
            assert explore.from_view == "orders"
            assert len(explore.joins) == 2
            
            # Check joins
            users_join = next(j for j in explore.joins if j.view_name == "users")
            assert users_join.type == "left_outer"
            assert users_join.sql_on == "${orders.user_id} = ${users.id}"
            assert users_join.relationship == "many_to_one"
    
    def test_empty_directory(self):
        """Test parsing empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            parser = LookMLParser(temp_dir)
            project = parser.parse_project()
            
            assert len(project.models) == 0
            assert len(project.views) == 0
    
    def test_nonexistent_directory(self):
        """Test error handling for nonexistent directory."""
        with pytest.raises(ValueError, match="does not exist"):
            LookMLParser("/nonexistent/path")
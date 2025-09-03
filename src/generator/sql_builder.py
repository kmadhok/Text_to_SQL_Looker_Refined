"""SQL builder for generating BigQuery SQL from query plans."""

import logging
import re
from typing import Dict, List, Optional, Set

from ..grounding.index import GroundingIndex, ExploreInfo, FieldInfo
from ..grounding.field_mapper import FieldMapper
from .planner import QueryPlan

logger = logging.getLogger(__name__)


class SQLBuilder:
    """Builds BigQuery SQL from query plans."""
    
    def __init__(self, grounding_index: GroundingIndex, field_mapper: Optional[FieldMapper] = None):
        """Initialize SQL builder."""
        self.grounding_index = grounding_index
        self.field_mapper = field_mapper or FieldMapper()
    
    def build_sql(self, plan: QueryPlan) -> str:
        """Build SQL from query plan."""
        logger.info(f"Building SQL for explore: {plan.explore_name}")
        
        # If LLM has already generated SQL, use it directly
        if hasattr(plan, 'llm_generated_sql') and plan.llm_generated_sql:
            logger.info("Using LLM-generated SQL directly")
            return plan.llm_generated_sql.strip()
        
        # Otherwise, fall back to rule-based generation
        logger.info("Using rule-based SQL generation")
        explore_info = self.grounding_index.get_explore_by_name(plan.explore_name)
        if not explore_info:
            raise ValueError(f"Explore not found: {plan.explore_name}")
        
        # Clear previous aliases
        self.field_mapper.clear_aliases()
        
        # Build SELECT clause
        select_clause = self._build_select_clause(plan.selected_fields)
        
        # Build FROM clause with joins
        from_clause = self._build_from_clause(explore_info, plan.required_joins)
        
        # Build WHERE clause
        where_clause = self._build_where_clause(plan.filters)
        
        # Build GROUP BY clause (if aggregation is present)
        group_by_clause = self._build_group_by_clause(plan.selected_fields, plan.has_aggregation)
        
        # Build LIMIT clause
        limit_clause = self._build_limit_clause(plan.limit)
        
        # Combine into final SQL
        sql_parts = [
            f"SELECT\n{select_clause}",
            f"FROM {from_clause}"
        ]
        
        if where_clause:
            sql_parts.append(f"WHERE {where_clause}")
        
        if group_by_clause:
            sql_parts.append(f"GROUP BY {group_by_clause}")
        
        if limit_clause:
            sql_parts.append(limit_clause)
        
        sql = "\n".join(sql_parts)
        logger.debug(f"Generated SQL:\n{sql}")
        return sql
    
    def _build_select_clause(self, selected_fields: List[FieldInfo]) -> str:
        """Build SELECT clause."""
        select_items = []
        
        for field in selected_fields:
            if field.sql_expression:
                # Resolve LookML expressions
                resolved_sql = self.field_mapper.resolve_lookml_expression(
                    field.sql_expression,
                    field.table_name or '',
                    field.view_name,
                    {}  # Field mappings would be built from context
                )
                select_items.append(f"  {resolved_sql} AS {field.name}")
            else:
                # Fallback to simple column reference
                table_alias = self._get_table_alias(field.view_name)
                select_items.append(f"  {table_alias}.{field.name} AS {field.name}")
        
        return ",\n".join(select_items)
    
    def _build_from_clause(self, explore_info: ExploreInfo, required_joins: Set[str]) -> str:
        """Build FROM clause with necessary joins."""
        # Get base view information
        base_view_name = explore_info.base_view
        base_view = self.grounding_index.lookml_project.get_all_views().get(base_view_name)
        
        if not base_view or not base_view.sql_table_name:
            raise ValueError(f"Base view {base_view_name} not found or missing sql_table_name")
        
        base_table = self._clean_table_name(base_view.sql_table_name)
        base_alias = self._get_table_alias(base_view_name)
        
        # Set alias for base table
        self.field_mapper.set_table_alias(base_table, base_alias)
        
        from_clause = f"{base_table} AS {base_alias}"
        
        # Add joins
        if required_joins:
            join_clauses = self._build_join_clauses(explore_info, required_joins)
            if join_clauses:
                from_clause += "\n" + "\n".join(join_clauses)
        
        return from_clause
    
    def _build_join_clauses(self, explore_info: ExploreInfo, required_joins: Set[str]) -> List[str]:
        """Build JOIN clauses for required views."""
        join_clauses = []
        all_views = self.grounding_index.lookml_project.get_all_views()
        
        # Find relevant joins from explore definition
        explore = None
        for exp_name, exp in self.grounding_index.lookml_project.get_all_explores().items():
            if exp_name == explore_info.name:
                explore = exp
                break
        
        if not explore:
            logger.warning(f"Could not find explore definition for {explore_info.name}")
            return join_clauses
        
        for join in explore.joins:
            if join.view_name in required_joins:
                join_view = all_views.get(join.view_name)
                if not join_view or not join_view.sql_table_name:
                    logger.warning(f"Join view {join.view_name} not found or missing sql_table_name")
                    continue
                
                join_table = self._clean_table_name(join_view.sql_table_name)
                join_alias = self._get_table_alias(join.view_name)
                
                # Set alias for joined table
                self.field_mapper.set_table_alias(join_table, join_alias)
                
                # Build join clause
                join_type = self._convert_join_type(join.type)
                join_clause = f"{join_type} JOIN {join_table} AS {join_alias}"
                
                if join.sql_on:
                    # Resolve join condition
                    resolved_condition = self.field_mapper.resolve_lookml_expression(
                        join.sql_on,
                        join_table,
                        join.view_name,
                        {}
                    )
                    join_clause += f" ON {resolved_condition}"
                
                join_clauses.append(join_clause)
        
        return join_clauses
    
    def _build_where_clause(self, filters: List[str]) -> Optional[str]:
        """Build WHERE clause from filters."""
        if not filters:
            return None
        
        # Resolve any LookML expressions in filters
        resolved_filters = []
        for filter_expr in filters:
            resolved = self.field_mapper.resolve_lookml_expression(
                filter_expr,
                '',  # Table name not needed for most filters
                '',  # View name not needed for most filters
                {}
            )
            resolved_filters.append(resolved)
        
        return " AND ".join(resolved_filters)
    
    def _build_group_by_clause(self, selected_fields: List[FieldInfo], has_aggregation: bool) -> Optional[str]:
        """Build GROUP BY clause if needed."""
        if not has_aggregation:
            return None
        
        # Group by all non-measure fields
        group_by_fields = []
        for i, field in enumerate(selected_fields, 1):
            if field.field_type != 'measure':
                group_by_fields.append(str(i))  # Use positional references
        
        if group_by_fields:
            return ", ".join(group_by_fields)
        
        return None
    
    def _build_limit_clause(self, limit: Optional[int]) -> Optional[str]:
        """Build LIMIT clause."""
        if limit:
            return f"LIMIT {limit}"
        return None
    
    def _clean_table_name(self, table_name: str) -> str:
        """Clean and format table name for BigQuery."""
        # Remove quotes and backticks
        cleaned = table_name.strip('`"\'')
        
        # Ensure proper quoting for BigQuery
        if '.' in cleaned and not cleaned.startswith('`'):
            # Fully qualified table names should be backtick-quoted
            return f"`{cleaned}`"
        
        return cleaned
    
    def _get_table_alias(self, view_name: str) -> str:
        """Generate table alias for view."""
        # Simple alias generation - use view name with underscores
        return view_name.lower().replace('-', '_')
    
    def _convert_join_type(self, lookml_join_type: str) -> str:
        """Convert LookML join type to SQL join type."""
        join_type_map = {
            'left_outer': 'LEFT',
            'right_outer': 'RIGHT',
            'full_outer': 'FULL OUTER',
            'inner': 'INNER',
            'cross': 'CROSS'
        }
        
        return join_type_map.get(lookml_join_type, 'LEFT')
    
    def enforce_limit(self, sql: str, default_limit: int = 100) -> str:
        """Ensure SQL has a LIMIT clause."""
        # Check if LIMIT already exists (case-insensitive)
        if re.search(r'\bLIMIT\s+\d+\b', sql, re.IGNORECASE):
            return sql
        
        # Add default limit
        return f"{sql}\nLIMIT {default_limit}"
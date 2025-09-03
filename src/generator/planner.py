"""Query planner for selecting explores, fields, and joins."""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass

from ..grounding.index import GroundingIndex, ExploreInfo, FieldInfo

logger = logging.getLogger(__name__)


@dataclass
class QueryPlan:
    """Plan for generating SQL from natural language query."""
    explore_name: str
    selected_fields: List[FieldInfo]
    required_joins: Set[str]  # View names that need to be joined
    filters: List[str]  # Filter conditions (basic support)
    limit: Optional[int]
    has_aggregation: bool
    
    # LLM-specific fields
    llm_generated_sql: Optional[str] = None  # Pre-generated SQL from LLM
    llm_response: Optional[Any] = None  # Full LLM response with metadata


class QueryPlanner:
    """Plans SQL generation based on natural language input."""
    
    def __init__(self, grounding_index: GroundingIndex, max_joins: int = 10):
        """Initialize query planner."""
        self.grounding_index = grounding_index
        self.max_joins = max_joins
    
    def plan_query(self, query: str, default_limit: int = 100) -> Optional[QueryPlan]:
        """Plan SQL generation from natural language query."""
        logger.info(f"Planning query: {query}")
        
        # Extract query components
        query_terms = self._extract_query_terms(query)
        limit = self._extract_limit(query) or default_limit
        
        # Select best explore
        explore_name = self._select_explore(query_terms)
        if not explore_name:
            logger.warning("Could not select appropriate explore")
            return None
        
        logger.info(f"Selected explore: {explore_name}")
        
        # Select relevant fields
        selected_fields = self._select_fields(explore_name, query_terms, query)
        if not selected_fields:
            logger.warning("Could not select any relevant fields")
            return None
        
        logger.info(f"Selected {len(selected_fields)} fields")
        
        # Determine required joins
        required_joins = self._determine_required_joins(explore_name, selected_fields)
        
        # Check for aggregation
        has_aggregation = any(field.field_type == 'measure' for field in selected_fields)
        
        # Extract basic filters (limited implementation)
        filters = self._extract_basic_filters(query, selected_fields)
        
        plan = QueryPlan(
            explore_name=explore_name,
            selected_fields=selected_fields,
            required_joins=required_joins,
            filters=filters,
            limit=limit,
            has_aggregation=has_aggregation
        )
        
        logger.info(f"Created query plan with {len(selected_fields)} fields, {len(required_joins)} joins")
        return plan
    
    def _extract_query_terms(self, query: str) -> List[str]:
        """Extract meaningful terms from query."""
        # Simple tokenization - split on whitespace and punctuation
        terms = re.findall(r'\b[a-zA-Z_]\w*\b', query.lower())
        
        # Filter out common stop words and short terms
        stop_words = {
            'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 
            'by', 'from', 'up', 'about', 'into', 'over', 'after', 'what', 'when', 
            'where', 'how', 'show', 'get', 'find', 'list', 'give', 'me', 'i', 'want',
            'need', 'can', 'you', 'please'
        }
        
        meaningful_terms = [term for term in terms if term not in stop_words and len(term) > 2]
        logger.debug(f"Extracted terms: {meaningful_terms}")
        return meaningful_terms
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT clause from query."""
        # Look for patterns like "limit 50", "top 10", "first 25"
        limit_patterns = [
            r'\blimit\s+(\d+)',
            r'\btop\s+(\d+)',
            r'\bfirst\s+(\d+)',
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query.lower())
            if match:
                return int(match.group(1))
        
        return None
    
    def _select_explore(self, query_terms: List[str]) -> Optional[str]:
        """Select the best explore for the query."""
        relevant_explores = self.grounding_index.find_relevant_explores(query_terms)
        
        if not relevant_explores:
            # Fallback: use first available explore
            explores = list(self.grounding_index.explores.keys())
            return explores[0] if explores else None
        
        # Return highest scoring explore
        return relevant_explores[0][0]
    
    def _select_fields(self, explore_name: str, query_terms: List[str], full_query: str) -> List[FieldInfo]:
        """Select relevant fields for the query."""
        relevant_fields = self.grounding_index.find_relevant_fields(explore_name, query_terms)
        
        if not relevant_fields:
            # Fallback: select some basic fields
            explore_info = self.grounding_index.get_explore_by_name(explore_name)
            if explore_info:
                # Get first few non-hidden fields
                available = [f for f in explore_info.available_fields.values() if not f.hidden]
                return available[:5]  # Limit to 5 fields as fallback
            return []
        
        # Filter and prioritize fields
        selected = []
        
        # Add high-scoring fields
        for field_info, score in relevant_fields:
            if score >= 1.0:  # Only include fields with meaningful matches
                selected.append(field_info)
        
        # Ensure we have at least one dimension if we have measures
        measures = [f for f in selected if f.field_type == 'measure']
        dimensions = [f for f in selected if f.field_type == 'dimension']
        
        if measures and not dimensions:
            # Add a primary key or first available dimension
            explore_info = self.grounding_index.get_explore_by_name(explore_name)
            if explore_info:
                for field in explore_info.available_fields.values():
                    if field.field_type == 'dimension' and not field.hidden:
                        selected.append(field)
                        break
        
        # Limit number of fields to avoid overly complex queries
        return selected[:10]
    
    def _determine_required_joins(self, explore_name: str, selected_fields: List[FieldInfo]) -> Set[str]:
        """Determine which views need to be joined based on selected fields."""
        explore_info = self.grounding_index.get_explore_by_name(explore_name)
        if not explore_info:
            return set()
        
        required_views = set()
        base_view = explore_info.base_view
        
        for field in selected_fields:
            if field.view_name != base_view:
                required_views.add(field.view_name)
        
        # Ensure we don't exceed join limit
        if len(required_views) > self.max_joins:
            logger.warning(f"Too many joins required ({len(required_views)}), limiting to {self.max_joins}")
            required_views = set(list(required_views)[:self.max_joins])
        
        return required_views
    
    def _extract_basic_filters(self, query: str, selected_fields: List[FieldInfo]) -> List[str]:
        """Extract basic filter conditions from query (limited implementation)."""
        filters = []
        
        # Look for time-based filters
        time_patterns = [
            (r'last\s+(\d+)\s+days?', lambda m: f"DATE_DIFF(CURRENT_DATE(), DATE({'{field}'}), DAY) <= {m.group(1)}"),
            (r'last\s+(\d+)\s+months?', lambda m: f"DATE_DIFF(CURRENT_DATE(), DATE({'{field}'}), MONTH) <= {m.group(1)}"),
            (r'this\s+year', lambda m: f"EXTRACT(YEAR FROM {'{field}'}) = EXTRACT(YEAR FROM CURRENT_DATE())"),
            (r'this\s+month', lambda m: f"DATE_TRUNC(DATE({'{field}'}), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)"),
        ]
        
        # Find time/date dimensions in selected fields
        time_fields = [f for f in selected_fields if f.lookml_type in ('time', 'date')]
        
        if time_fields:
            time_field = time_fields[0]  # Use first time field
            
            for pattern, filter_func in time_patterns:
                match = re.search(pattern, query.lower())
                if match:
                    filter_expr = filter_func(match).format(field=f"{time_field.view_name}.{time_field.name}")
                    filters.append(filter_expr)
                    break  # Only apply one time filter
        
        return filters
"""Rich schema context generator for LLM prompts."""

import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

from ..grounding.index import GroundingIndex, ExploreInfo, FieldInfo
from ..bigquery.metadata_loader import TableMetadata

logger = logging.getLogger(__name__)


@dataclass
class SchemaContextMetrics:
    """Metrics about generated schema context."""
    total_size: int
    num_tables: int
    num_fields: int
    num_explores: int
    num_relationships: int
    generation_time: float


class SchemaContextGenerator:
    """Generates rich schema context for LLM prompts."""
    
    def __init__(self, grounding_index: GroundingIndex):
        """Initialize schema context generator."""
        self.grounding_index = grounding_index
        self.logger = logging.getLogger(__name__)
    
    def generate_full_context(self, max_size: int = 15000) -> str:
        """Generate comprehensive schema context for LLM.
        
        Args:
            max_size: Maximum context size in characters
            
        Returns:
            Rich schema context string
        """
        import time
        start_time = time.time()
        
        self.logger.info("Generating rich schema context for LLM")
        
        context_parts = []
        
        # Add overview
        context_parts.append(self._generate_overview())
        
        # Add table schemas with relationships
        context_parts.append(self._generate_table_schemas())
        
        # Add explore definitions (table relationships)
        context_parts.append(self._generate_explore_definitions())
        
        # Add field glossary and descriptions
        context_parts.append(self._generate_field_glossary())
        
        # Add sample queries and patterns
        context_parts.append(self._generate_sample_patterns())
        
        full_context = "\n\n".join(context_parts)
        
        # Truncate if too large
        if len(full_context) > max_size:
            self.logger.warning(f"Schema context ({len(full_context)} chars) exceeds max size ({max_size}). Truncating.")
            full_context = full_context[:max_size - 100] + "\n\n... (truncated for length)"
        
        generation_time = time.time() - start_time
        
        # Log metrics
        metrics = SchemaContextMetrics(
            total_size=len(full_context),
            num_tables=len(self.grounding_index.lookml_project.get_all_views()),
            num_fields=sum(len(explore.available_fields) for explore in self.grounding_index.explores.values()),
            num_explores=len(self.grounding_index.explores),
            num_relationships=sum(len(explore.join_graph) for explore in self.grounding_index.explores.values()),
            generation_time=generation_time
        )
        
        self._log_context_metrics(metrics)
        
        return full_context
    
    def generate_focused_context(self, query_terms: List[str], max_size: int = 8000) -> str:
        """Generate focused schema context based on query terms.
        
        Args:
            query_terms: Terms from the user's query
            max_size: Maximum context size
            
        Returns:
            Focused schema context
        """
        import time
        start_time = time.time()
        
        self.logger.info(f"Generating focused schema context for terms: {query_terms}")
        
        # Find relevant explores
        relevant_explores = self.grounding_index.find_relevant_explores(query_terms)[:3]  # Top 3 explores
        
        if not relevant_explores:
            return self.generate_full_context(max_size)
        
        context_parts = []
        
        # Add focused overview
        context_parts.append(self._generate_focused_overview(relevant_explores))
        
        # Add schemas for relevant explores
        for explore_name, score in relevant_explores:
            context_parts.append(self._generate_explore_schema(explore_name))
        
        # Add relevant field descriptions
        context_parts.append(self._generate_relevant_fields(query_terms, relevant_explores))
        
        full_context = "\n\n".join(context_parts)
        
        if len(full_context) > max_size:
            full_context = full_context[:max_size - 100] + "\n\n... (truncated)"
        
        generation_time = time.time() - start_time
        self.logger.info(f"Generated focused context in {generation_time:.2f}s - {len(full_context)} chars")
        
        return full_context
    
    def _generate_overview(self) -> str:
        """Generate database overview."""
        all_views = self.grounding_index.lookml_project.get_all_views()
        explores = self.grounding_index.explores
        
        overview = [
            "# Database Overview",
            f"This is an e-commerce analytics database with {len(all_views)} tables and {len(explores)} pre-defined explores (table relationships).",
            "",
            "## Available Tables:",
        ]
        
        for view_name, view in all_views.items():
            table_name = view.sql_table_name or f"`{view_name}`"
            dim_count = len(view.dimensions)
            measure_count = len(view.measures)
            overview.append(f"- **{view_name}** ({table_name}): {dim_count} dimensions, {measure_count} measures")
        
        return "\n".join(overview)
    
    def _generate_table_schemas(self) -> str:
        """Generate detailed table schemas."""
        all_views = self.grounding_index.lookml_project.get_all_views()
        
        schemas = ["# Table Schemas"]
        
        for view_name, view in all_views.items():
            schemas.append(f"\n## Table: {view_name}")
            schemas.append(f"**Physical Table:** {view.sql_table_name}")
            
            if view.dimensions:
                schemas.append(f"\n**Dimensions ({len(view.dimensions)}):**")
                for dim_name, dimension in view.dimensions.items():
                    type_info = f" ({dimension.type})" if dimension.type else ""
                    desc_info = f" - {dimension.description}" if dimension.description else ""
                    pk_info = " [PRIMARY KEY]" if dimension.primary_key else ""
                    schemas.append(f"- `{dim_name}`{type_info}{pk_info}{desc_info}")
            
            if view.measures:
                schemas.append(f"\n**Measures ({len(view.measures)}):**")
                for measure_name, measure in view.measures.items():
                    type_info = f" ({measure.type})" if measure.type else ""
                    desc_info = f" - {measure.description}" if measure.description else ""
                    schemas.append(f"- `{measure_name}`{type_info}{desc_info}")
        
        return "\n".join(schemas)
    
    def _generate_explore_definitions(self) -> str:
        """Generate explore definitions showing table relationships."""
        explores = ["# Table Relationships (Explores)"]
        
        for explore_name, explore_info in self.grounding_index.explores.items():
            explores.append(f"\n## Explore: {explore_name}")
            explores.append(f"**Base Table:** {explore_info.base_view}")
            explores.append(f"**Available Fields:** {len(explore_info.available_fields)}")
            
            if explore_info.join_graph:
                explores.append(f"\n**Joins ({len(explore_info.join_graph)}):**")
                for joined_view, join_type in explore_info.join_graph.items():
                    condition = explore_info.join_conditions.get(joined_view, "")
                    condition_info = f" ON {condition}" if condition else ""
                    explores.append(f"- {join_type.upper()} JOIN `{joined_view}`{condition_info}")
            else:
                explores.append("**Joins:** None (single table)")
        
        return "\n".join(explores)
    
    def _generate_field_glossary(self) -> str:
        """Generate field glossary with common terms."""
        glossary = ["# Field Glossary"]
        
        # Get sample of common terms
        common_terms = dict(list(self.grounding_index.field_glossary.items())[:20])
        
        if common_terms:
            glossary.append("Common searchable terms and their associated fields:")
            for term, field_infos in common_terms.items():
                field_names = [f.qualified_name for f in field_infos[:3]]  # Top 3 matches
                glossary.append(f"- **{term}**: {', '.join(field_names)}")
        
        return "\n".join(glossary)
    
    def _generate_sample_patterns(self) -> str:
        """Generate sample query patterns."""
        patterns = [
            "# Sample Query Patterns",
            "",
            "## Ranking Queries:",
            "- For 'highest/maximum/top': Use ORDER BY field DESC LIMIT N",
            "- For 'lowest/minimum/bottom': Use ORDER BY field ASC LIMIT N",
            "",
            "## Analytical Queries:",
            "- For 'what product has highest sale_price': Join order_items + products, ORDER BY sale_price DESC LIMIT 1",
            "- For counts: Use COUNT() aggregate function",
            "- For totals: Use SUM() aggregate function",
            "",
            "## Common Field Mappings:",
            "- 'product info' → products table (id, name, brand, category)",
            "- 'user info' → users table (id, name, location, demographics)"
        ]
        
        return "\n".join(patterns)
    
    def _generate_focused_overview(self, relevant_explores: List[tuple]) -> str:
        """Generate focused overview for specific explores."""
        overview = [
            "# Focused Database Context",
            f"Based on your query, the most relevant table relationships are:"
        ]
        
        for explore_name, score in relevant_explores:
            explore_info = self.grounding_index.explores[explore_name]
            overview.append(f"- **{explore_name}** (score: {score:.1f}) - {len(explore_info.available_fields)} fields available")
        
        return "\n".join(overview)
    
    def _generate_explore_schema(self, explore_name: str) -> str:
        """Generate schema for a specific explore."""
        explore_info = self.grounding_index.explores.get(explore_name)
        if not explore_info:
            return f"# Explore: {explore_name} (not found)"
        
        schema = [f"# Explore: {explore_name}"]
        schema.append(f"**Base Table:** {explore_info.base_view}")
        
        # Group fields by view
        fields_by_view = {}
        for field_name, field_info in explore_info.available_fields.items():
            view_name = field_info.view_name
            if view_name not in fields_by_view:
                fields_by_view[view_name] = []
            fields_by_view[view_name].append(field_info)
        
        for view_name, fields in fields_by_view.items():
            schema.append(f"\n## Fields from {view_name}:")
            for field_info in fields[:10]:  # Limit to 10 fields per view
                type_info = f" ({field_info.lookml_type})" if field_info.lookml_type else ""
                desc_info = f" - {field_info.lookml_description}" if field_info.lookml_description else ""
                schema.append(f"- `{field_info.name}`{type_info}{desc_info}")
        
        return "\n".join(schema)
    
    def _generate_relevant_fields(self, query_terms: List[str], relevant_explores: List[tuple]) -> str:
        """Generate field descriptions for relevant fields."""
        fields = ["# Relevant Fields"]
        
        for explore_name, _ in relevant_explores:
            relevant_fields = self.grounding_index.find_relevant_fields(explore_name, query_terms)[:10]
            
            if relevant_fields:
                fields.append(f"\n## Fields matching your query in {explore_name}:")
                for field_info, score in relevant_fields:
                    desc = field_info.combined_description or "No description"
                    fields.append(f"- `{field_info.qualified_name}` (score: {score:.1f}) - {desc}")
        
        return "\n".join(fields)
    
    def _log_context_metrics(self, metrics: SchemaContextMetrics):
        """Log schema context generation metrics."""
        self.logger.info(
            f"Schema context generated - "
            f"Size: {metrics.total_size} chars, "
            f"Tables: {metrics.num_tables}, "
            f"Fields: {metrics.num_fields}, "
            f"Explores: {metrics.num_explores}, "
            f"Relationships: {metrics.num_relationships}, "
            f"Time: {metrics.generation_time:.2f}s"
        )
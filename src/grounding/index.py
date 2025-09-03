"""Grounding index that combines LookML and BigQuery metadata."""

import logging
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass

from ..lookml.models import LookMLProject, LookMLExplore, LookMLView
from ..bigquery.metadata_loader import BigQueryMetadataLoader, TableMetadata
from .field_mapper import FieldMapper

logger = logging.getLogger(__name__)


@dataclass
class FieldInfo:
    """Information about a field combining LookML and BigQuery metadata."""
    name: str
    field_type: str  # 'dimension' or 'measure'
    lookml_type: Optional[str]
    sql_expression: Optional[str]
    lookml_description: Optional[str]
    bigquery_description: Optional[str]
    bigquery_data_type: Optional[str]
    view_name: str
    table_name: Optional[str]
    hidden: bool = False
    
    @property
    def combined_description(self) -> str:
        """Get combined description from LookML and BigQuery."""
        descriptions = []
        if self.lookml_description:
            descriptions.append(f"LookML: {self.lookml_description}")
        if self.bigquery_description:
            descriptions.append(f"BigQuery: {self.bigquery_description}")
        return "; ".join(descriptions) if descriptions else ""
    
    @property
    def qualified_name(self) -> str:
        """Get fully qualified field name."""
        return f"{self.view_name}.{self.name}"


@dataclass
class ExploreInfo:
    """Information about an explore with available fields and joins."""
    name: str
    base_view: str
    available_fields: Dict[str, FieldInfo]
    join_graph: Dict[str, str]  # view_name -> join_type
    join_conditions: Dict[str, str]  # view_name -> join condition


class GroundingIndex:
    """Index combining LookML semantics with BigQuery metadata."""
    
    def __init__(
        self, 
        lookml_project: LookMLProject,
        metadata_loader: BigQueryMetadataLoader,
        field_mapper: Optional[FieldMapper] = None
    ):
        """Initialize grounding index."""
        self.lookml_project = lookml_project
        self.metadata_loader = metadata_loader
        self.field_mapper = field_mapper or FieldMapper()
        
        self.explores: Dict[str, ExploreInfo] = {}
        self.field_glossary: Dict[str, List[FieldInfo]] = {}  # term -> fields
        
        self._build_index()
    
    def _build_index(self) -> None:
        """Build the complete grounding index."""
        logger.info("Building grounding index")
        
        # Get all tables referenced in LookML
        referenced_tables = self._extract_referenced_tables()
        logger.info(f"Found {len(referenced_tables)} tables referenced in LookML")
        
        # Load BigQuery metadata for these tables
        table_metadata = self.metadata_loader.load_metadata_for_tables(referenced_tables)
        
        # Build explore information
        self._build_explores(table_metadata)
        
        # Build field glossary for synonym matching
        self._build_field_glossary()
        
        logger.info(f"Built index with {len(self.explores)} explores and {len(self.field_glossary)} glossary terms")
    
    def _extract_referenced_tables(self) -> Set[str]:
        """Extract all table names referenced in LookML."""
        tables = set()
        
        for view in self.lookml_project.get_all_views().values():
            if view.sql_table_name:
                # Clean up the table name (remove dataset prefix if present)
                table_name = view.sql_table_name.strip('`"')
                if '.' in table_name:
                    table_name = table_name.split('.')[-1]  # Get just the table name
                tables.add(table_name)
        
        return tables
    
    def _build_explores(self, table_metadata: Dict[str, TableMetadata]) -> None:
        """Build explore information with field mappings."""
        all_views = self.lookml_project.get_all_views()
        
        for explore_name, explore in self.lookml_project.get_all_explores().items():
            logger.info(f"Processing explore: {explore_name}")
            
            available_fields = {}
            join_graph = {}
            join_conditions = {}
            
            # Process base view
            base_view = all_views.get(explore.base_view_name)
            if base_view:
                base_table_metadata = self._get_table_metadata_for_view(base_view, table_metadata)
                fields = self._process_view_fields(base_view, base_table_metadata)
                available_fields.update(fields)
            
            # Process joined views
            for join in explore.joins:
                join_view = all_views.get(join.view_name)
                if join_view:
                    join_table_metadata = self._get_table_metadata_for_view(join_view, table_metadata)
                    fields = self._process_view_fields(join_view, join_table_metadata)
                    available_fields.update(fields)
                    
                    join_graph[join.view_name] = join.type
                    if join.sql_on:
                        join_conditions[join.view_name] = join.sql_on
            
            self.explores[explore_name] = ExploreInfo(
                name=explore_name,
                base_view=explore.base_view_name,
                available_fields=available_fields,
                join_graph=join_graph,
                join_conditions=join_conditions
            )
    
    def _get_table_metadata_for_view(
        self, 
        view: LookMLView, 
        table_metadata: Dict[str, TableMetadata]
    ) -> Optional[TableMetadata]:
        """Get BigQuery metadata for a LookML view."""
        if not view.sql_table_name:
            return None
        
        table_name = view.sql_table_name.strip('`"')
        if '.' in table_name:
            table_name = table_name.split('.')[-1]
        
        return table_metadata.get(table_name)
    
    def _process_view_fields(
        self, 
        view: LookMLView, 
        table_metadata: Optional[TableMetadata]
    ) -> Dict[str, FieldInfo]:
        """Process fields from a view, combining LookML and BigQuery metadata."""
        fields = {}
        
        # Process dimensions
        for dim_name, dimension in view.dimensions.items():
            if dimension.hidden:
                continue
            
            # Get corresponding BigQuery column info
            bq_column = None
            if table_metadata and dimension.sql:
                column_name = self.field_mapper.extract_column_name(dimension.sql)
                if column_name and column_name in table_metadata.columns:
                    bq_column = table_metadata.columns[column_name]
            
            field_info = FieldInfo(
                name=dim_name,
                field_type='dimension',
                lookml_type=dimension.type,
                sql_expression=dimension.sql,
                lookml_description=dimension.description,
                bigquery_description=bq_column.description if bq_column else None,
                bigquery_data_type=bq_column.data_type if bq_column else None,
                view_name=view.name,
                table_name=view.sql_table_name,
                hidden=dimension.hidden
            )
            fields[field_info.qualified_name] = field_info
        
        # Process measures
        for measure_name, measure in view.measures.items():
            if measure.hidden:
                continue
            
            field_info = FieldInfo(
                name=measure_name,
                field_type='measure',
                lookml_type=measure.type,
                sql_expression=measure.sql,
                lookml_description=measure.description,
                bigquery_description=None,  # Measures typically don't map to single columns
                bigquery_data_type=None,
                view_name=view.name,
                table_name=view.sql_table_name,
                hidden=measure.hidden
            )
            fields[field_info.qualified_name] = field_info
        
        return fields
    
    def _build_field_glossary(self) -> None:
        """Build field glossary for synonym matching."""
        for explore_info in self.explores.values():
            for field_info in explore_info.available_fields.values():
                # Add field name
                self._add_to_glossary(field_info.name.lower(), field_info)
                
                # Add words from descriptions
                if field_info.lookml_description:
                    words = self._extract_keywords(field_info.lookml_description)
                    for word in words:
                        self._add_to_glossary(word, field_info)
                
                if field_info.bigquery_description:
                    words = self._extract_keywords(field_info.bigquery_description)
                    for word in words:
                        self._add_to_glossary(word, field_info)
    
    def _add_to_glossary(self, term: str, field_info: FieldInfo) -> None:
        """Add a term to the field glossary."""
        term = term.lower().strip()
        if len(term) > 2:  # Skip very short terms
            if term not in self.field_glossary:
                self.field_glossary[term] = []
            if field_info not in self.field_glossary[term]:
                self.field_glossary[term].append(field_info)
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Extract keywords from description text."""
        import re
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        # Filter out common stop words
        stop_words = {'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him', 'his', 'how', 'man', 'new', 'now', 'old', 'see', 'two', 'way', 'who', 'boy', 'did', 'its', 'let', 'put', 'say', 'she', 'too', 'use'}
        return [word for word in words if word not in stop_words]
    
    def get_explore_by_name(self, explore_name: str) -> Optional[ExploreInfo]:
        """Get explore info by name."""
        return self.explores.get(explore_name)
    
    def find_relevant_explores(self, query_terms: List[str]) -> List[Tuple[str, float]]:
        """Find explores relevant to query terms with relevance scores."""
        explore_scores = {}
        
        for explore_name, explore_info in self.explores.items():
            score = 0.0
            
            # Score based on field name matches
            for term in query_terms:
                term_lower = term.lower()
                
                # Direct field name matches
                for field_name in explore_info.available_fields:
                    if term_lower in field_name.lower():
                        score += 2.0
                
                # Glossary matches
                if term_lower in self.field_glossary:
                    matching_fields = self.field_glossary[term_lower]
                    explore_fields = set(explore_info.available_fields.keys())
                    
                    for field_info in matching_fields:
                        if field_info.qualified_name in explore_fields:
                            score += 1.0
            
            if score > 0:
                explore_scores[explore_name] = score
        
        # Sort by score descending
        return sorted(explore_scores.items(), key=lambda x: x[1], reverse=True)
    
    def find_relevant_fields(self, explore_name: str, query_terms: List[str]) -> List[Tuple[FieldInfo, float]]:
        """Find fields relevant to query terms within an explore."""
        explore_info = self.explores.get(explore_name)
        if not explore_info:
            return []
        
        field_scores = {}
        
        for term in query_terms:
            term_lower = term.lower()
            
            # Check direct field name matches
            for field_name, field_info in explore_info.available_fields.items():
                if term_lower in field_info.name.lower():
                    field_scores[field_info.qualified_name] = field_scores.get(field_info.qualified_name, 0) + 3.0
            
            # Check glossary matches
            if term_lower in self.field_glossary:
                for field_info in self.field_glossary[term_lower]:
                    if field_info.qualified_name in explore_info.available_fields:
                        field_scores[field_info.qualified_name] = field_scores.get(field_info.qualified_name, 0) + 1.0
        
        # Convert to list of (field_info, score) tuples
        result = []
        for field_name, score in field_scores.items():
            field_info = explore_info.available_fields[field_name]
            result.append((field_info, score))
        
        # Sort by score descending
        return sorted(result, key=lambda x: x[1], reverse=True)
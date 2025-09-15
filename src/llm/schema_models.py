"""Data models for schema intelligence."""

from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from enum import Enum

from ..grounding.index import FieldInfo


class FieldSemanticType(Enum):
    """Semantic types for database fields."""
    TRANSACTIONAL_VALUE = "transactional_value"  # Actual money exchanged
    REFERENCE_PRICE = "reference_price"  # Listed/catalog prices
    QUANTITY = "quantity"  # Counts, amounts, volumes
    IDENTIFIER = "identifier"  # Keys, IDs, references
    TEMPORAL = "temporal"  # Dates, timestamps
    CATEGORICAL = "categorical"  # Classifications, statuses, types
    DESCRIPTIVE = "descriptive"  # Names, descriptions, text
    CALCULATED = "calculated"  # Derived measures


class TableBusinessType(Enum):
    """Business types for database tables."""
    FACT = "fact"  # Transactional data
    DIMENSION = "dimension"  # Reference/lookup data
    BRIDGE = "bridge"  # Many-to-many relationships
    AGGREGATE = "aggregate"  # Pre-calculated summaries


@dataclass
class EnrichedFieldInfo:
    """Field information enriched with semantic analysis."""
    field_info: FieldInfo
    semantic_type: FieldSemanticType
    business_purpose: str
    confidence_score: float
    usage_recommendations: List[str]
    common_mistakes: List[str]
    related_fields: List[str]


@dataclass
class TableSemantics:
    """Semantic analysis of a table's business purpose."""
    table_name: str
    business_type: TableBusinessType
    primary_purpose: str
    key_concepts: List[str]
    best_for_queries: List[str]
    avoid_for_queries: List[str]
    performance_notes: List[str]


@dataclass
class SchemaIntelligence:
    """Complete semantic understanding of the database schema."""
    enriched_fields: Dict[str, EnrichedFieldInfo]  # qualified_name -> enriched info
    table_semantics: Dict[str, TableSemantics]  # table_name -> semantics
    business_concept_map: Dict[str, List[str]]  # concept -> relevant field names
    query_patterns: Dict[str, Dict[str, Any]]  # query_type -> guidance
    relationship_insights: Dict[str, List[str]]  # table -> related tables with context
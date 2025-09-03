"""Schema Intelligence Service for dynamic metadata analysis using Gemini 2.5 Pro."""

import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from enum import Enum

from .gemini_service import GeminiService
from ..grounding.index import GroundingIndex, FieldInfo, ExploreInfo
from ..bigquery.metadata_loader import TableMetadata, ColumnMetadata

logger = logging.getLogger(__name__)


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


class SchemaIntelligenceService:
    """Service for analyzing schema semantics using Gemini 2.5 Pro."""
    
    def __init__(self, gemini_service: GeminiService, cache_ttl: int = 3600):
        """Initialize schema intelligence service.
        
        Args:
            gemini_service: Gemini service for LLM analysis
            cache_ttl: Cache time-to-live in seconds
        """
        self.gemini_service = gemini_service
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, float] = {}
        self.logger = logging.getLogger(__name__)
    
    def analyze_schema(self, grounding_index: GroundingIndex) -> SchemaIntelligence:
        """Perform complete semantic analysis of the schema.
        
        Args:
            grounding_index: Grounding index with LookML and BigQuery metadata
            
        Returns:
            Complete schema intelligence with semantic understanding
        """
        cache_key = "full_schema_analysis"
        
        # Check cache
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            self.logger.info("Using cached schema intelligence")
            return cached_result
        
        self.logger.info("Starting comprehensive schema semantic analysis")
        start_time = time.time()
        
        # Step 1: Analyze field semantics
        enriched_fields = self._analyze_field_semantics(grounding_index)
        
        # Step 2: Analyze table business purposes
        table_semantics = self._analyze_table_semantics(grounding_index, enriched_fields)
        
        # Step 3: Build business concept mappings
        business_concept_map = self._build_business_concept_map(enriched_fields)
        
        # Step 4: Generate query patterns
        query_patterns = self._generate_query_patterns(enriched_fields, table_semantics)
        
        # Step 5: Analyze relationships
        relationship_insights = self._analyze_relationships(grounding_index, table_semantics)
        
        schema_intelligence = SchemaIntelligence(
            enriched_fields=enriched_fields,
            table_semantics=table_semantics,
            business_concept_map=business_concept_map,
            query_patterns=query_patterns,
            relationship_insights=relationship_insights
        )
        
        # Cache the result
        self._cache_result(cache_key, schema_intelligence)
        
        elapsed = time.time() - start_time
        self.logger.info(
            f"Schema semantic analysis completed in {elapsed:.2f}s - "
            f"Analyzed {len(enriched_fields)} fields across {len(table_semantics)} tables"
        )
        
        return schema_intelligence
    
    def _analyze_field_semantics(self, grounding_index: GroundingIndex) -> Dict[str, EnrichedFieldInfo]:
        """Analyze semantic meaning of each field using parallel processing."""
        self.logger.info("Analyzing field semantics with Gemini (parallel processing)")
        
        enriched_fields = {}
        
        # Group fields by table for efficient analysis
        fields_by_table = {}
        for explore_info in grounding_index.explores.values():
            for field_name, field_info in explore_info.available_fields.items():
                table = field_info.view_name
                if table not in fields_by_table:
                    fields_by_table[table] = []
                fields_by_table[table].append(field_info)
        
        # Process tables in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all table analysis tasks
            future_to_table = {
                executor.submit(self._analyze_table_fields, table_name, fields): table_name
                for table_name, fields in fields_by_table.items()
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    table_analysis = future.result()
                    enriched_fields.update(table_analysis)
                    self.logger.debug(f"Completed field analysis for table: {table_name}")
                except Exception as e:
                    self.logger.error(f"Field analysis failed for table {table_name}: {e}")
                    # Continue processing other tables
        
        self.logger.info(f"Completed parallel field analysis for {len(fields_by_table)} tables")
        return enriched_fields
    
    def _analyze_table_fields(self, table_name: str, fields: List[FieldInfo]) -> Dict[str, EnrichedFieldInfo]:
        """Analyze all fields in a table together for context."""
        # Prepare field metadata for analysis
        field_metadata = []
        for field in fields:
            metadata = {
                "name": field.name,
                "qualified_name": field.qualified_name,
                "type": field.field_type,
                "lookml_type": field.lookml_type,
                "sql_expression": field.sql_expression,
                "lookml_description": field.lookml_description,
                "bigquery_description": field.bigquery_description,
                "bigquery_data_type": field.bigquery_data_type
            }
            field_metadata.append(metadata)
        
        # Analyze with Gemini
        analysis_prompt = self._build_field_analysis_prompt(table_name, field_metadata)
        
        try:
            response = self.gemini_service.generate_content(
                prompt=analysis_prompt,
                temperature=0.1  # Lower temperature for consistent analysis
            )
            
            # Parse Gemini's response
            field_analysis = self._parse_field_analysis_response(response.content, fields)
            return field_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing fields for table {table_name}: {e}")
            # Fallback: create basic enriched fields
            return self._create_fallback_field_analysis(fields)
    
    def _build_field_analysis_prompt(self, table_name: str, field_metadata: List[Dict]) -> str:
        """Build prompt for field semantic analysis."""
        return f"""
Analyze the following database fields from table '{table_name}' and provide semantic understanding:

FIELDS TO ANALYZE:
{json.dumps(field_metadata, indent=2)}

For each field, determine:
1. SEMANTIC_TYPE: Choose from:
   - TRANSACTIONAL_VALUE: Fields representing actual money exchanged in transactions
   - REFERENCE_PRICE: Listed/catalog prices (not actual transaction values)
   - QUANTITY: Counts, amounts, volumes, numeric measurements
   - IDENTIFIER: Keys, IDs, references to other entities
   - TEMPORAL: Dates, timestamps, time-based fields
   - CATEGORICAL: Classifications, statuses, types, categories
   - DESCRIPTIVE: Names, descriptions, text content
   - CALCULATED: Derived measures, computed values

2. BUSINESS_PURPOSE: One sentence explaining what this field represents in business terms

3. CONFIDENCE_SCORE: 0.0-1.0 confidence in the semantic type classification

4. USAGE_RECOMMENDATIONS: List of when/how to use this field appropriately

5. COMMON_MISTAKES: List of common mistakes when using this field

6. RELATED_FIELDS: List of other field names that are commonly used together

Respond in this exact JSON format:
{{
  "field_qualified_name": {{
    "semantic_type": "SEMANTIC_TYPE_VALUE",
    "business_purpose": "Business explanation",
    "confidence_score": 0.95,
    "usage_recommendations": ["recommendation1", "recommendation2"],
    "common_mistakes": ["mistake1", "mistake2"],
    "related_fields": ["field1", "field2"]
  }}
}}

CRITICAL: For e-commerce/transaction data:
- Fields representing actual money received should be TRANSACTIONAL_VALUE
- Catalog/listed prices should be REFERENCE_PRICE  
- Revenue calculations should use TRANSACTIONAL_VALUE fields, never REFERENCE_PRICE
"""
    
    def _parse_field_analysis_response(self, response: str, fields: List[FieldInfo]) -> Dict[str, EnrichedFieldInfo]:
        """Parse Gemini's field analysis response."""
        try:
            # Clean the response to extract JSON
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            analysis_data = json.loads(response)
            
            enriched_fields = {}
            field_map = {f.qualified_name: f for f in fields}
            
            for qualified_name, analysis in analysis_data.items():
                if qualified_name in field_map:
                    try:
                        semantic_type = FieldSemanticType(analysis["semantic_type"].lower())
                    except ValueError:
                        semantic_type = FieldSemanticType.DESCRIPTIVE  # Fallback
                    
                    enriched_fields[qualified_name] = EnrichedFieldInfo(
                        field_info=field_map[qualified_name],
                        semantic_type=semantic_type,
                        business_purpose=analysis.get("business_purpose", ""),
                        confidence_score=analysis.get("confidence_score", 0.5),
                        usage_recommendations=analysis.get("usage_recommendations", []),
                        common_mistakes=analysis.get("common_mistakes", []),
                        related_fields=analysis.get("related_fields", [])
                    )
            
            return enriched_fields
            
        except Exception as e:
            self.logger.error(f"Error parsing field analysis response: {e}")
            return self._create_fallback_field_analysis(fields)
    
    def _create_fallback_field_analysis(self, fields: List[FieldInfo]) -> Dict[str, EnrichedFieldInfo]:
        """Create basic field analysis as fallback."""
        enriched_fields = {}
        
        for field in fields:
            # Basic semantic type inference
            semantic_type = self._infer_basic_semantic_type(field)
            
            enriched_fields[field.qualified_name] = EnrichedFieldInfo(
                field_info=field,
                semantic_type=semantic_type,
                business_purpose=field.lookml_description or field.bigquery_description or "Field purpose unknown",
                confidence_score=0.3,  # Low confidence for fallback
                usage_recommendations=[],
                common_mistakes=[],
                related_fields=[]
            )
        
        return enriched_fields
    
    def _infer_basic_semantic_type(self, field: FieldInfo) -> FieldSemanticType:
        """Basic inference of semantic type from field properties."""
        field_name_lower = field.name.lower()
        
        if any(term in field_name_lower for term in ['id', 'key']):
            return FieldSemanticType.IDENTIFIER
        elif any(term in field_name_lower for term in ['price', 'cost', 'amount', 'value']):
            if 'sale' in field_name_lower or 'transaction' in field_name_lower:
                return FieldSemanticType.TRANSACTIONAL_VALUE
            else:
                return FieldSemanticType.REFERENCE_PRICE
        elif any(term in field_name_lower for term in ['count', 'quantity', 'number']):
            return FieldSemanticType.QUANTITY
        elif any(term in field_name_lower for term in ['date', 'time', 'created', 'updated']):
            return FieldSemanticType.TEMPORAL
        elif any(term in field_name_lower for term in ['status', 'type', 'category']):
            return FieldSemanticType.CATEGORICAL
        elif any(term in field_name_lower for term in ['name', 'description', 'title']):
            return FieldSemanticType.DESCRIPTIVE
        else:
            return FieldSemanticType.DESCRIPTIVE
    
    def _analyze_table_semantics(self, grounding_index: GroundingIndex, enriched_fields: Dict[str, EnrichedFieldInfo]) -> Dict[str, TableSemantics]:
        """Analyze business purpose of each table using parallel processing."""
        self.logger.info("Analyzing table business semantics (parallel processing)")
        
        table_semantics = {}
        
        # Get all unique tables
        all_views = grounding_index.lookml_project.get_all_views()
        
        # Prepare table analysis tasks
        table_analysis_tasks = []
        for view_name, view in all_views.items():
            # Collect field information for this table
            table_fields = []
            for qualified_name, enriched_field in enriched_fields.items():
                if enriched_field.field_info.view_name == view_name:
                    table_fields.append(enriched_field)
            
            if table_fields:
                table_analysis_tasks.append((view_name, view, table_fields))
        
        # Process tables in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all table analysis tasks
            future_to_table = {
                executor.submit(self._analyze_single_table_semantics, view_name, view, table_fields): view_name
                for view_name, view, table_fields in table_analysis_tasks
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_table):
                view_name = future_to_table[future]
                try:
                    semantics = future.result()
                    table_semantics[view_name] = semantics
                    self.logger.debug(f"Completed table semantic analysis for: {view_name}")
                except Exception as e:
                    self.logger.error(f"Table semantic analysis failed for {view_name}: {e}")
                    # Continue processing other tables
        
        self.logger.info(f"Completed parallel table semantic analysis for {len(table_analysis_tasks)} tables")
        return table_semantics
    
    def _analyze_single_table_semantics(self, table_name: str, view, enriched_fields: List[EnrichedFieldInfo]) -> TableSemantics:
        """Analyze semantics of a single table."""
        # Prepare table analysis data
        table_summary = {
            "table_name": table_name,
            "sql_table_name": view.sql_table_name,
            "dimensions_count": len(view.dimensions),
            "measures_count": len(view.measures),
            "field_types": {}
        }
        
        # Summarize field types
        for enriched_field in enriched_fields:
            semantic_type = enriched_field.semantic_type.value
            if semantic_type not in table_summary["field_types"]:
                table_summary["field_types"][semantic_type] = 0
            table_summary["field_types"][semantic_type] += 1
        
        # Analyze with Gemini
        analysis_prompt = self._build_table_analysis_prompt(table_summary, enriched_fields)
        
        try:
            response = self.gemini_service.generate_content(
                prompt=analysis_prompt,
                temperature=0.1
            )
            
            return self._parse_table_analysis_response(response.content, table_name)
            
        except Exception as e:
            self.logger.error(f"Error analyzing table {table_name}: {e}")
            return self._create_fallback_table_analysis(table_name, enriched_fields)
    
    def _build_table_analysis_prompt(self, table_summary: Dict, enriched_fields: List[EnrichedFieldInfo]) -> str:
        """Build prompt for table semantic analysis."""
        field_summary = []
        for field in enriched_fields:
            field_summary.append({
                "name": field.field_info.name,
                "semantic_type": field.semantic_type.value,
                "business_purpose": field.business_purpose
            })
        
        return f"""
Analyze this database table and determine its business purpose:

TABLE SUMMARY:
{json.dumps(table_summary, indent=2)}

FIELD ANALYSIS:
{json.dumps(field_summary, indent=2)}

Determine:
1. BUSINESS_TYPE: Choose from:
   - FACT: Contains transactional/event data, measures, metrics
   - DIMENSION: Contains reference/lookup data, descriptions, categories
   - BRIDGE: Handles many-to-many relationships
   - AGGREGATE: Contains pre-calculated summaries

2. PRIMARY_PURPOSE: One sentence describing the table's main business function

3. KEY_CONCEPTS: List of main business concepts this table represents

4. BEST_FOR_QUERIES: Types of business questions this table is ideal for answering

5. AVOID_FOR_QUERIES: Types of questions this table should NOT be used for

6. PERFORMANCE_NOTES: Any performance considerations when using this table

Respond in this exact JSON format:
{{
  "business_type": "BUSINESS_TYPE_VALUE",
  "primary_purpose": "Business purpose description",
  "key_concepts": ["concept1", "concept2"],
  "best_for_queries": ["query_type1", "query_type2"],
  "avoid_for_queries": ["avoid_type1", "avoid_type2"], 
  "performance_notes": ["note1", "note2"]
}}
"""
    
    def _parse_table_analysis_response(self, response: str, table_name: str) -> TableSemantics:
        """Parse Gemini's table analysis response."""
        try:
            # Clean response
            response = response.strip()
            if response.startswith("```json"):
                response = response[7:]
            if response.endswith("```"):
                response = response[:-3]
            response = response.strip()
            
            analysis = json.loads(response)
            
            try:
                business_type = TableBusinessType(analysis["business_type"].lower())
            except ValueError:
                business_type = TableBusinessType.DIMENSION  # Fallback
            
            return TableSemantics(
                table_name=table_name,
                business_type=business_type,
                primary_purpose=analysis.get("primary_purpose", ""),
                key_concepts=analysis.get("key_concepts", []),
                best_for_queries=analysis.get("best_for_queries", []),
                avoid_for_queries=analysis.get("avoid_for_queries", []),
                performance_notes=analysis.get("performance_notes", [])
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing table analysis for {table_name}: {e}")
            return self._create_fallback_table_analysis(table_name, [])
    
    def _create_fallback_table_analysis(self, table_name: str, enriched_fields: List[EnrichedFieldInfo]) -> TableSemantics:
        """Create fallback table analysis."""
        # Basic inference
        has_transactional_values = any(f.semantic_type == FieldSemanticType.TRANSACTIONAL_VALUE for f in enriched_fields)
        has_many_identifiers = sum(1 for f in enriched_fields if f.semantic_type == FieldSemanticType.IDENTIFIER) > 2
        
        if has_transactional_values:
            business_type = TableBusinessType.FACT
        elif has_many_identifiers:
            business_type = TableBusinessType.BRIDGE
        else:
            business_type = TableBusinessType.DIMENSION
        
        return TableSemantics(
            table_name=table_name,
            business_type=business_type,
            primary_purpose=f"Table containing {table_name} data",
            key_concepts=[table_name],
            best_for_queries=[],
            avoid_for_queries=[],
            performance_notes=[]
        )
    
    def _build_business_concept_map(self, enriched_fields: Dict[str, EnrichedFieldInfo]) -> Dict[str, List[str]]:
        """Build mapping from business concepts to relevant fields."""
        concept_map = {}
        
        for qualified_name, enriched_field in enriched_fields.items():
            # Extract concepts from business purpose and field name
            concepts = self._extract_business_concepts(enriched_field)
            
            for concept in concepts:
                if concept not in concept_map:
                    concept_map[concept] = []
                concept_map[concept].append(qualified_name)
        
        return concept_map
    
    def _extract_business_concepts(self, enriched_field: EnrichedFieldInfo) -> List[str]:
        """Extract business concepts from field information."""
        concepts = []
        
        # Add semantic type as concept
        concepts.append(enriched_field.semantic_type.value)
        
        # Extract from business purpose
        purpose_lower = enriched_field.business_purpose.lower()
        if 'revenue' in purpose_lower or 'sales' in purpose_lower:
            concepts.append('revenue')
        if 'customer' in purpose_lower or 'user' in purpose_lower:
            concepts.append('customer')
        if 'product' in purpose_lower:
            concepts.append('product')
        if 'order' in purpose_lower:
            concepts.append('order')
        if 'time' in purpose_lower or 'date' in purpose_lower:
            concepts.append('time')
        
        # Extract from field name
        field_name_lower = enriched_field.field_info.name.lower()
        if 'price' in field_name_lower or 'amount' in field_name_lower:
            concepts.append('monetary')
        if 'count' in field_name_lower or 'quantity' in field_name_lower:
            concepts.append('quantity')
        
        return list(set(concepts))  # Remove duplicates
    
    def _generate_query_patterns(self, enriched_fields: Dict[str, EnrichedFieldInfo], table_semantics: Dict[str, TableSemantics]) -> Dict[str, Dict[str, Any]]:
        """Generate query patterns based on schema analysis."""
        self.logger.info("Generating query patterns based on schema analysis")
        
        # Identify key business concepts and their fields
        revenue_fields = []
        customer_fields = []
        product_fields = []
        time_fields = []
        
        for qualified_name, enriched_field in enriched_fields.items():
            if enriched_field.semantic_type == FieldSemanticType.TRANSACTIONAL_VALUE:
                revenue_fields.append(qualified_name)
            elif 'customer' in enriched_field.business_purpose.lower() or 'user' in enriched_field.business_purpose.lower():
                customer_fields.append(qualified_name)
            elif 'product' in enriched_field.business_purpose.lower():
                product_fields.append(qualified_name)
            elif enriched_field.semantic_type == FieldSemanticType.TEMPORAL:
                time_fields.append(qualified_name)
        
        # Find fact tables for different business processes
        fact_tables = [name for name, semantics in table_semantics.items() 
                      if semantics.business_type == TableBusinessType.FACT]
        
        query_patterns = {}
        
        # Revenue calculation patterns
        if revenue_fields and fact_tables:
            query_patterns["revenue_calculation"] = {
                "description": "Patterns for calculating revenue/sales",
                "primary_fields": revenue_fields,
                "primary_tables": fact_tables,
                "guidance": [
                    f"Use {', '.join(revenue_fields)} for actual revenue calculations",
                    "Avoid using reference_price or catalog price fields for revenue",
                    f"Primary transaction tables: {', '.join(fact_tables)}"
                ],
                "example_patterns": [
                    "SELECT SUM(transactional_value_field) FROM fact_table",
                    "For revenue by category: JOIN fact_table with dimension_table on relationship"
                ]
            }
        
        # Customer analysis patterns
        if customer_fields:
            query_patterns["customer_analysis"] = {
                "description": "Patterns for customer/user analysis",
                "primary_fields": customer_fields,
                "guidance": [
                    f"Customer identification fields: {', '.join(customer_fields[:3])}",
                    "Use customer dimensions for segmentation and grouping"
                ]
            }
        
        # Time-based analysis patterns
        if time_fields:
            query_patterns["time_analysis"] = {
                "description": "Patterns for time-based queries",
                "primary_fields": time_fields,
                "guidance": [
                    f"Available time fields: {', '.join(time_fields[:3])}",
                    "Use DATE_TRUNC for grouping by time periods",
                    "Use timestamp comparison for filtering date ranges"
                ]
            }
        
        return query_patterns
    
    def _analyze_relationships(self, grounding_index: GroundingIndex, table_semantics: Dict[str, TableSemantics]) -> Dict[str, List[str]]:
        """Analyze table relationships and their business context."""
        self.logger.info("Analyzing table relationships and business context")
        
        relationship_insights = {}
        
        # Analyze each explore's join structure
        for explore_name, explore_info in grounding_index.explores.items():
            base_view = explore_info.base_view
            insights = []
            
            # Base table context
            if base_view in table_semantics:
                base_semantics = table_semantics[base_view]
                insights.append(f"Base table '{base_view}': {base_semantics.primary_purpose}")
                
                if base_semantics.business_type == TableBusinessType.FACT:
                    insights.append("This is a FACT table - contains transactional/measurable data")
                elif base_semantics.business_type == TableBusinessType.DIMENSION:
                    insights.append("This is a DIMENSION table - contains reference/descriptive data")
            
            # Analyze joins
            if explore_info.join_graph:
                insights.append(f"Available joins: {len(explore_info.join_graph)} tables")
                
                for joined_view, join_type in explore_info.join_graph.items():
                    join_condition = explore_info.join_conditions.get(joined_view, "")
                    
                    join_insight = f"{join_type.upper()} JOIN {joined_view}"
                    if join_condition:
                        join_insight += f" ON {join_condition}"
                    
                    # Add business context for joined table
                    if joined_view in table_semantics:
                        joined_semantics = table_semantics[joined_view]
                        join_insight += f" - {joined_semantics.primary_purpose}"
                    
                    insights.append(join_insight)
            else:
                insights.append("Single table explore - no joins available")
            
            # Add guidance for this explore
            if base_view in table_semantics:
                base_semantics = table_semantics[base_view]
                if base_semantics.best_for_queries:
                    insights.append(f"Best for: {', '.join(base_semantics.best_for_queries)}")
                if base_semantics.avoid_for_queries:
                    insights.append(f"Avoid for: {', '.join(base_semantics.avoid_for_queries)}")
            
            relationship_insights[explore_name] = insights
        
        return relationship_insights
    
    def _get_cached_result(self, cache_key: str) -> Optional[Any]:
        """Get cached result if still valid."""
        if cache_key in self._cache and cache_key in self._cache_timestamps:
            if time.time() - self._cache_timestamps[cache_key] < self.cache_ttl:
                return self._cache[cache_key]
        return None
    
    def _cache_result(self, cache_key: str, result: Any) -> None:
        """Cache analysis result."""
        self._cache[cache_key] = result
        self._cache_timestamps[cache_key] = time.time()
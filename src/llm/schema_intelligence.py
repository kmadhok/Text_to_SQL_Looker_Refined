"""Schema Intelligence Service for dynamic metadata analysis using Gemini 2.5 Pro."""

import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from enum import Enum

from .gemini_service import GeminiService
from .schema_models import (
    SchemaIntelligence, EnrichedFieldInfo, TableSemantics, 
    FieldSemanticType, TableBusinessType
)
from .schema_intelligence_storage import SchemaIntelligencePersistentStorage
from ..grounding.index import GroundingIndex, FieldInfo, ExploreInfo
from ..bigquery.metadata_loader import TableMetadata, ColumnMetadata

logger = logging.getLogger(__name__)


class SchemaIntelligenceService:
    """Service for analyzing schema semantics using Gemini 2.5 Pro."""
    
    def __init__(self, gemini_service: GeminiService, storage_dir: Optional[str] = None):
        """Initialize schema intelligence service.
        
        Args:
            gemini_service: Gemini service for LLM analysis
            storage_dir: Directory for persistent storage (default: data/schema_intelligence)
        """
        self.gemini_service = gemini_service
        self.persistent_storage = SchemaIntelligencePersistentStorage(
            storage_dir or "data/schema_intelligence"
        )
        self.logger = logging.getLogger(__name__)
    
    def analyze_schema(self, grounding_index: GroundingIndex) -> SchemaIntelligence:
        """Perform complete semantic analysis of the schema.
        
        Args:
            grounding_index: Grounding index with LookML and BigQuery metadata
            
        Returns:
            Complete schema intelligence with semantic understanding
        """
        # Generate current schema fingerprint
        current_fingerprint = self.persistent_storage.generate_schema_fingerprint(grounding_index)
        
        # Check if we can use existing analysis
        if not self.persistent_storage.has_schema_changed(current_fingerprint):
            # Try to load existing analysis
            cached_intelligence = self.persistent_storage.load_schema_intelligence()
            if cached_intelligence:
                self.logger.info("Using saved schema intelligence (no schema changes detected)")
                return cached_intelligence
        
        # Schema has changed or no saved analysis - perform full analysis
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
        
        # Save the analysis and fingerprint
        try:
            self.persistent_storage.save_schema_intelligence(schema_intelligence, current_fingerprint)
            self.logger.info("Schema intelligence saved to persistent storage")
        except Exception as e:
            self.logger.warning(f"Failed to save schema intelligence: {e}")
        
        elapsed = time.time() - start_time
        self.logger.info(
            f"Schema semantic analysis completed in {elapsed:.2f}s - "
            f"Analyzed {len(enriched_fields)} fields across {len(table_semantics)} tables"
        )
        
        return schema_intelligence
    
    def _analyze_field_semantics(self, grounding_index: GroundingIndex) -> Dict[str, EnrichedFieldInfo]:
        """Analyze semantic meaning of each field using intelligent batching and parallel processing."""
        self.logger.info("Analyzing field semantics with Gemini (intelligent batching + parallel processing)")
        
        enriched_fields = {}
        
        # Group fields by table for efficient analysis
        fields_by_table = {}
        for explore_info in grounding_index.explores.values():
            for field_name, field_info in explore_info.available_fields.items():
                table = field_info.view_name
                if table not in fields_by_table:
                    fields_by_table[table] = []
                fields_by_table[table].append(field_info)
        
        # Create intelligent batches of similar tables
        table_batches = self._create_intelligent_table_batches(fields_by_table)
        
        self.logger.info(f"Created {len(table_batches)} intelligent batches from {len(fields_by_table)} tables")
        
        # Process batches in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all batch analysis tasks
            future_to_batch = {
                executor.submit(self._analyze_table_batch, batch): f"batch_{i}"
                for i, batch in enumerate(table_batches)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_batch):
                batch_name = future_to_batch[future]
                try:
                    batch_analysis = future.result()
                    enriched_fields.update(batch_analysis)
                    self.logger.debug(f"Completed field analysis for {batch_name}")
                except Exception as e:
                    self.logger.error(f"Field analysis failed for {batch_name}: {e}")
                    # Continue processing other batches
        
        self.logger.info(f"Completed intelligent batched field analysis - {len(enriched_fields)} fields analyzed")
        return enriched_fields
    
    def _create_intelligent_table_batches(self, fields_by_table: Dict[str, List[FieldInfo]]) -> List[Dict[str, List[FieldInfo]]]:
        """Create intelligent batches of similar tables to reduce API calls."""
        
        # Classify tables by business type for intelligent batching
        fact_tables = {}
        dimension_tables = {}
        bridge_tables = {}
        large_tables = {}
        
        for table_name, fields in fields_by_table.items():
            # Quick classification based on field characteristics
            table_type = self._classify_table_by_fields(table_name, fields)
            field_count = len(fields)
            
            # Large tables (>15 fields) get their own batch for better focus
            if field_count > 15:
                large_tables[table_name] = fields
            elif table_type == "fact":
                fact_tables[table_name] = fields
            elif table_type == "bridge":
                bridge_tables[table_name] = fields
            else:
                dimension_tables[table_name] = fields
        
        batches = []
        
        # Large tables: one per batch (they need focused attention)
        for table_name, fields in large_tables.items():
            batches.append({table_name: fields})
            self.logger.debug(f"Large table batch: {table_name} ({len(fields)} fields)")
        
        # Fact tables: batch up to 2 together (they're usually complex)
        fact_batch = {}
        for table_name, fields in fact_tables.items():
            fact_batch[table_name] = fields
            if len(fact_batch) >= 2:  # Max 2 fact tables per batch
                batches.append(fact_batch)
                self.logger.debug(f"Fact table batch: {list(fact_batch.keys())}")
                fact_batch = {}
        if fact_batch:  # Add remaining fact tables
            batches.append(fact_batch)
            self.logger.debug(f"Final fact table batch: {list(fact_batch.keys())}")
        
        # Dimension tables: batch up to 3 together (they're usually simpler)
        dimension_batch = {}
        for table_name, fields in dimension_tables.items():
            dimension_batch[table_name] = fields
            if len(dimension_batch) >= 3:  # Max 3 dimension tables per batch
                batches.append(dimension_batch)
                self.logger.debug(f"Dimension table batch: {list(dimension_batch.keys())}")
                dimension_batch = {}
        if dimension_batch:  # Add remaining dimension tables
            batches.append(dimension_batch)
            self.logger.debug(f"Final dimension table batch: {list(dimension_batch.keys())}")
        
        # Bridge tables: batch up to 2 together
        bridge_batch = {}
        for table_name, fields in bridge_tables.items():
            bridge_batch[table_name] = fields
            if len(bridge_batch) >= 2:  # Max 2 bridge tables per batch
                batches.append(bridge_batch)
                self.logger.debug(f"Bridge table batch: {list(bridge_batch.keys())}")
                bridge_batch = {}
        if bridge_batch:  # Add remaining bridge tables
            batches.append(bridge_batch)
            self.logger.debug(f"Final bridge table batch: {list(bridge_batch.keys())}")
        
        return batches
    
    def _classify_table_by_fields(self, table_name: str, fields: List[FieldInfo]) -> str:
        """Quick classification of table type based on field characteristics."""
        
        transactional_indicators = 0
        reference_indicators = 0
        bridge_indicators = 0
        
        for field in fields:
            field_name_lower = field.name.lower()
            
            # Look for transaction/fact indicators
            if any(term in field_name_lower for term in ['price', 'amount', 'cost', 'revenue', 'sale', 'quantity', 'total']):
                transactional_indicators += 1
            
            # Look for reference/dimension indicators  
            elif any(term in field_name_lower for term in ['name', 'description', 'category', 'type', 'status', 'brand']):
                reference_indicators += 1
            
            # Look for bridge indicators (multiple foreign keys)
            elif field_name_lower.endswith('_id') and field_name_lower != 'id':
                bridge_indicators += 1
        
        # Classification logic
        if transactional_indicators >= 2:
            return "fact"
        elif bridge_indicators >= 3:  # Multiple foreign keys suggest bridge table
            return "bridge"
        elif reference_indicators >= 2 or 'users' in table_name.lower() or 'products' in table_name.lower():
            return "dimension"
        else:
            return "dimension"  # Default to dimension
    
    def _analyze_table_batch(self, table_batch: Dict[str, List[FieldInfo]]) -> Dict[str, EnrichedFieldInfo]:
        """Analyze a batch of related tables together for efficiency."""
        
        batch_table_names = list(table_batch.keys())
        self.logger.debug(f"Analyzing table batch: {batch_table_names}")
        
        if len(table_batch) == 1:
            # Single table in batch - use original method
            table_name = batch_table_names[0]
            fields = table_batch[table_name]
            return self._analyze_table_fields(table_name, fields)
        else:
            # Multiple tables in batch - use new batch method
            return self._analyze_multiple_tables_batch(table_batch)
    
    def _analyze_multiple_tables_batch(self, table_batch: Dict[str, List[FieldInfo]]) -> Dict[str, EnrichedFieldInfo]:
        """Analyze multiple related tables in a single API call."""
        
        # Prepare batch metadata for analysis
        batch_metadata = {}
        all_fields = []
        
        for table_name, fields in table_batch.items():
            table_fields_metadata = []
            for field in fields:
                field_metadata = {
                    "name": field.name,
                    "qualified_name": field.qualified_name,
                    "type": field.field_type,
                    "lookml_type": field.lookml_type,
                    "sql_expression": field.sql_expression,
                    "lookml_description": field.lookml_description,
                    "bigquery_description": field.bigquery_description,
                    "bigquery_data_type": field.bigquery_data_type
                }
                table_fields_metadata.append(field_metadata)
                all_fields.append(field)
            
            batch_metadata[table_name] = table_fields_metadata
        
        # Analyze batch with Gemini
        analysis_prompt = self._build_batch_field_analysis_prompt(batch_metadata)
        
        try:
            response = self.gemini_service.generate_content(
                prompt=analysis_prompt,
                temperature=0.1  # Lower temperature for consistent analysis
            )
            
            # Parse Gemini's response for the batch
            batch_analysis = self._parse_batch_field_analysis_response(response.content, all_fields)
            return batch_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing table batch {list(table_batch.keys())}: {e}")
            # Fallback: analyze each table individually
            fallback_results = {}
            for table_name, fields in table_batch.items():
                try:
                    table_results = self._analyze_table_fields(table_name, fields)
                    fallback_results.update(table_results)
                except Exception as table_error:
                    self.logger.error(f"Fallback analysis failed for {table_name}: {table_error}")
                    # Create basic fallback for this table
                    fallback_results.update(self._create_fallback_field_analysis(fields))
            
            return fallback_results
    
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
    
    def _build_batch_field_analysis_prompt(self, batch_metadata: Dict[str, List[Dict]]) -> str:
        """Build prompt for analyzing multiple tables together in a batch."""
        
        tables_info = []
        for table_name, field_metadata in batch_metadata.items():
            table_info = f"TABLE: {table_name}\nFIELDS:\n{json.dumps(field_metadata, indent=2)}"
            tables_info.append(table_info)
        
        tables_section = "\n\n".join(tables_info)
        
        return f"""
Analyze the following related database tables and their fields together to understand their semantic relationships:

{tables_section}

For each field in each table, determine:
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

6. RELATED_FIELDS: List of other field names (from any table) that are commonly used together

IMPORTANT: Consider relationships between tables when analyzing fields. 
For example, if one table has transaction data and another has product details, 
note how they might be joined and used together in business queries.

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
- Consider table relationships when recommending related fields
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
    
    def _parse_batch_field_analysis_response(self, response: str, all_fields: List[FieldInfo]) -> Dict[str, EnrichedFieldInfo]:
        """Parse Gemini's batch field analysis response."""
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
            field_map = {f.qualified_name: f for f in all_fields}
            
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
            self.logger.error(f"Error parsing batch field analysis response: {e}")
            return self._create_fallback_field_analysis(all_fields)
    
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
    
    def clear_cache(self) -> None:
        """Clear persistent schema intelligence cache."""
        self.persistent_storage.clear_cache()
        self.logger.info("Schema intelligence persistent cache cleared")
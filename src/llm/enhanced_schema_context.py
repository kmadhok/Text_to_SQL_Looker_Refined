"""Enhanced schema context generator using dynamic semantic intelligence."""

import logging
import time
from typing import Dict, List, Optional, Set, Tuple, Any

from .schema_intelligence import SchemaIntelligenceService, SchemaIntelligence, FieldSemanticType, TableBusinessType
from ..grounding.index import GroundingIndex

logger = logging.getLogger(__name__)


class EnhancedSchemaContextGenerator:
    """Enhanced schema context generator using semantic intelligence."""
    
    def __init__(self, schema_intelligence_service: SchemaIntelligenceService):
        """Initialize enhanced context generator.
        
        Args:
            schema_intelligence_service: Service for semantic schema analysis
        """
        self.intelligence_service = schema_intelligence_service
        self.logger = logging.getLogger(__name__)
        self._schema_intelligence_cache: Optional[SchemaIntelligence] = None
    
    def generate_intelligent_context(
        self, 
        grounding_index: GroundingIndex,
        query_terms: List[str], 
        full_query: str,
        max_size: int = 12000
    ) -> str:
        """Generate intelligent schema context based on semantic understanding.
        
        Args:
            grounding_index: Grounding index with schema information
            query_terms: Extracted terms from user query
            full_query: Complete user query
            max_size: Maximum context size in characters
            
        Returns:
            Rich, semantically-aware schema context
        """
        start_time = time.time()
        
        self.logger.info(f"Generating intelligent context for query: {full_query}")
        
        # Get or generate schema intelligence
        if not self._schema_intelligence_cache:
            self._schema_intelligence_cache = self.intelligence_service.analyze_schema(grounding_index)
        
        schema_intelligence = self._schema_intelligence_cache
        
        # Determine query intent and select relevant components
        query_intent = self._analyze_query_intent(full_query, query_terms, schema_intelligence)
        
        # Select most relevant explores based on semantic understanding
        relevant_explores = self._select_relevant_explores_intelligently(
            query_intent, schema_intelligence, max_explores=3
        )
        
        # Build multi-layered context
        context_parts = []
        
        # Layer 1: Query-specific overview
        context_parts.append(self._generate_intelligent_overview(query_intent, relevant_explores, schema_intelligence))
        
        # Layer 2: Semantically enriched table schemas
        for explore_name, relevance_score in relevant_explores:
            context_parts.append(self._generate_enriched_explore_schema(explore_name, schema_intelligence))
        
        # Layer 3: Business logic guidance
        context_parts.append(self._generate_business_logic_guidance(query_intent, schema_intelligence))
        
        # Layer 4: Query-specific warnings and recommendations
        context_parts.append(self._generate_contextual_warnings(query_intent, relevant_explores, schema_intelligence))
        
        full_context = "\n\n".join(context_parts)
        
        # Truncate if necessary
        if len(full_context) > max_size:
            self.logger.warning(f"Context ({len(full_context)} chars) exceeds max size ({max_size}). Truncating.")
            full_context = full_context[:max_size - 100] + "\n\n... (truncated for length)"
        
        generation_time = time.time() - start_time
        self.logger.info(f"Generated intelligent context in {generation_time:.2f}s - {len(full_context)} chars")
        
        return full_context
    
    def _analyze_query_intent(self, full_query: str, query_terms: List[str], schema_intelligence: SchemaIntelligence) -> Dict[str, Any]:
        """Analyze the intent behind the user's query."""
        query_lower = full_query.lower()
        intent = {
            "type": "general",
            "primary_concepts": [],
            "requires_aggregation": False,
            "requires_joins": False,
            "requires_time_filtering": False,
            "semantic_focus": []
        }
        
        # Detect query types
        if any(term in query_lower for term in ['revenue', 'sales', 'money', 'earnings', 'income']):
            intent["type"] = "revenue_analysis"
            intent["primary_concepts"].append("revenue")
            intent["requires_aggregation"] = True
            intent["semantic_focus"].append(FieldSemanticType.TRANSACTIONAL_VALUE)
        
        elif any(term in query_lower for term in ['customer', 'user', 'people']):
            intent["type"] = "customer_analysis"
            intent["primary_concepts"].append("customer")
            intent["semantic_focus"].append(FieldSemanticType.IDENTIFIER)
        
        elif any(term in query_lower for term in ['product', 'item']):
            intent["type"] = "product_analysis"
            intent["primary_concepts"].append("product")
        
        elif any(term in query_lower for term in ['count', 'total', 'sum', 'average']):
            intent["requires_aggregation"] = True
        
        # Detect time-based requirements
        if any(term in query_lower for term in ['month', 'year', 'day', 'time', 'last', 'this', 'trend']):
            intent["requires_time_filtering"] = True
            intent["semantic_focus"].append(FieldSemanticType.TEMPORAL)
        
        # Detect join requirements
        if len(intent["primary_concepts"]) > 1 or any(term in query_lower for term in ['by', 'per', 'breakdown']):
            intent["requires_joins"] = True
        
        return intent
    
    def _select_relevant_explores_intelligently(
        self, 
        query_intent: Dict[str, Any], 
        schema_intelligence: SchemaIntelligence,
        max_explores: int = 3
    ) -> List[Tuple[str, float]]:
        """Select explores based on semantic understanding rather than name matching."""
        explore_scores = {}
        
        for explore_name, table_insights in schema_intelligence.relationship_insights.items():
            score = 0.0
            
            # Get table semantics for the base table
            base_table = None
            for insight in table_insights:
                if insight.startswith("Base table"):
                    base_table = insight.split("'")[1]
                    break
            
            if base_table and base_table in schema_intelligence.table_semantics:
                table_semantics = schema_intelligence.table_semantics[base_table]
                
                # Score based on business type alignment
                if query_intent["type"] == "revenue_analysis" and table_semantics.business_type == TableBusinessType.FACT:
                    score += 50.0
                elif query_intent["type"] in ["customer_analysis", "product_analysis"] and table_semantics.business_type == TableBusinessType.DIMENSION:
                    score += 30.0
                
                # Score based on key concepts
                for concept in query_intent["primary_concepts"]:
                    if concept in table_semantics.key_concepts:
                        score += 25.0
                
                # Score based on query type suitability
                for best_query in table_semantics.best_for_queries:
                    if query_intent["type"] in best_query.lower():
                        score += 20.0
            
            # Score based on available fields with relevant semantic types
            for qualified_name, enriched_field in schema_intelligence.enriched_fields.items():
                if enriched_field.field_info.view_name == base_table:
                    for semantic_focus in query_intent["semantic_focus"]:
                        if enriched_field.semantic_type == semantic_focus:
                            score += 10.0
            
            if score > 0:
                explore_scores[explore_name] = score
        
        # If no high-scoring explores, fall back to business concept mapping
        if not explore_scores:
            for concept in query_intent["primary_concepts"]:
                if concept in schema_intelligence.business_concept_map:
                    relevant_fields = schema_intelligence.business_concept_map[concept]
                    for field_name in relevant_fields:
                        enriched_field = schema_intelligence.enriched_fields.get(field_name)
                        if enriched_field:
                            table_name = enriched_field.field_info.view_name
                            for explore_name, insights in schema_intelligence.relationship_insights.items():
                                if table_name in str(insights):
                                    explore_scores[explore_name] = explore_scores.get(explore_name, 0) + 5.0
        
        # Sort and return top explores
        sorted_explores = sorted(explore_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_explores[:max_explores]
    
    def _generate_intelligent_overview(
        self, 
        query_intent: Dict[str, Any], 
        relevant_explores: List[Tuple[str, float]], 
        schema_intelligence: SchemaIntelligence
    ) -> str:
        """Generate intelligent overview based on query intent."""
        overview = ["# Intelligent Schema Context"]
        
        # Query intent summary
        overview.append(f"Query Type: {query_intent['type'].replace('_', ' ').title()}")
        
        if query_intent["primary_concepts"]:
            overview.append(f"Primary Business Concepts: {', '.join(query_intent['primary_concepts'])}")
        
        # Selected explores with reasoning
        overview.append(f"\n## Selected Data Sources (Top {len(relevant_explores)}):")
        for explore_name, score in relevant_explores:
            overview.append(f"- **{explore_name}** (relevance: {score:.1f})")
            
            # Add business context
            if explore_name in schema_intelligence.relationship_insights:
                insights = schema_intelligence.relationship_insights[explore_name]
                for insight in insights[:2]:  # First 2 insights
                    overview.append(f"  • {insight}")
        
        return "\n".join(overview)
    
    def _generate_enriched_explore_schema(self, explore_name: str, schema_intelligence: SchemaIntelligence) -> str:
        """Generate enriched schema for an explore with semantic annotations."""
        schema = [f"# Explore: {explore_name}"]
        
        # Add relationship insights
        if explore_name in schema_intelligence.relationship_insights:
            insights = schema_intelligence.relationship_insights[explore_name]
            schema.append("\n## Business Context:")
            for insight in insights:
                schema.append(f"- {insight}")
        
        # Group fields by semantic type for better organization
        fields_by_semantic_type = {}
        for qualified_name, enriched_field in schema_intelligence.enriched_fields.items():
            # Check if this field belongs to tables in this explore
            field_table = enriched_field.field_info.view_name
            if any(field_table in insight for insight in schema_intelligence.relationship_insights.get(explore_name, [])):
                semantic_type = enriched_field.semantic_type
                if semantic_type not in fields_by_semantic_type:
                    fields_by_semantic_type[semantic_type] = []
                fields_by_semantic_type[semantic_type].append(enriched_field)
        
        # Display fields by semantic category
        semantic_type_labels = {
            FieldSemanticType.TRANSACTIONAL_VALUE: "💰 Transaction Values (Actual Revenue/Money)",
            FieldSemanticType.REFERENCE_PRICE: "📋 Reference Prices (Catalog/Listed Prices)",
            FieldSemanticType.QUANTITY: "📊 Quantities & Counts",
            FieldSemanticType.IDENTIFIER: "🔑 Identifiers & Keys",
            FieldSemanticType.TEMPORAL: "📅 Date & Time Fields",
            FieldSemanticType.CATEGORICAL: "🏷️ Categories & Classifications",
            FieldSemanticType.DESCRIPTIVE: "📝 Names & Descriptions",
            FieldSemanticType.CALCULATED: "🧮 Calculated Measures"
        }
        
        for semantic_type, fields in fields_by_semantic_type.items():
            if fields:
                label = semantic_type_labels.get(semantic_type, semantic_type.value)
                schema.append(f"\n## {label}:")
                
                # Sort by confidence and limit to top fields
                fields.sort(key=lambda f: f.confidence_score, reverse=True)
                for field in fields[:5]:  # Top 5 fields per category
                    field_desc = f"- **{field.field_info.qualified_name}** "
                    field_desc += f"({field.field_info.lookml_type or field.field_info.bigquery_data_type})"
                    
                    if field.confidence_score < 0.7:
                        field_desc += f" [confidence: {field.confidence_score:.1f}]"
                    
                    schema.append(field_desc)
                    schema.append(f"  • {field.business_purpose}")
                    
                    if field.usage_recommendations:
                        schema.append(f"  • Usage: {field.usage_recommendations[0]}")
                    
                    if field.common_mistakes:
                        schema.append(f"  • ⚠️ Avoid: {field.common_mistakes[0]}")
        
        return "\n".join(schema)
    
    def _generate_business_logic_guidance(self, query_intent: Dict[str, Any], schema_intelligence: SchemaIntelligence) -> str:
        """Generate business logic guidance based on query intent."""
        guidance = ["# Business Logic Guidance"]
        
        # Query-specific patterns
        query_type = query_intent["type"]
        if query_type in schema_intelligence.query_patterns:
            pattern_info = schema_intelligence.query_patterns[query_type]
            guidance.append(f"\n## {pattern_info['description']}")
            
            if "primary_fields" in pattern_info:
                guidance.append(f"**Primary Fields:** {', '.join(pattern_info['primary_fields'][:3])}")
            
            if "guidance" in pattern_info:
                guidance.append("**Key Guidelines:**")
                for guide in pattern_info["guidance"]:
                    guidance.append(f"- {guide}")
        
        # Add semantic-specific guidance
        if FieldSemanticType.TRANSACTIONAL_VALUE in query_intent.get("semantic_focus", []):
            guidance.append("\n## ⚠️ CRITICAL: Revenue Field Selection")
            guidance.append("- ALWAYS use TRANSACTIONAL_VALUE fields for actual revenue calculations")
            guidance.append("- NEVER use REFERENCE_PRICE fields for revenue (these are catalog prices)")
            guidance.append("- Look for fields marked with 💰 symbol - these represent actual money exchanged")
        
        return "\n".join(guidance)
    
    def _generate_contextual_warnings(
        self, 
        query_intent: Dict[str, Any], 
        relevant_explores: List[Tuple[str, float]], 
        schema_intelligence: SchemaIntelligence
    ) -> str:
        """Generate contextual warnings based on query analysis."""
        warnings = ["# Contextual Warnings & Recommendations"]
        
        # Check for potential semantic mistakes
        if query_intent["type"] == "revenue_analysis":
            # Find reference price fields that might be confused for revenue
            reference_price_fields = []
            for qualified_name, enriched_field in schema_intelligence.enriched_fields.items():
                if enriched_field.semantic_type == FieldSemanticType.REFERENCE_PRICE:
                    reference_price_fields.append(qualified_name)
            
            if reference_price_fields:
                warnings.append("\n## ⚠️ Common Revenue Calculation Mistakes:")
                warnings.append("**DO NOT USE these fields for revenue calculations:**")
                for field in reference_price_fields[:3]:
                    warnings.append(f"- ❌ {field} (this is a catalog/listed price, not actual revenue)")
        
        # Table selection warnings
        fact_tables = []
        dimension_tables = []
        for explore_name, _ in relevant_explores:
            base_table = None
            insights = schema_intelligence.relationship_insights.get(explore_name, [])
            for insight in insights:
                if "FACT table" in insight:
                    fact_tables.append(explore_name)
                elif "DIMENSION table" in insight:
                    dimension_tables.append(explore_name)
        
        if query_intent["requires_aggregation"] and not fact_tables:
            warnings.append("\n## ⚠️ Aggregation Warning:")
            warnings.append("- Your query requires aggregation but selected tables are primarily dimensional")
            warnings.append("- Consider including fact tables that contain measurable/transactional data")
        
        if query_intent["requires_joins"] and len(relevant_explores) == 1:
            warnings.append("\n## 💡 Join Recommendation:")
            warnings.append("- Your query may benefit from joining additional tables")
            warnings.append("- Review available joins in the selected explore")
        
        return "\n".join(warnings)
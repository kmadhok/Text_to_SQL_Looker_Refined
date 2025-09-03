"""LLM-powered query planner using Gemini 2.5 Pro."""

import logging
import uuid
import re
from typing import Optional, List, Dict, Any

from .gemini_service import GeminiService, LLMResponse
from .schema_context import SchemaContextGenerator
from .schema_intelligence import SchemaIntelligenceService
from .enhanced_schema_context import EnhancedSchemaContextGenerator
from ..grounding.index import GroundingIndex
from ..generator.planner import QueryPlan
from ..generator.validator import SQLValidator

logger = logging.getLogger(__name__)


class LLMQueryPlanner:
    """LLM-powered query planner that generates SQL using Gemini 2.5 Pro."""
    
    def __init__(
        self,
        grounding_index: GroundingIndex,
        gemini_service: Optional[GeminiService] = None,
        validator: Optional[SQLValidator] = None,
        max_retries: int = 3,
        conversation_log_dir: Optional[str] = None,
        use_enhanced_context: bool = True
    ):
        """Initialize LLM query planner.
        
        Args:
            grounding_index: Grounding index for schema information
            gemini_service: Gemini service instance (will create if not provided)
            validator: SQL validator for schema validation
            max_retries: Maximum retry attempts for self-correction
            conversation_log_dir: Directory to save conversation logs
            use_enhanced_context: Whether to use enhanced semantic context generation
        """
        self.grounding_index = grounding_index
        self.validator = validator
        self.max_retries = max_retries
        self.use_enhanced_context = use_enhanced_context
        
        # Initialize Gemini service
        if gemini_service:
            self.gemini_service = gemini_service
        else:
            self.gemini_service = GeminiService(
                conversation_log_dir=conversation_log_dir,
                debug_mode=True
            )
        
        # Initialize context generators
        self.schema_generator = SchemaContextGenerator(grounding_index)  # Fallback
        
        if use_enhanced_context:
            self.schema_intelligence_service = SchemaIntelligenceService(self.gemini_service)
            self.enhanced_context_generator = EnhancedSchemaContextGenerator(self.schema_intelligence_service)
            logger.info("Initialized LLM query planner with enhanced semantic context")
        else:
            logger.info("Initialized LLM query planner with basic context")
        
        logger.info(f"Max retries: {max_retries}")
    
    def plan_query(self, query: str, default_limit: int = 100) -> Optional[QueryPlan]:
        """Plan SQL generation from natural language query using LLM.
        
        Args:
            query: Natural language query
            default_limit: Default limit for results
            
        Returns:
            QueryPlan object or None if planning fails
        """
        conversation_id = str(uuid.uuid4())[:8]
        
        logger.info(f"Planning query with LLM [{conversation_id}]: {query}")
        
        # Extract basic query terms for focused context
        query_terms = self._extract_query_terms(query)
        
        # Generate schema context
        schema_context = self._generate_schema_context(query_terms, query)
        
        # Attempt to generate SQL with retries
        for attempt in range(self.max_retries + 1):
            try:
                # Generate SQL with Gemini
                llm_response = self.gemini_service.generate_sql(
                    user_query=query,
                    schema_context=schema_context,
                    conversation_id=f"{conversation_id}_attempt_{attempt + 1}"
                )
                
                if not llm_response.content.strip():
                    logger.warning(f"Empty response from LLM on attempt {attempt + 1}")
                    continue
                
                # Extract SQL from response
                sql = self._extract_sql(llm_response.content)
                if not sql:
                    logger.warning(f"No SQL found in LLM response on attempt {attempt + 1}")
                    continue
                
                # Validate SQL using schema information
                validation_result = self._validate_sql(sql)
                
                if validation_result["valid"]:
                    # Convert to QueryPlan
                    query_plan = self._sql_to_query_plan(
                        sql=sql,
                        query=query,
                        validation_result=validation_result,
                        llm_response=llm_response
                    )
                    
                    if query_plan:
                        logger.info(
                            f"Successfully generated query plan [{conversation_id}] "
                            f"on attempt {attempt + 1} - "
                            f"Explore: {query_plan.explore_name}, "
                            f"Fields: {len(query_plan.selected_fields)}"
                        )
                        return query_plan
                
                else:
                    # Validation failed - prepare for retry
                    logger.warning(
                        f"SQL validation failed on attempt {attempt + 1}: {validation_result['errors']}"
                    )
                    
                    if attempt < self.max_retries:
                        # Update schema context with validation errors for self-correction
                        schema_context = self._add_validation_feedback(
                            schema_context,
                            validation_result,
                            sql,
                            attempt + 1
                        )
                        logger.info(f"Retrying with validation feedback...")
                    
            except Exception as e:
                logger.error(f"Error on attempt {attempt + 1}: {e}")
                
                if attempt < self.max_retries:
                    continue
        
        logger.error(f"Failed to generate valid query plan after {self.max_retries + 1} attempts")
        return None
    
    def _extract_query_terms(self, query: str) -> List[str]:
        """Extract meaningful terms from query."""
        terms = re.findall(r'\b[a-zA-Z_]\w*\b', query.lower())
        
        stop_words = {
            'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
            'by', 'from', 'up', 'about', 'into', 'over', 'after', 'what', 'when',
            'where', 'how', 'show', 'get', 'find', 'list', 'give', 'me', 'i', 'want',
            'need', 'can', 'you', 'please', 'has', 'have', 'is', 'are', 'that', 'this'
        }
        
        meaningful_terms = [term for term in terms if term not in stop_words and len(term) > 2]
        logger.debug(f"Extracted query terms: {meaningful_terms}")
        return meaningful_terms
    
    def _generate_schema_context(self, query_terms: List[str], full_query: str) -> str:
        """Generate rich schema context for the query."""
        logger.debug("Generating schema context for LLM")
        
        try:
            if self.use_enhanced_context and hasattr(self, 'enhanced_context_generator'):
                # Use enhanced semantic context
                context = self.enhanced_context_generator.generate_intelligent_context(
                    grounding_index=self.grounding_index,
                    query_terms=query_terms,
                    full_query=full_query,
                    max_size=12000
                )
                logger.info("Using enhanced semantic context generation")
            else:
                # Fallback to original context generation
                if query_terms:
                    context = self.schema_generator.generate_focused_context(
                        query_terms=query_terms,
                        max_size=12000
                    )
                else:
                    context = self.schema_generator.generate_full_context(max_size=12000)
                
                # Add query-specific guidance
                context += self._add_query_specific_guidance(full_query)
                logger.info("Using basic context generation")
                
        except Exception as e:
            logger.warning(f"Enhanced context generation failed: {e}. Falling back to basic context.")
            # Fallback to original method
            if query_terms:
                context = self.schema_generator.generate_focused_context(
                    query_terms=query_terms,
                    max_size=12000
                )
            else:
                context = self.schema_generator.generate_full_context(max_size=12000)
            
            context += self._add_query_specific_guidance(full_query)
        
        return context
    
    def _add_query_specific_guidance(self, query: str) -> str:
        """Add query-specific guidance to schema context."""
        guidance = ["\n# Query-Specific Guidance"]
        
        query_lower = query.lower()
        
        # Detect ranking queries
        ranking_keywords = ['highest', 'lowest', 'maximum', 'minimum', 'top', 'bottom', 'best', 'worst']
        if any(keyword in query_lower for keyword in ranking_keywords):
            guidance.append("- This is a RANKING query. Use ORDER BY with appropriate direction and LIMIT.")
            if any(word in query_lower for word in ['highest', 'maximum', 'top', 'best']):
                guidance.append("- Use ORDER BY field DESC for highest/maximum/top/best")
            if any(word in query_lower for word in ['lowest', 'minimum', 'bottom', 'worst']):
                guidance.append("- Use ORDER BY field ASC for lowest/minimum/bottom/worst")
        
        # Detect "what X has" pattern
        if re.search(r'what\s+\w+\s+(has|have|with)', query_lower):
            guidance.append("- This asks for entity identification. Include identifying fields (id, name, brand).")
            guidance.append("- May need joins to connect data tables with entity details.")
        
        # Detect aggregation needs
        if any(word in query_lower for word in ['count', 'total', 'sum', 'average', 'avg']):
            guidance.append("- Use appropriate aggregate functions: COUNT(), SUM(), AVG()")
            guidance.append("- Include GROUP BY if aggregating by categories/dimensions")
        
        # Revenue/price-related queries - only add if not using enhanced context
        if any(term in query_lower for term in ['revenue', 'sales', 'sale price', 'price', 'money', 'earnings', 'income']):
            guidance.append("- For REVENUE calculations, use actual transaction value fields")
            guidance.append("- Avoid using catalog/listed price fields for revenue calculations")
            guidance.append("- Look for fields that represent actual money received from transactions")
        
        return "\n".join(guidance) if len(guidance) > 1 else ""
    
    def _extract_sql(self, llm_response: str) -> Optional[str]:
        """Extract SQL query from LLM response."""
        # Remove markdown code blocks if present
        sql = llm_response.strip()
        
        # Remove ```sql and ``` markers
        sql = re.sub(r'^```sql\s*\n?', '', sql, flags=re.MULTILINE | re.IGNORECASE)
        sql = re.sub(r'^```\s*\n?', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'\n```$', '', sql, flags=re.MULTILINE)
        
        # Clean up the SQL
        sql = sql.strip()
        
        # Basic validation that it looks like SQL
        if not sql:
            return None
            
        # Must contain SELECT
        if 'select' not in sql.lower():
            return None
            
        logger.debug(f"Extracted SQL: {sql[:200]}...")
        return sql
    
    def _validate_sql(self, sql: str) -> Dict[str, Any]:
        """Validate SQL using schema information."""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "extracted_tables": [],
            "extracted_fields": [],
            "suggested_corrections": []
        }
        
        try:
            # Extract table names from SQL
            table_pattern = r'`([^`]+\.)?([^`]+)`'
            table_matches = re.findall(table_pattern, sql)
            extracted_tables = [match[1] for match in table_matches if match[1]]
            validation_result["extracted_tables"] = extracted_tables
            
            # Extract field references
            field_pattern = r'(\w+)\.(\w+)'
            field_matches = re.findall(field_pattern, sql)
            extracted_fields = [f"{match[0]}.{match[1]}" for match in field_matches]
            validation_result["extracted_fields"] = extracted_fields
            
            # Check if tables exist in our schema
            all_views = self.grounding_index.lookml_project.get_all_views()
            valid_tables = set(all_views.keys())
            
            for table in extracted_tables:
                table_short = table.split('.')[-1]  # Get table name without dataset
                if table_short not in valid_tables:
                    validation_result["valid"] = False
                    validation_result["errors"].append(f"Unknown table: {table}")
                    # Suggest similar tables
                    similar = [t for t in valid_tables if t in table_short or table_short in t]
                    if similar:
                        validation_result["suggested_corrections"].append(f"Did you mean: {similar[0]}?")
            
            # Check if fields exist
            all_explores = self.grounding_index.explores
            all_available_fields = set()
            for explore in all_explores.values():
                all_available_fields.update(explore.available_fields.keys())
            
            for field in extracted_fields:
                if field not in all_available_fields:
                    # Check if just the field name exists somewhere
                    field_name = field.split('.')[-1]
                    matching_fields = [f for f in all_available_fields if f.endswith(f".{field_name}")]
                    if not matching_fields:
                        validation_result["warnings"].append(f"Field not found: {field}")
                    else:
                        validation_result["suggested_corrections"].append(
                            f"Field {field} not found. Did you mean: {matching_fields[0]}?"
                        )
            
            # Use SQL validator if available
            if self.validator:
                try:
                    is_valid, error_msg = self.validator.validate_sql(sql)
                    if not is_valid:
                        validation_result["valid"] = False
                        validation_result["errors"].append(f"SQL syntax error: {error_msg}")
                except Exception as e:
                    validation_result["warnings"].append(f"SQL validator error: {e}")
            
        except Exception as e:
            validation_result["valid"] = False
            validation_result["errors"].append(f"Validation error: {e}")
        
        logger.debug(f"SQL validation result: {validation_result}")
        return validation_result
    
    def _add_validation_feedback(
        self,
        original_context: str,
        validation_result: Dict[str, Any],
        failed_sql: str,
        attempt: int
    ) -> str:
        """Add validation feedback to context for self-correction."""
        feedback = [
            f"\n# Self-Correction Attempt {attempt}",
            "The previous SQL generation attempt failed validation. Please fix the following issues:",
            ""
        ]
        
        if validation_result["errors"]:
            feedback.append("## Errors:")
            for error in validation_result["errors"]:
                feedback.append(f"- {error}")
            feedback.append("")
        
        if validation_result["suggested_corrections"]:
            feedback.append("## Suggested Corrections:")
            for correction in validation_result["suggested_corrections"]:
                feedback.append(f"- {correction}")
            feedback.append("")
        
        if validation_result["warnings"]:
            feedback.append("## Warnings:")
            for warning in validation_result["warnings"]:
                feedback.append(f"- {warning}")
            feedback.append("")
        
        feedback.append("## Previous Failed SQL:")
        feedback.append(f"```sql\n{failed_sql}\n```")
        feedback.append("")
        feedback.append("Please generate corrected SQL addressing these issues.")
        
        return original_context + "\n".join(feedback)
    
    def _sql_to_query_plan(
        self,
        sql: str,
        query: str,
        validation_result: Dict[str, Any],
        llm_response: LLMResponse
    ) -> Optional[QueryPlan]:
        """Convert SQL to QueryPlan format for compatibility."""
        try:
            # This is a simplified conversion - the LLM generates the actual SQL
            # We just need to create a QueryPlan structure for compatibility
            
            # Try to determine the main explore/table
            extracted_tables = validation_result.get("extracted_tables", [])
            main_table = None
            
            if extracted_tables:
                # Find the most relevant explore
                table_short = extracted_tables[0].split('.')[-1]
                for explore_name, explore_info in self.grounding_index.explores.items():
                    if explore_info.base_view == table_short:
                        main_table = explore_name
                        break
            
            if not main_table:
                # Fallback to first available explore
                main_table = list(self.grounding_index.explores.keys())[0] if self.grounding_index.explores else None
            
            if not main_table:
                logger.error("Could not determine main table/explore from SQL")
                return None
            
            # Extract fields mentioned in SQL
            extracted_fields = validation_result.get("extracted_fields", [])
            
            # Create a simplified QueryPlan
            # Note: The actual SQL generation is done by LLM, not rule-based system
            query_plan = QueryPlan(
                explore_name=main_table,
                selected_fields=[],  # We'll populate this with dummy field info
                required_joins=set(),
                filters=[],
                limit=100,  # Default limit
                has_aggregation="count" in sql.lower() or "sum" in sql.lower() or "avg" in sql.lower()
            )
            
            # Store the actual LLM-generated SQL for use by SQL builder
            query_plan.llm_generated_sql = sql
            query_plan.llm_response = llm_response
            
            logger.debug(f"Created QueryPlan for explore: {main_table}")
            return query_plan
            
        except Exception as e:
            logger.error(f"Error converting SQL to QueryPlan: {e}")
            return None
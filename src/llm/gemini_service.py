"""Gemini 2.5 Pro service for SQL generation with comprehensive logging."""

import logging
import os
import time
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pathlib import Path

import google.generativeai as genai
from google.generativeai import GenerationConfig
from google.generativeai.types import HarmCategory, HarmBlockThreshold

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Response from LLM with metadata."""
    content: str
    reasoning: Optional[str] = None
    token_usage: Optional[Dict[str, int]] = None
    processing_time: float = 0.0
    model_name: str = ""
    temperature: float = 0.0
    prompt_token_count: int = 0
    response_token_count: int = 0
    total_cost_estimate: float = 0.0


@dataclass 
class LLMContext:
    """Context sent to LLM with metadata."""
    system_prompt: str
    user_prompt: str
    schema_context: str
    total_context_size: int
    timestamp: str


class GeminiService:
    """Service for interacting with Gemini 2.5 Pro with comprehensive logging."""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "gemini-2.5-pro",
        temperature: float = 0.1,
        debug_mode: bool = True,
        conversation_log_dir: Optional[str] = None
    ):
        """Initialize Gemini service.
        
        Args:
            api_key: Gemini API key (will use GEMINI_API_KEY env var if not provided)
            model_name: Model to use
            temperature: Generation temperature
            debug_mode: Enable detailed logging
            conversation_log_dir: Directory to save conversation logs
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        self.model_name = model_name
        self.temperature = temperature
        self.debug_mode = debug_mode
        
        # Configure Gemini
        genai.configure(api_key=self.api_key)
        
        # Initialize model
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            generation_config=GenerationConfig(
                temperature=temperature,
                top_p=0.95,
                top_k=40,
                max_output_tokens=4096,
            ),
            safety_settings={
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
        )
        
        # Set up conversation logging
        self.conversation_log_dir = None
        if conversation_log_dir:
            self.conversation_log_dir = Path(conversation_log_dir)
            self.conversation_log_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized Gemini service with model: {model_name}, temperature: {temperature}")
    
    def generate_content(
        self,
        prompt: str,
        temperature: Optional[float] = None,
        max_output_tokens: Optional[int] = None
    ) -> LLMResponse:
        """Generate content from a prompt using Gemini.
        
        Args:
            prompt: Input prompt
            temperature: Optional temperature override
            max_output_tokens: Optional max output tokens override
            
        Returns:
            LLMResponse with generated content and metadata
        """
        start_time = time.time()
        
        try:
            # Use provided temperature or instance default
            temp = temperature if temperature is not None else self.temperature
            max_tokens = max_output_tokens or 8192
            
            logger.info(f"Generating content with Gemini - Prompt size: {len(prompt)} chars")
            
            # Configure generation
            config = GenerationConfig(
                temperature=temp,
                top_p=0.8,
                top_k=40,
                max_output_tokens=max_tokens,
                stop_sequences=[]
            )
            
            # Safety settings
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            }
            
            # Generate response
            response = self.model.generate_content(
                prompt,
                generation_config=config,
                safety_settings=safety_settings
            )
            
            processing_time = time.time() - start_time
            
            # Extract content
            response_content = response.text if response.text else ""
            
            # Extract token usage
            prompt_tokens = response.usage_metadata.prompt_token_count if response.usage_metadata else 0
            response_tokens = response.usage_metadata.candidates_token_count if response.usage_metadata else 0
            
            token_usage = {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": response_tokens,
                "total_tokens": prompt_tokens + response_tokens
            }
            
            # Estimate cost
            cost_per_input_token = 0.00000375  # $3.75 per 1M input tokens
            cost_per_output_token = 0.000015   # $15 per 1M output tokens
            total_cost = (prompt_tokens * cost_per_input_token) + (response_tokens * cost_per_output_token)
            
            # Create response object
            llm_response = LLMResponse(
                content=response_content,
                token_usage=token_usage,
                processing_time=processing_time,
                model_name=self.model_name,
                temperature=temp,
                prompt_token_count=prompt_tokens,
                response_token_count=response_tokens,
                total_cost_estimate=total_cost
            )
            
            logger.info(
                f"Content generated - "
                f"Time: {processing_time:.2f}s, "
                f"Tokens: {prompt_tokens + response_tokens}, "
                f"Cost: ${total_cost:.6f}"
            )
            
            return llm_response
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error generating content with Gemini: {e}")
            
            # Return error response
            return LLMResponse(
                content="",
                processing_time=processing_time,
                model_name=self.model_name,
                temperature=temp if 'temp' in locals() else self.temperature
            )
    
    def generate_sql(
        self,
        user_query: str,
        schema_context: str,
        conversation_id: Optional[str] = None
    ) -> LLMResponse:
        """Generate SQL from natural language query with comprehensive logging.
        
        Args:
            user_query: Natural language query
            schema_context: Rich schema context
            conversation_id: Optional conversation ID for logging
            
        Returns:
            LLMResponse with generated SQL and metadata
        """
        start_time = time.time()
        
        # Build prompts
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(user_query, schema_context)
        
        # Create context object for logging
        context = LLMContext(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_context=schema_context,
            total_context_size=len(system_prompt) + len(user_prompt),
            timestamp=time.strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Log context if debug mode
        if self.debug_mode:
            self._log_context(context, conversation_id)
        
        try:
            # Generate response
            full_prompt = f"{system_prompt}\n\n{user_prompt}"
            
            logger.info(f"Sending request to Gemini - Context size: {context.total_context_size} chars")
            
            response = self.model.generate_content(full_prompt)
            
            processing_time = time.time() - start_time
            
            # Extract response content
            response_content = response.text if response.text else ""
            
            # Parse token usage if available
            token_usage = {}
            prompt_tokens = 0
            response_tokens = 0
            
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                usage = response.usage_metadata
                prompt_tokens = getattr(usage, 'prompt_token_count', 0)
                response_tokens = getattr(usage, 'candidates_token_count', 0)
                token_usage = {
                    'prompt_tokens': prompt_tokens,
                    'completion_tokens': response_tokens,
                    'total_tokens': prompt_tokens + response_tokens
                }
            
            # Estimate cost (approximate pricing for Gemini 2.5 Pro)
            cost_per_input_token = 0.00000375  # $3.75 per 1M input tokens
            cost_per_output_token = 0.000015   # $15 per 1M output tokens
            total_cost = (prompt_tokens * cost_per_input_token) + (response_tokens * cost_per_output_token)
            
            # Create response object
            llm_response = LLMResponse(
                content=response_content,
                token_usage=token_usage,
                processing_time=processing_time,
                model_name=self.model_name,
                temperature=self.temperature,
                prompt_token_count=prompt_tokens,
                response_token_count=response_tokens,
                total_cost_estimate=total_cost
            )
            
            # Log response if debug mode
            if self.debug_mode:
                self._log_response(llm_response, conversation_id)
            
            # Save conversation if logging enabled
            if self.conversation_log_dir and conversation_id:
                self._save_conversation(context, llm_response, conversation_id)
            
            logger.info(
                f"Gemini response received - "
                f"Time: {processing_time:.2f}s, "
                f"Tokens: {prompt_tokens + response_tokens}, "
                f"Cost: ${total_cost:.6f}"
            )
            
            return llm_response
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"Error generating SQL with Gemini: {e}")
            logger.error(f"Processing time: {processing_time:.2f}s")
            
            # Return error response
            return LLMResponse(
                content="",
                processing_time=processing_time,
                model_name=self.model_name,
                temperature=self.temperature
            )
    
    def _build_system_prompt(self) -> str:
        """Build system prompt for SQL generation."""
        return """You are an expert SQL developer specializing in BigQuery SQL generation from natural language queries.

Your task is to generate accurate, efficient BigQuery SQL queries based on:
1. The user's natural language question
2. The provided database schema with table relationships, field descriptions, and sample data

Key requirements:
- Generate syntactically correct BigQuery SQL
- Use proper table aliases and fully qualified table names
- Handle complex analytical queries (aggregations, rankings, comparisons)
- Include appropriate JOINs when data spans multiple tables
- Use ORDER BY for ranking queries (highest, lowest, maximum, minimum)
- Add LIMIT clauses for top/bottom N queries
- CRITICAL: Use EXACT field names as shown in the schema - if schema shows 'created', use 'created', NOT 'created_at'
- Use appropriate aggregate functions (COUNT, SUM, AVG, MAX, MIN)
- Handle time-based filtering and grouping correctly

For ranking queries like "highest sale price" or "top products":
- Use ORDER BY with DESC for highest/maximum/top
- Use ORDER BY with ASC for lowest/minimum/bottom  
- Include LIMIT 1 for single results or LIMIT N for top N

For "what product has highest sale price" type queries:
- Join order_items (has sale_price) with products (has product details)
- Include product identification fields (name, id, brand)
- Order by the metric and limit appropriately

Respond with ONLY the SQL query, no explanations or markdown formatting."""

    def _build_user_prompt(self, user_query: str, schema_context: str) -> str:
        """Build user prompt with query and schema context."""
        return f"""Database Schema:
{schema_context}

Natural Language Query: {user_query}

Generate the BigQuery SQL query:"""

    def _log_context(self, context: LLMContext, conversation_id: Optional[str] = None):
        """Log the context being sent to LLM."""
        logger.debug("=" * 80)
        logger.debug(f"LLM CONTEXT [{conversation_id or 'NO_ID'}] - {context.timestamp}")
        logger.debug("=" * 80)
        logger.debug(f"Context size: {context.total_context_size} characters")
        logger.debug(f"Schema context size: {len(context.schema_context)} characters")
        logger.debug("\nSYSTEM PROMPT:")
        logger.debug("-" * 40)
        logger.debug(context.system_prompt)
        logger.debug("\nUSER PROMPT:")
        logger.debug("-" * 40)
        logger.debug(context.user_prompt)
        logger.debug("\nSCHEMA CONTEXT:")
        logger.debug("-" * 40)
        logger.debug(context.schema_context[:1000] + "..." if len(context.schema_context) > 1000 else context.schema_context)
        logger.debug("=" * 80)

    def _log_response(self, response: LLMResponse, conversation_id: Optional[str] = None):
        """Log the response from LLM."""
        logger.debug("=" * 80)
        logger.debug(f"LLM RESPONSE [{conversation_id or 'NO_ID'}]")
        logger.debug("=" * 80)
        logger.debug(f"Model: {response.model_name}")
        logger.debug(f"Temperature: {response.temperature}")
        logger.debug(f"Processing time: {response.processing_time:.2f}s")
        logger.debug(f"Prompt tokens: {response.prompt_token_count}")
        logger.debug(f"Response tokens: {response.response_token_count}")
        logger.debug(f"Estimated cost: ${response.total_cost_estimate:.6f}")
        logger.debug("\nGENERATED SQL:")
        logger.debug("-" * 40)
        logger.debug(response.content)
        logger.debug("=" * 80)

    def _save_conversation(
        self, 
        context: LLMContext, 
        response: LLMResponse, 
        conversation_id: str
    ):
        """Save conversation to file for analysis."""
        try:
            conversation_data = {
                "conversation_id": conversation_id,
                "timestamp": context.timestamp,
                "context": {
                    "system_prompt": context.system_prompt,
                    "user_prompt": context.user_prompt,
                    "schema_context": context.schema_context,
                    "total_size": context.total_context_size
                },
                "response": {
                    "content": response.content,
                    "model_name": response.model_name,
                    "temperature": response.temperature,
                    "processing_time": response.processing_time,
                    "prompt_tokens": response.prompt_token_count,
                    "response_tokens": response.response_token_count,
                    "estimated_cost": response.total_cost_estimate
                }
            }
            
            log_file = self.conversation_log_dir / f"conversation_{conversation_id}.json"
            with open(log_file, 'w') as f:
                json.dump(conversation_data, f, indent=2)
                
            logger.debug(f"Conversation saved to: {log_file}")
            
        except Exception as e:
            logger.error(f"Error saving conversation: {e}")

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the current model."""
        return {
            "model_name": self.model_name,
            "temperature": self.temperature,
            "debug_mode": self.debug_mode,
            "conversation_log_dir": str(self.conversation_log_dir) if self.conversation_log_dir else None
        }
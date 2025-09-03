"""Main entry point for LookML Text-to-SQL system."""

import logging
import time
from typing import Optional, Dict, Any

import click

from src.config import load_config, setup_logging
from src.lookml.parser import LookMLParser
from src.bigquery.client import BigQueryClient
from src.bigquery.metadata_loader import BigQueryMetadataLoader
from src.grounding.index import GroundingIndex
from src.grounding.field_mapper import FieldMapper
from src.generator.planner import QueryPlanner
from src.generator.sql_builder import SQLBuilder
from src.generator.validator import SQLValidator
from src.llm.gemini_service import GeminiService
from src.llm.llm_planner import LLMQueryPlanner

logger = logging.getLogger(__name__)


class TextToSQLEngine:
    """Main engine for converting natural language to SQL."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the engine."""
        self.config = load_config(config_path)
        setup_logging(self.config)
        
        logger.info("Initializing LookML Text-to-SQL Engine")
        
        # Initialize components
        self.bigquery_client = BigQueryClient(
            project_id=self.config.bigquery.project_id,
            location=self.config.bigquery.location
        )
        
        self.metadata_loader = BigQueryMetadataLoader(
            client=self.bigquery_client,
            dataset=self.config.bigquery.dataset,
            cache_dir=self.config.cache.directory if self.config.cache.enabled else None
        )
        
        self.field_mapper = FieldMapper()
        
        # These will be initialized when project is loaded
        self.lookml_project = None
        self.grounding_index = None
        self.query_planner = None
        self.sql_builder = None
        self.validator = None
        
        self.initialized = False
    
    def initialize(self) -> None:
        """Initialize the engine by loading LookML and building index."""
        if self.initialized:
            return
        
        start_time = time.time()
        logger.info("Loading LookML project and building grounding index")
        
        # Parse LookML
        lookml_parser = LookMLParser(self.config.lookml.repo_path)
        self.lookml_project = lookml_parser.parse_project()
        
        # Build grounding index
        self.grounding_index = GroundingIndex(
            lookml_project=self.lookml_project,
            metadata_loader=self.metadata_loader,
            field_mapper=self.field_mapper
        )
        
        # Initialize generator components based on configuration
        if self.config.generator.use_llm_planner:
            logger.info("Using LLM-based query planner (Gemini 2.5 Pro)")
            
            # Create conversation log directory if enabled
            conversation_log_dir = None
            if self.config.llm.save_conversations:
                from pathlib import Path
                conversation_log_dir = self.config.llm.conversation_log_dir
                Path(conversation_log_dir).mkdir(parents=True, exist_ok=True)
            
            # Initialize Gemini service
            gemini_service = GeminiService(
                model_name=self.config.llm.model_name,
                temperature=self.config.llm.temperature,
                debug_mode=self.config.llm.enable_debug_logging,
                conversation_log_dir=conversation_log_dir
            )
            
            # Initialize validator for schema validation
            validator = None
            if self.config.generator.enable_dry_run:
                validator = SQLValidator(self.bigquery_client)
            
            # Initialize LLM planner with enhanced semantic context
            self.query_planner = LLMQueryPlanner(
                grounding_index=self.grounding_index,
                gemini_service=gemini_service,
                validator=validator,
                max_retries=self.config.llm.max_retries,
                conversation_log_dir=conversation_log_dir,
                use_enhanced_context=True  # Enable enhanced semantic context
            )
        else:
            logger.info("Using rule-based query planner")
            self.query_planner = QueryPlanner(
                grounding_index=self.grounding_index,
                max_joins=self.config.generator.max_joins
            )
        
        self.sql_builder = SQLBuilder(
            grounding_index=self.grounding_index,
            field_mapper=self.field_mapper
        )
        
        if self.config.generator.enable_dry_run:
            self.validator = SQLValidator(self.bigquery_client)
        
        elapsed = time.time() - start_time
        logger.info(f"Initialization completed in {elapsed:.2f} seconds")
        self.initialized = True
    
    def generate_sql(self, query: str) -> Dict[str, Any]:
        """Generate SQL from natural language query."""
        if not self.initialized:
            self.initialize()
        
        start_time = time.time()
        logger.info(f"Generating SQL for query: {query}")
        
        result = {
            'query': query,
            'sql': None,
            'explore_used': None,
            'fields_selected': [],
            'joins_required': [],
            'limit_applied': False,
            'validation_passed': None,
            'error': None,
            'processing_time': 0,
            'llm_used': self.config.generator.use_llm_planner,
            'llm_cost_estimate': 0.0,
            'llm_token_usage': {}
        }
        
        try:
            # Plan the query
            plan = self.query_planner.plan_query(query, self.config.generator.default_limit)
            
            if not plan:
                result['error'] = "Could not generate query plan from input"
                return result
            
            # Build SQL
            sql = self.sql_builder.build_sql(plan)
            
            # Enforce limit
            original_sql = sql
            sql = self.sql_builder.enforce_limit(sql, self.config.generator.default_limit)
            result['limit_applied'] = sql != original_sql
            
            # Validate if enabled
            if self.validator:
                is_valid, error_msg = self.validator.validate_sql(sql)
                result['validation_passed'] = is_valid
                if not is_valid:
                    result['error'] = error_msg
            
            # Populate result
            result['sql'] = sql
            result['explore_used'] = plan.explore_name
            result['fields_selected'] = [f.qualified_name for f in plan.selected_fields]
            result['joins_required'] = list(plan.required_joins)
            
            # Add LLM metadata if available
            if hasattr(plan, 'llm_response') and plan.llm_response:
                result['llm_cost_estimate'] = plan.llm_response.total_cost_estimate
                result['llm_token_usage'] = plan.llm_response.token_usage or {}
        
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            result['error'] = str(e)
        
        finally:
            result['processing_time'] = time.time() - start_time
            logger.info(f"SQL generation completed in {result['processing_time']:.2f} seconds")
        
        return result


@click.command()
@click.option('--config', '-c', help='Path to configuration file')
@click.option('--query', '-q', help='Natural language query to convert')
@click.option('--interactive', '-i', is_flag=True, help='Run in interactive mode')
@click.option('--validate', is_flag=True, help='Enable SQL validation via dry-run')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(config: Optional[str], query: Optional[str], interactive: bool, validate: bool, verbose: bool):
    """LookML Text-to-SQL converter."""
    
    # Set up logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize engine
    engine = TextToSQLEngine(config)
    
    if validate:
        engine.config.generator.enable_dry_run = True
    
    if interactive:
        # Interactive mode
        click.echo("LookML Text-to-SQL Interactive Mode")
        click.echo("Type your queries below (type 'quit' to exit):")
        click.echo()
        
        while True:
            try:
                user_query = click.prompt("Query", type=str).strip()
                
                if user_query.lower() in ['quit', 'exit', 'q']:
                    break
                
                if not user_query:
                    continue
                
                result = engine.generate_sql(user_query)
                
                if result['error']:
                    click.echo(f"Error: {result['error']}", err=True)
                else:
                    click.echo(result['sql'])
                    
                    if verbose:
                        click.echo()
                        click.echo(f"Explore: {result['explore_used']}")
                        click.echo(f"Fields: {', '.join(result['fields_selected'])}")
                        click.echo(f"Joins: {', '.join(result['joins_required']) or 'None'}")
                        click.echo(f"Time: {result['processing_time']:.2f}s")
                        if result['validation_passed'] is not None:
                            click.echo(f"Validation: {'PASSED' if result['validation_passed'] else 'FAILED'}")
                
                click.echo()
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                click.echo(f"Error: {e}", err=True)
    
    elif query:
        # Single query mode
        result = engine.generate_sql(query)
        
        if result['error']:
            click.echo(f"Error: {result['error']}", err=True)
            exit(1)
        else:
            click.echo(result['sql'])
            
            if verbose:
                click.echo(f"\n# Explore: {result['explore_used']}", err=True)
                click.echo(f"# Fields: {', '.join(result['fields_selected'])}", err=True)
                click.echo(f"# Processing time: {result['processing_time']:.2f}s", err=True)
    
    else:
        # Show help if no query provided
        click.echo("Please provide a query with --query or use --interactive mode")
        exit(1)


if __name__ == "__main__":
    main()
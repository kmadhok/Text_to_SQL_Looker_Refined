"""Configuration management for LookML Text-to-SQL system."""

import os
import logging
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class BigQueryConfig(BaseModel):
    """BigQuery configuration."""
    project_id: Optional[str] = None
    dataset: str = "bigquery-public-data.thelook_ecommerce"
    location: str = "US"


class LookMLConfig(BaseModel):
    """LookML configuration."""
    repo_path: str = "./data/lookml"


class CacheConfig(BaseModel):
    """Cache configuration."""
    enabled: bool = True
    directory: str = "./data/cache"
    metadata_ttl: int = 3600


class GeneratorConfig(BaseModel):
    """SQL generator configuration."""
    default_limit: int = 100
    enable_dry_run: bool = False
    max_joins: int = 10
    use_llm_planner: bool = True  # Use LLM planner instead of rule-based


class LLMConfig(BaseModel):
    """LLM configuration."""
    model_name: str = "gemini-2.5-pro"
    temperature: float = 0.1
    max_retries: int = 3
    enable_debug_logging: bool = True
    save_conversations: bool = True
    conversation_log_dir: str = "./data/llm_logs"
    max_context_size: int = 15000


class LoggingConfig(BaseModel):
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class Config(BaseModel):
    """Main configuration class."""
    bigquery: BigQueryConfig = Field(default_factory=BigQueryConfig)
    lookml: LookMLConfig = Field(default_factory=LookMLConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    generator: GeneratorConfig = Field(default_factory=GeneratorConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from file and environment variables."""
    load_dotenv()
    
    if config_path is None:
        config_path = "config/config.yaml"
    
    config_data = {}
    if Path(config_path).exists():
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f) or {}
    
    # Override with environment variables
    if os.getenv('GOOGLE_CLOUD_PROJECT'):
        config_data.setdefault('bigquery', {})['project_id'] = os.getenv('GOOGLE_CLOUD_PROJECT')
    
    if os.getenv('BIGQUERY_DATASET'):
        config_data.setdefault('bigquery', {})['dataset'] = os.getenv('BIGQUERY_DATASET')
    
    if os.getenv('BIGQUERY_LOCATION'):
        config_data.setdefault('bigquery', {})['location'] = os.getenv('BIGQUERY_LOCATION')
    
    if os.getenv('LOOKML_REPO_PATH'):
        config_data.setdefault('lookml', {})['repo_path'] = os.getenv('LOOKML_REPO_PATH')
    
    if os.getenv('ENABLE_CACHE'):
        config_data.setdefault('cache', {})['enabled'] = os.getenv('ENABLE_CACHE').lower() == 'true'
    
    if os.getenv('CACHE_DIR'):
        config_data.setdefault('cache', {})['directory'] = os.getenv('CACHE_DIR')
    
    if os.getenv('ENABLE_DRY_RUN'):
        config_data.setdefault('generator', {})['enable_dry_run'] = os.getenv('ENABLE_DRY_RUN').lower() == 'true'
    
    if os.getenv('DEFAULT_LIMIT'):
        config_data.setdefault('generator', {})['default_limit'] = int(os.getenv('DEFAULT_LIMIT'))
    
    if os.getenv('LOG_LEVEL'):
        config_data.setdefault('logging', {})['level'] = os.getenv('LOG_LEVEL')
    
    # LLM configuration from environment variables
    if os.getenv('USE_LLM_PLANNER'):
        config_data.setdefault('generator', {})['use_llm_planner'] = os.getenv('USE_LLM_PLANNER').lower() == 'true'
    
    if os.getenv('LLM_MODEL_NAME'):
        config_data.setdefault('llm', {})['model_name'] = os.getenv('LLM_MODEL_NAME')
    
    if os.getenv('LLM_TEMPERATURE'):
        config_data.setdefault('llm', {})['temperature'] = float(os.getenv('LLM_TEMPERATURE'))
    
    if os.getenv('LLM_MAX_RETRIES'):
        config_data.setdefault('llm', {})['max_retries'] = int(os.getenv('LLM_MAX_RETRIES'))
    
    if os.getenv('LLM_DEBUG_LOGGING'):
        config_data.setdefault('llm', {})['enable_debug_logging'] = os.getenv('LLM_DEBUG_LOGGING').lower() == 'true'
    
    if os.getenv('LLM_SAVE_CONVERSATIONS'):
        config_data.setdefault('llm', {})['save_conversations'] = os.getenv('LLM_SAVE_CONVERSATIONS').lower() == 'true'
    
    if os.getenv('LLM_CONVERSATION_LOG_DIR'):
        config_data.setdefault('llm', {})['conversation_log_dir'] = os.getenv('LLM_CONVERSATION_LOG_DIR')
    
    return Config(**config_data)


def setup_logging(config: Config) -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, config.logging.level),
        format=config.logging.format
    )
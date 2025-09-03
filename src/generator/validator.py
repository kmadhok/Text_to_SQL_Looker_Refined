"""SQL validator for dry-run validation against BigQuery."""

import logging
from typing import Tuple

from ..bigquery.client import BigQueryClient

logger = logging.getLogger(__name__)


class SQLValidator:
    """Validates SQL queries using BigQuery dry-run."""
    
    def __init__(self, bigquery_client: BigQueryClient):
        """Initialize SQL validator."""
        self.bigquery_client = bigquery_client
    
    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """Validate SQL using BigQuery dry-run."""
        logger.info("Validating SQL with dry-run")
        
        try:
            is_valid = self.bigquery_client.dry_run_query(sql)
            
            if is_valid:
                logger.info("SQL validation passed")
                return True, "SQL is valid"
            else:
                logger.warning("SQL validation failed")
                return False, "SQL validation failed - check syntax and references"
        
        except Exception as e:
            error_msg = str(e)
            logger.error(f"SQL validation error: {error_msg}")
            
            # Extract relevant error information
            if "not found" in error_msg.lower():
                return False, f"Reference error: {error_msg}"
            elif "syntax error" in error_msg.lower():
                return False, f"Syntax error: {error_msg}"
            else:
                return False, f"Validation error: {error_msg}"
    
    def extract_error_type(self, error_message: str) -> str:
        """Extract and categorize error type from BigQuery error message."""
        error_lower = error_message.lower()
        
        if "not found" in error_lower:
            if "table" in error_lower:
                return "MISSING_TABLE"
            elif "column" in error_lower:
                return "MISSING_COLUMN"
            elif "dataset" in error_lower:
                return "MISSING_DATASET"
            else:
                return "REFERENCE_ERROR"
        elif "syntax error" in error_lower:
            return "SYNTAX_ERROR"
        elif "permission" in error_lower or "access" in error_lower:
            return "PERMISSION_ERROR"
        elif "invalid" in error_lower:
            return "INVALID_QUERY"
        else:
            return "UNKNOWN_ERROR"
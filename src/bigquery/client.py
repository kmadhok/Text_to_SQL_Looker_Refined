"""BigQuery client wrapper for metadata operations."""

import logging
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class BigQueryClient:
    """Wrapper for BigQuery client with error handling."""
    
    def __init__(self, project_id: Optional[str] = None, location: str = "US"):
        """Initialize BigQuery client."""
        self.project_id = project_id
        self.location = location
        
        try:
            self.client = bigquery.Client(project=project_id, location=location)
            logger.info(f"Initialized BigQuery client for project: {project_id}, location: {location}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    def query(self, sql: str, **kwargs) -> bigquery.QueryJob:
        """Execute a query with error handling."""
        try:
            job_config = bigquery.QueryJobConfig(**kwargs)
            query_job = self.client.query(sql, job_config=job_config, location=self.location)
            return query_job
        except GoogleCloudError as e:
            logger.error(f"BigQuery query failed: {e}")
            raise
    
    def dry_run_query(self, sql: str) -> bool:
        """Perform a dry run to validate SQL syntax."""
        try:
            job_config = bigquery.QueryJobConfig(
                dry_run=True,
                use_query_cache=False
            )
            self.client.query(sql, job_config=job_config, location=self.location)
            return True
        except GoogleCloudError as e:
            logger.error(f"SQL validation failed: {e}")
            return False
    
    def get_dataset_ref(self, dataset_id: str) -> bigquery.DatasetReference:
        """Get dataset reference."""
        # Handle fully qualified dataset names
        if '.' in dataset_id:
            project_id, dataset_name = dataset_id.split('.', 1)
            return bigquery.DatasetReference(project_id, dataset_name)
        else:
            return bigquery.DatasetReference(self.project_id, dataset_id)
    
    def get_table_ref(self, dataset_id: str, table_id: str) -> bigquery.TableReference:
        """Get table reference."""
        dataset_ref = self.get_dataset_ref(dataset_id)
        return dataset_ref.table(table_id)
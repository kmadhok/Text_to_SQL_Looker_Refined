"""Persistent storage system for SchemaIntelligence objects."""

import json
import hashlib
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
import logging
from dataclasses import dataclass, asdict
from enum import Enum

from .schema_models import (
    SchemaIntelligence, EnrichedFieldInfo, TableSemantics, 
    FieldSemanticType, TableBusinessType
)
from ..grounding.index import GroundingIndex, FieldInfo

logger = logging.getLogger(__name__)


@dataclass
class SchemaFingerprint:
    """Fingerprint for detecting schema changes."""
    lookml_files_hash: str
    bigquery_metadata_hash: str
    schema_version: str = "1.0"
    timestamp: float = 0.0
    
    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


class SchemaIntelligencePersistentStorage:
    """Handles persistent storage and retrieval of schema intelligence data."""
    
    def __init__(self, storage_dir: str = "data/schema_intelligence"):
        """Initialize persistent storage.
        
        Args:
            storage_dir: Directory to store schema intelligence files
        """
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        self.intelligence_file = self.storage_dir / "schema_intelligence.json"
        self.fingerprint_file = self.storage_dir / "schema_fingerprint.json"
        
        self.logger = logging.getLogger(__name__)
    
    def generate_schema_fingerprint(self, grounding_index: GroundingIndex) -> SchemaFingerprint:
        """Generate fingerprint for current schema state.
        
        Args:
            grounding_index: Current grounding index
            
        Returns:
            Schema fingerprint for change detection
        """
        # Generate hash for LookML files
        lookml_hash = self._generate_lookml_files_hash(grounding_index)
        
        # Generate hash for BigQuery metadata
        bigquery_hash = self._generate_bigquery_metadata_hash(grounding_index)
        
        return SchemaFingerprint(
            lookml_files_hash=lookml_hash,
            bigquery_metadata_hash=bigquery_hash
        )
    
    def _generate_lookml_files_hash(self, grounding_index: GroundingIndex) -> str:
        """Generate hash of LookML structure and file timestamps.

        Uses data that actually exists in this codebase to avoid false positives.
        """
        hash_content = []

        # Add high-level model and explore structure
        for model_name, model in grounding_index.lookml_project.models.items():
            hash_content.append(f"model:{model_name}")
            for explore in model.explores.values():
                # LookMLExplore exposes base_view_name (not base_view)
                hash_content.append(f"explore:{explore.name}:{explore.base_view_name}")

        # Add views with counts (cheap proxy for schema changes)
        for view_name, view in grounding_index.lookml_project.views.items():
            hash_content.append(f"view:{view_name}:{view.sql_table_name}")
            hash_content.append(f"dims:{len(view.dimensions)}:measures:{len(view.measures)}")

        # Add file mtimes for *.lkml files to detect edits
        if hasattr(grounding_index.lookml_project, 'repo_path'):
            repo_path = Path(grounding_index.lookml_project.repo_path)
            if repo_path.exists():
                for pattern in ("*.lkml", "*.model.lkml", "*.view.lkml"):
                    for lookml_file in repo_path.rglob(pattern):
                        if lookml_file.is_file():
                            stat = lookml_file.stat()
                            hash_content.append(f"file:{lookml_file.name}:{int(stat.st_mtime)}")

        content_str = "|".join(sorted(hash_content))
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]
    
    def _generate_bigquery_metadata_hash(self, grounding_index: GroundingIndex) -> str:
        """Generate hash of effective BigQuery metadata seen by the system.

        Since GroundingIndex does not expose raw BQ metadata objects, we derive a
        stable hash from available fields per explore, including data types when present.
        """
        hash_content = []

        # Include dataset id to distinguish environments
        try:
            dataset_id = getattr(grounding_index.metadata_loader, 'dataset', None)
            if dataset_id:
                hash_content.append(f"dataset:{dataset_id}")
        except Exception:
            pass

        # Traverse explores and their available fields
        for explore_name, explore_info in grounding_index.explores.items():
            hash_content.append(f"explore:{explore_name}:base:{explore_info.base_view}")
            for qualified_name, field in explore_info.available_fields.items():
                # Capture view, field name, and any known types/descriptions
                hash_content.append(
                    "|".join(
                        [
                            f"field:{qualified_name}",
                            f"lt:{field.lookml_type or ''}",
                            f"bq:{field.bigquery_data_type or ''}",
                            f"hidden:{int(field.hidden)}",
                        ]
                    )
                )

        content_str = "|".join(sorted(hash_content))
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]
    
    def has_schema_changed(self, current_fingerprint: SchemaFingerprint) -> bool:
        """Check if schema has changed since last analysis.
        
        Args:
            current_fingerprint: Current schema fingerprint
            
        Returns:
            True if schema has changed or no previous analysis exists
        """
        if not self.fingerprint_file.exists():
            self.logger.info("No previous schema fingerprint found")
            return True
        
        try:
            with open(self.fingerprint_file, 'r') as f:
                saved_data = json.load(f)
            
            saved_fingerprint = SchemaFingerprint(**saved_data)
            
            # Compare fingerprints
            changed = (
                saved_fingerprint.lookml_files_hash != current_fingerprint.lookml_files_hash or
                saved_fingerprint.bigquery_metadata_hash != current_fingerprint.bigquery_metadata_hash
            )
            
            if changed:
                self.logger.info("Schema changes detected")
                self.logger.debug(f"LookML hash: {saved_fingerprint.lookml_files_hash} -> {current_fingerprint.lookml_files_hash}")
                self.logger.debug(f"BigQuery hash: {saved_fingerprint.bigquery_metadata_hash} -> {current_fingerprint.bigquery_metadata_hash}")
            else:
                self.logger.info("No schema changes detected")
            
            return changed
            
        except Exception as e:
            self.logger.warning(f"Error reading schema fingerprint: {e}")
            return True  # Assume changed if can't read fingerprint
    
    def save_schema_intelligence(self, intelligence: SchemaIntelligence, fingerprint: SchemaFingerprint) -> None:
        """Save schema intelligence and fingerprint to disk.
        
        Args:
            intelligence: Schema intelligence to save
            fingerprint: Current schema fingerprint
        """
        try:
            # Convert to serializable format
            intelligence_data = self._serialize_schema_intelligence(intelligence)
            
            # Save intelligence data
            with open(self.intelligence_file, 'w') as f:
                json.dump(intelligence_data, f, indent=2)
            
            # Save fingerprint
            with open(self.fingerprint_file, 'w') as f:
                json.dump(asdict(fingerprint), f, indent=2)
            
            self.logger.info(f"Schema intelligence saved to {self.intelligence_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving schema intelligence: {e}")
            raise
    
    def load_schema_intelligence(self) -> Optional[SchemaIntelligence]:
        """Load schema intelligence from disk.
        
        Returns:
            Loaded schema intelligence or None if not available
        """
        if not self.intelligence_file.exists():
            self.logger.info("No saved schema intelligence found")
            return None
        
        try:
            with open(self.intelligence_file, 'r') as f:
                intelligence_data = json.load(f)
            
            intelligence = self._deserialize_schema_intelligence(intelligence_data)
            self.logger.info(f"Schema intelligence loaded from {self.intelligence_file}")
            return intelligence
            
        except Exception as e:
            self.logger.error(f"Error loading schema intelligence: {e}")
            return None
    
    def _serialize_schema_intelligence(self, intelligence: SchemaIntelligence) -> Dict[str, Any]:
        """Convert SchemaIntelligence to JSON-serializable format."""
        # Convert enriched fields
        enriched_fields_data = {}
        for qualified_name, enriched_field in intelligence.enriched_fields.items():
            enriched_fields_data[qualified_name] = {
                "field_info": {
                    "name": enriched_field.field_info.name,
                    "qualified_name": enriched_field.field_info.qualified_name,
                    "view_name": enriched_field.field_info.view_name,
                    "field_type": enriched_field.field_info.field_type,
                    "lookml_type": enriched_field.field_info.lookml_type,
                    "sql_expression": enriched_field.field_info.sql_expression,
                    "lookml_description": enriched_field.field_info.lookml_description,
                    "bigquery_description": enriched_field.field_info.bigquery_description,
                    "bigquery_data_type": enriched_field.field_info.bigquery_data_type
                },
                "semantic_type": enriched_field.semantic_type.value,
                "business_purpose": enriched_field.business_purpose,
                "confidence_score": enriched_field.confidence_score,
                "usage_recommendations": enriched_field.usage_recommendations,
                "common_mistakes": enriched_field.common_mistakes,
                "related_fields": enriched_field.related_fields
            }
        
        # Convert table semantics
        table_semantics_data = {}
        for table_name, semantics in intelligence.table_semantics.items():
            table_semantics_data[table_name] = {
                "table_name": semantics.table_name,
                "business_type": semantics.business_type.value,
                "primary_purpose": semantics.primary_purpose,
                "key_concepts": semantics.key_concepts,
                "best_for_queries": semantics.best_for_queries,
                "avoid_for_queries": semantics.avoid_for_queries,
                "performance_notes": semantics.performance_notes
            }
        
        return {
            "enriched_fields": enriched_fields_data,
            "table_semantics": table_semantics_data,
            "business_concept_map": intelligence.business_concept_map,
            "query_patterns": intelligence.query_patterns,
            "relationship_insights": intelligence.relationship_insights,
            "version": "1.0",
            "timestamp": time.time()
        }
    
    def _deserialize_schema_intelligence(self, data: Dict[str, Any]) -> SchemaIntelligence:
        """Convert JSON data back to SchemaIntelligence object."""
        # Reconstruct enriched fields
        enriched_fields = {}
        for qualified_name, field_data in data["enriched_fields"].items():
            # Reconstruct FieldInfo
            field_info_data = field_data["field_info"]
            field_info = FieldInfo(
                name=field_info_data["name"],
                qualified_name=field_info_data["qualified_name"],
                view_name=field_info_data["view_name"],
                field_type=field_info_data["field_type"],
                lookml_type=field_info_data["lookml_type"],
                sql_expression=field_info_data["sql_expression"],
                lookml_description=field_info_data["lookml_description"],
                bigquery_description=field_info_data["bigquery_description"],
                bigquery_data_type=field_info_data["bigquery_data_type"]
            )
            
            # Reconstruct EnrichedFieldInfo
            enriched_fields[qualified_name] = EnrichedFieldInfo(
                field_info=field_info,
                semantic_type=FieldSemanticType(field_data["semantic_type"]),
                business_purpose=field_data["business_purpose"],
                confidence_score=field_data["confidence_score"],
                usage_recommendations=field_data["usage_recommendations"],
                common_mistakes=field_data["common_mistakes"],
                related_fields=field_data["related_fields"]
            )
        
        # Reconstruct table semantics
        table_semantics = {}
        for table_name, semantics_data in data["table_semantics"].items():
            table_semantics[table_name] = TableSemantics(
                table_name=semantics_data["table_name"],
                business_type=TableBusinessType(semantics_data["business_type"]),
                primary_purpose=semantics_data["primary_purpose"],
                key_concepts=semantics_data["key_concepts"],
                best_for_queries=semantics_data["best_for_queries"],
                avoid_for_queries=semantics_data["avoid_for_queries"],
                performance_notes=semantics_data["performance_notes"]
            )
        
        return SchemaIntelligence(
            enriched_fields=enriched_fields,
            table_semantics=table_semantics,
            business_concept_map=data["business_concept_map"],
            query_patterns=data["query_patterns"],
            relationship_insights=data["relationship_insights"]
        )
    
    def clear_cache(self) -> None:
        """Clear all cached schema intelligence data."""
        try:
            if self.intelligence_file.exists():
                self.intelligence_file.unlink()
            if self.fingerprint_file.exists():
                self.fingerprint_file.unlink()
            self.logger.info("Schema intelligence cache cleared")
        except Exception as e:
            self.logger.error(f"Error clearing cache: {e}")

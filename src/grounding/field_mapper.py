"""Field mapping utilities for resolving LookML expressions."""

import re
import logging
from typing import Dict, Optional, Set

logger = logging.getLogger(__name__)


class FieldMapper:
    """Maps LookML fields to physical SQL expressions."""
    
    def __init__(self):
        """Initialize field mapper."""
        self.table_aliases: Dict[str, str] = {}
    
    def resolve_lookml_expression(
        self, 
        expression: str, 
        table_name: str,
        view_name: str,
        field_mappings: Dict[str, str]
    ) -> str:
        """Resolve LookML expressions to SQL."""
        if not expression:
            return expression
        
        resolved = expression
        
        # Replace ${TABLE} references
        resolved = self._resolve_table_references(resolved, table_name)
        
        # Replace ${view.field} references
        resolved = self._resolve_field_references(resolved, view_name, field_mappings)
        
        logger.debug(f"Resolved '{expression}' to '{resolved}'")
        return resolved
    
    def _resolve_table_references(self, expression: str, table_name: str) -> str:
        """Replace ${TABLE} with actual table name or alias."""
        # Pattern to match ${TABLE}
        table_pattern = r'\$\{TABLE\}'
        
        # Use alias if available, otherwise use table name
        table_ref = self.table_aliases.get(table_name, table_name)
        
        resolved = re.sub(table_pattern, table_ref, expression)
        return resolved
    
    def _resolve_field_references(
        self, 
        expression: str, 
        current_view: str, 
        field_mappings: Dict[str, str]
    ) -> str:
        """Replace ${view.field} references with resolved SQL."""
        # Pattern to match ${view.field} or ${field} (current view)
        field_pattern = r'\$\{(?:([^.}]+)\.)?([^}]+)\}'
        
        def replace_field_ref(match):
            view_name = match.group(1) or current_view
            field_name = match.group(2)
            
            # Look for the field in mappings
            field_key = f"{view_name}.{field_name}"
            
            if field_key in field_mappings:
                return field_mappings[field_key]
            elif field_name in field_mappings:
                return field_mappings[field_name]
            else:
                logger.warning(f"Could not resolve field reference: {match.group(0)}")
                return match.group(0)  # Return original if not found
        
        resolved = re.sub(field_pattern, replace_field_ref, expression)
        return resolved
    
    def set_table_alias(self, table_name: str, alias: str) -> None:
        """Set alias for a table."""
        self.table_aliases[table_name] = alias
    
    def clear_aliases(self) -> None:
        """Clear all table aliases."""
        self.table_aliases.clear()
    
    def extract_referenced_fields(self, expression: str) -> Set[str]:
        """Extract all field references from a LookML expression."""
        field_pattern = r'\$\{(?:([^.}]+)\.)?([^}]+)\}'
        references = set()
        
        for match in re.finditer(field_pattern, expression):
            view_name = match.group(1)
            field_name = match.group(2)
            
            if view_name:
                references.add(f"{view_name}.{field_name}")
            else:
                references.add(field_name)
        
        return references
    
    def is_simple_column_reference(self, expression: str) -> bool:
        """Check if expression is a simple column reference like ${TABLE}.column_name."""
        simple_pattern = r'^\$\{TABLE\}\.[\w_]+$'
        return bool(re.match(simple_pattern, expression.strip()))
    
    def extract_column_name(self, expression: str) -> Optional[str]:
        """Extract column name from simple ${TABLE}.column_name expressions."""
        if self.is_simple_column_reference(expression):
            return expression.split('.', 1)[1]
        return None
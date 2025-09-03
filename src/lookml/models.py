"""Pydantic models for LookML structures."""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class LookMLDimension(BaseModel):
    """Represents a dimension in a LookML view."""
    name: str
    type: Optional[str] = None
    sql: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    primary_key: bool = False
    timeframes: List[str] = Field(default_factory=list)
    
    
class LookMLMeasure(BaseModel):
    """Represents a measure in a LookML view."""
    name: str
    type: Optional[str] = None
    sql: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False


class LookMLView(BaseModel):
    """Represents a LookML view."""
    name: str
    sql_table_name: Optional[str] = None
    dimensions: Dict[str, LookMLDimension] = Field(default_factory=dict)
    measures: Dict[str, LookMLMeasure] = Field(default_factory=dict)
    primary_key: Optional[str] = None
    
    def get_all_fields(self) -> Dict[str, Any]:
        """Get all fields (dimensions + measures) as a combined dict."""
        fields = {}
        fields.update(self.dimensions)
        fields.update(self.measures)
        return fields


class LookMLJoin(BaseModel):
    """Represents a join in a LookML explore."""
    view_name: str
    type: str = "left_outer"
    sql_on: Optional[str] = None
    relationship: Optional[str] = None
    required: bool = False


class LookMLExplore(BaseModel):
    """Represents a LookML explore."""
    name: str
    from_view: str
    view_name: Optional[str] = None  # If different from name
    joins: List[LookMLJoin] = Field(default_factory=list)
    hidden: bool = False
    
    @property
    def base_view_name(self) -> str:
        """Get the base view name for this explore."""
        return self.view_name or self.from_view


class LookMLModel(BaseModel):
    """Represents a complete LookML model."""
    name: str
    connection: Optional[str] = None
    include: List[str] = Field(default_factory=list)
    views: Dict[str, LookMLView] = Field(default_factory=dict)
    explores: Dict[str, LookMLExplore] = Field(default_factory=dict)


class LookMLProject(BaseModel):
    """Represents the entire LookML project."""
    models: Dict[str, LookMLModel] = Field(default_factory=dict)
    views: Dict[str, LookMLView] = Field(default_factory=dict)  # Standalone views
    
    def get_all_views(self) -> Dict[str, LookMLView]:
        """Get all views from models and standalone views."""
        all_views = {}
        
        # Add standalone views
        all_views.update(self.views)
        
        # Add views from models
        for model in self.models.values():
            all_views.update(model.views)
            
        return all_views
    
    def get_all_explores(self) -> Dict[str, LookMLExplore]:
        """Get all explores from all models."""
        all_explores = {}
        
        for model in self.models.values():
            # Prefix explore names with model name to avoid conflicts
            for explore_name, explore in model.explores.items():
                key = f"{model.name}.{explore_name}"
                all_explores[key] = explore
                
        return all_explores
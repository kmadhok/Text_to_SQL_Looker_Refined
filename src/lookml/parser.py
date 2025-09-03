"""LookML parser for loading and parsing LookML files."""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Any

import lkml

from .models import (
    LookMLDimension, LookMLMeasure, LookMLView, 
    LookMLJoin, LookMLExplore, LookMLModel, LookMLProject
)

logger = logging.getLogger(__name__)


class LookMLParser:
    """Parser for LookML files."""
    
    def __init__(self, repo_path: str):
        """Initialize parser with repository path."""
        self.repo_path = Path(repo_path)
        if not self.repo_path.exists():
            raise ValueError(f"LookML repository path does not exist: {repo_path}")
    
    def parse_project(self) -> LookMLProject:
        """Parse entire LookML project."""
        logger.info(f"Parsing LookML project from {self.repo_path}")
        
        project = LookMLProject()
        
        # Parse model files
        for model_file in self.repo_path.glob("**/*.model.lkml"):
            try:
                model = self._parse_model_file(model_file)
                project.models[model.name] = model
                logger.info(f"Parsed model: {model.name}")
            except Exception as e:
                logger.error(f"Error parsing model file {model_file}: {e}")
        
        # Parse standalone view files
        for view_file in self.repo_path.glob("**/*.view.lkml"):
            try:
                views = self._parse_view_file(view_file)
                for view in views:
                    project.views[view.name] = view
                    logger.info(f"Parsed view: {view.name}")
            except Exception as e:
                logger.error(f"Error parsing view file {view_file}: {e}")
        
        logger.info(f"Parsed {len(project.models)} models and {len(project.views)} views")
        return project
    
    def _parse_model_file(self, file_path: Path) -> LookMLModel:
        """Parse a single model file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        parsed = lkml.load(content)
        model_name = file_path.stem
        
        model = LookMLModel(name=model_name)
        
        # Parse connection
        if 'connection' in parsed:
            model.connection = parsed['connection']
        
        # Parse includes
        if 'include' in parsed:
            includes = parsed['include']
            if isinstance(includes, str):
                model.include = [includes]
            elif isinstance(includes, list):
                model.include = includes
        
        # Parse explores - handle both 'explore' and 'explores' keys
        explores = parsed.get('explores', parsed.get('explore', []))
        if not isinstance(explores, list):
            explores = [explores]
        
        for explore_data in explores:
            explore = self._parse_explore(explore_data)
            model.explores[explore.name] = explore
        
        # Parse views embedded in model - handle both 'view' and 'views' keys  
        views = parsed.get('views', parsed.get('view', []))
        if not isinstance(views, list):
            views = [views]
        
        for view_data in views:
            view = self._parse_view(view_data)
            model.views[view.name] = view
        
        return model
    
    def _parse_view_file(self, file_path: Path) -> List[LookMLView]:
        """Parse a view file that may contain multiple views."""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        parsed = lkml.load(content)
        views = []
        
        view_data_list = parsed.get('views', parsed.get('view', []))
        if not isinstance(view_data_list, list):
            view_data_list = [view_data_list]
        
        for view_data in view_data_list:
            view = self._parse_view(view_data)
            views.append(view)
        
        return views
    
    def _parse_view(self, view_data: Dict[str, Any]) -> LookMLView:
        """Parse a single view from LookML data."""
        name = view_data.get('name', '')
        view = LookMLView(name=name)
        
        # Parse sql_table_name
        if 'sql_table_name' in view_data:
            view.sql_table_name = view_data['sql_table_name']
        
        # Parse dimensions - try both plural and singular keys
        dimensions = view_data.get('dimensions', view_data.get('dimension', []))
        if not isinstance(dimensions, list):
            dimensions = [dimensions]
        
        for dim_data in dimensions:
            dimension = self._parse_dimension(dim_data)
            view.dimensions[dimension.name] = dimension
            
            if dimension.primary_key:
                view.primary_key = dimension.name
        
        # Parse dimension_groups (time dimensions) - try both plural and singular keys
        dimension_groups = view_data.get('dimension_groups', view_data.get('dimension_group', []))
        if not isinstance(dimension_groups, list):
            dimension_groups = [dimension_groups]
        
        for dim_group_data in dimension_groups:
            dimension = self._parse_dimension_group(dim_group_data)
            view.dimensions[dimension.name] = dimension
        
        # Parse measures - try both plural and singular keys
        measures = view_data.get('measures', view_data.get('measure', []))
        if not isinstance(measures, list):
            measures = [measures]
        
        for measure_data in measures:
            measure = self._parse_measure(measure_data)
            view.measures[measure.name] = measure
        
        return view
    
    def _parse_dimension(self, dim_data: Dict[str, Any]) -> LookMLDimension:
        """Parse a dimension from LookML data."""
        dimension = LookMLDimension(
            name=dim_data.get('name', ''),
            type=dim_data.get('type'),
            sql=dim_data.get('sql'),
            description=dim_data.get('description'),
            hidden=dim_data.get('hidden', False),
            primary_key=dim_data.get('primary_key', False)
        )
        
        # Handle timeframes for time dimensions
        if dimension.type == 'time':
            timeframes = dim_data.get('timeframes', [])
            if isinstance(timeframes, str):
                timeframes = [timeframes]
            dimension.timeframes = timeframes
        
        return dimension
    
    def _parse_dimension_group(self, dim_group_data: Dict[str, Any]) -> LookMLDimension:
        """Parse a dimension_group from LookML data."""
        dimension = LookMLDimension(
            name=dim_group_data.get('name', ''),
            type=dim_group_data.get('type'),
            sql=dim_group_data.get('sql'),
            description=dim_group_data.get('description'),
            hidden=dim_group_data.get('hidden', False),
            primary_key=dim_group_data.get('primary_key', False)
        )
        
        # Handle timeframes for time dimensions  
        timeframes = dim_group_data.get('timeframes', [])
        if isinstance(timeframes, str):
            timeframes = [timeframes]
        dimension.timeframes = timeframes
        
        return dimension
    
    def _parse_measure(self, measure_data: Dict[str, Any]) -> LookMLMeasure:
        """Parse a measure from LookML data."""
        return LookMLMeasure(
            name=measure_data.get('name', ''),
            type=measure_data.get('type'),
            sql=measure_data.get('sql'),
            description=measure_data.get('description'),
            hidden=measure_data.get('hidden', False)
        )
    
    def _parse_explore(self, explore_data: Dict[str, Any]) -> LookMLExplore:
        """Parse an explore from LookML data."""
        explore = LookMLExplore(
            name=explore_data.get('name', ''),
            from_view=explore_data.get('from', explore_data.get('name', '')),
            view_name=explore_data.get('view_name'),
            hidden=explore_data.get('hidden', False)
        )
        
        # Parse joins
        joins = explore_data.get('join', [])
        if not isinstance(joins, list):
            joins = [joins]
        
        for join_data in joins:
            join = self._parse_join(join_data)
            explore.joins.append(join)
        
        return explore
    
    def _parse_join(self, join_data: Dict[str, Any]) -> LookMLJoin:
        """Parse a join from LookML data."""
        return LookMLJoin(
            view_name=join_data.get('name', ''),
            type=join_data.get('type', 'left_outer'),
            sql_on=join_data.get('sql_on'),
            relationship=join_data.get('relationship'),
            required=join_data.get('required', False)
        )
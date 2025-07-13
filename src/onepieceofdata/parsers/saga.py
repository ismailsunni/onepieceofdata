"""Modern saga parser for processing scraped saga data."""

import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger

from ..models.data import SagaModel, ScrapingResult
from ..database.operations import DatabaseManager


class SagaParser:
    """Modern saga parser with data validation and database integration."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the saga parser.
        
        Args:
            db_manager: Database manager instance. If None, creates a new one.
        """
        self.db_manager = db_manager or DatabaseManager()
        
    def parse_and_validate_sagas(self, sagas_results: List[ScrapingResult]) -> List[SagaModel]:
        """Parse and validate saga data from scraping results.
        
        Args:
            sagas_results: List of ScrapingResult objects containing saga data
            
        Returns:
            List of validated SagaModel objects
        """
        validated_sagas = []
        
        logger.info(f"Parsing and validating {len(sagas_results)} saga results")
        
        for result in sagas_results:
            if not result.success or not result.data:
                logger.warning(f"Skipping failed result: {result.error}")
                continue
                
            try:
                # Create SagaModel from the data
                saga_model = SagaModel(**result.data)
                validated_sagas.append(saga_model)
                
            except Exception as e:
                logger.warning(f"Failed to validate saga data: {str(e)}")
                continue
                
        logger.success(f"Successfully validated {len(validated_sagas)} sagas")
        return validated_sagas
        
    def save_sagas_to_json(self, sagas: List[SagaModel], output_path: str) -> bool:
        """Save validated saga data to JSON file.
        
        Args:
            sagas: List of validated SagaModel objects
            output_path: Path to save JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving {len(sagas)} sagas to {output_path}")
            
            # Convert to list of dictionaries
            sagas_data = [saga.model_dump() for saga in sagas]
            
            # Create output directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Save to JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(sagas_data, f, indent=2, ensure_ascii=False)
                
            logger.success(f"Successfully saved sagas to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save sagas to JSON: {str(e)}")
            return False
            
    def load_sagas_from_json(self, json_path: str) -> List[SagaModel]:
        """Load saga data from JSON file.
        
        Args:
            json_path: Path to JSON file containing saga data
            
        Returns:
            List of SagaModel objects
        """
        try:
            logger.info(f"Loading sagas from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                sagas_data = json.load(f)
                
            sagas = []
            for saga_data in sagas_data:
                try:
                    saga = SagaModel(**saga_data)
                    sagas.append(saga)
                except Exception as e:
                    logger.warning(f"Failed to load saga: {str(e)}")
                    continue
                    
            logger.success(f"Successfully loaded {len(sagas)} sagas from JSON")
            return sagas
            
        except Exception as e:
            logger.error(f"Failed to load sagas from JSON: {str(e)}")
            return []
            
    def save_sagas_to_database(self, sagas: List[SagaModel]) -> bool:
        """Save saga data to database.
        
        Args:
            sagas: List of SagaModel objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving {len(sagas)} sagas to database")
            
            # Convert SagaModel objects to ScrapingResult format
            scraping_results = []
            for saga in sagas:
                result = ScrapingResult(
                    success=True,
                    data=saga.model_dump(),
                    url=f"https://onepiece.fandom.com/wiki/Story_Arcs"
                )
                scraping_results.append(result)
                
            # Use database manager to save
            success = self.db_manager.load_sagas_from_scraped_data(scraping_results)
            
            if success:
                logger.success("Successfully saved sagas to database")
            else:
                logger.error("Failed to save sagas to database")
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to save sagas to database: {str(e)}")
            return False
            
    def process_saga_data(self, sagas_results: List[ScrapingResult], 
                         output_json: Optional[str] = None,
                         save_to_db: bool = True) -> List[SagaModel]:
        """Process saga data end-to-end: validate, save, and load to database.
        
        Args:
            sagas_results: List of ScrapingResult objects containing saga data
            output_json: Optional path to save JSON file
            save_to_db: Whether to save to database
            
        Returns:
            List of validated SagaModel objects
        """
        logger.info("Starting end-to-end saga data processing")
        
        # Parse and validate
        validated_sagas = self.parse_and_validate_sagas(sagas_results)
        
        if not validated_sagas:
            logger.warning("No valid sagas found after validation")
            return []
            
        # Save to JSON if requested
        if output_json:
            self.save_sagas_to_json(validated_sagas, output_json)
            
        # Save to database if requested
        if save_to_db:
            self.save_sagas_to_database(validated_sagas)
            
        logger.success(f"Completed saga data processing. Total sagas: {len(validated_sagas)}")
        return validated_sagas
        
    def create_saga_arc_mapping(self, sagas: List[SagaModel], 
                               manual_mapping: Optional[Dict[str, List[str]]] = None) -> Dict[str, str]:
        """Create mapping from arc_id to saga_id.
        
        Args:
            sagas: List of SagaModel objects
            manual_mapping: Optional manual mapping of saga_id to list of arc_ids
            
        Returns:
            Dictionary mapping arc_id to saga_id
        """
        mapping = {}
        
        if manual_mapping:
            logger.info("Using manual saga-arc mapping")
            for saga_id, arc_ids in manual_mapping.items():
                for arc_id in arc_ids:
                    mapping[arc_id] = saga_id
        else:
            logger.info("Creating saga-arc mapping based on chapter ranges")
            # This would require more complex logic to match arcs to sagas
            # based on chapter ranges - for now, return empty mapping
            logger.warning("Automatic saga-arc mapping not implemented yet")
            
        logger.info(f"Created mapping for {len(mapping)} arcs")
        return mapping

"""Modern arc parser for processing scraped arc data."""

import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from loguru import logger

from ..models.data import ArcModel, ScrapingResult
from ..database.operations import DatabaseManager


class ArcParser:
    """Modern arc parser with data validation and database integration."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the arc parser.
        
        Args:
            db_manager: Database manager instance. If None, creates a new one.
        """
        self.db_manager = db_manager or DatabaseManager()
        
    def parse_and_validate_arcs(self, arcs_results: List[ScrapingResult]) -> List[ArcModel]:
        """Parse and validate arc data from scraping results.
        
        Args:
            arcs_results: List of ScrapingResult objects containing arc data
            
        Returns:
            List of validated ArcModel objects
        """
        validated_arcs = []
        
        logger.info(f"Parsing and validating {len(arcs_results)} arc results")
        
        for result in arcs_results:
            if not result.success or not result.data:
                logger.warning(f"Skipping failed result: {result.error}")
                continue
                
            try:
                # Create ArcModel from the data
                arc_model = ArcModel(**result.data)
                validated_arcs.append(arc_model)
                
            except Exception as e:
                logger.warning(f"Failed to validate arc data: {str(e)}")
                continue
                
        logger.success(f"Successfully validated {len(validated_arcs)} arcs")
        return validated_arcs
        
    def save_arcs_to_json(self, arcs: List[ArcModel], output_path: str) -> bool:
        """Save validated arc data to JSON file.
        
        Args:
            arcs: List of validated ArcModel objects
            output_path: Path to save JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving {len(arcs)} arcs to {output_path}")
            
            # Convert to list of dictionaries
            arcs_data = [arc.model_dump() for arc in arcs]
            
            # Create output directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Save to JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(arcs_data, f, indent=2, ensure_ascii=False)
                
            logger.success(f"Successfully saved arcs to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save arcs to JSON: {str(e)}")
            return False
            
    def load_arcs_from_json(self, json_path: str) -> List[ArcModel]:
        """Load arc data from JSON file.
        
        Args:
            json_path: Path to JSON file containing arc data
            
        Returns:
            List of ArcModel objects
        """
        try:
            logger.info(f"Loading arcs from {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as f:
                arcs_data = json.load(f)
                
            arcs = []
            for arc_data in arcs_data:
                try:
                    arc = ArcModel(**arc_data)
                    arcs.append(arc)
                except Exception as e:
                    logger.warning(f"Failed to load arc: {str(e)}")
                    continue
                    
            logger.success(f"Successfully loaded {len(arcs)} arcs from JSON")
            return arcs
            
        except Exception as e:
            logger.error(f"Failed to load arcs from JSON: {str(e)}")
            return []
            
    def save_arcs_to_database(self, arcs: List[ArcModel]) -> bool:
        """Save arc data to database.
        
        Args:
            arcs: List of ArcModel objects
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving {len(arcs)} arcs to database")
            
            # Convert ArcModel objects to ScrapingResult format
            scraping_results = []
            for arc in arcs:
                result = ScrapingResult(
                    success=True,
                    data=arc.model_dump(),
                    url=f"https://onepiece.fandom.com/wiki/Story_Arcs"
                )
                scraping_results.append(result)
                
            # Use database manager to save
            success = self.db_manager.load_arcs_from_scraped_data(scraping_results)
            
            if success:
                logger.success("Successfully saved arcs to database")
            else:
                logger.error("Failed to save arcs to database")
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to save arcs to database: {str(e)}")
            return False
            
    def process_arc_data(self, arcs_results: List[ScrapingResult], 
                        output_json: Optional[str] = None,
                        save_to_db: bool = True) -> List[ArcModel]:
        """Process arc data end-to-end: validate, save, and load to database.
        
        Args:
            arcs_results: List of ScrapingResult objects containing arc data
            output_json: Optional path to save JSON file
            save_to_db: Whether to save to database
            
        Returns:
            List of validated ArcModel objects
        """
        logger.info("Starting end-to-end arc data processing")
        
        # Parse and validate
        validated_arcs = self.parse_and_validate_arcs(arcs_results)
        
        if not validated_arcs:
            logger.warning("No valid arcs found after validation")
            return []
            
        # Save to JSON if requested
        if output_json:
            self.save_arcs_to_json(validated_arcs, output_json)
            
        # Save to database if requested
        if save_to_db:
            self.save_arcs_to_database(validated_arcs)
            
        logger.success(f"Completed arc data processing. Total arcs: {len(validated_arcs)}")
        return validated_arcs
        
    def link_arcs_to_sagas(self, arcs: List[ArcModel], saga_arc_mapping: Dict[str, str]) -> List[ArcModel]:
        """Link arcs to their respective sagas.

        Args:
            arcs: List of ArcModel objects
            saga_arc_mapping: Dictionary mapping arc_id to saga_id

        Returns:
            List of ArcModel objects with saga_id populated
        """
        logger.info(f"Linking {len(arcs)} arcs to sagas")

        linked_arcs = []
        for arc in arcs:
            if arc.arc_id in saga_arc_mapping:
                arc.saga_id = saga_arc_mapping[arc.arc_id]
                logger.debug(f"Linked arc '{arc.title}' to saga '{arc.saga_id}'")
            else:
                logger.debug(f"No saga mapping found for arc '{arc.title}'")

            linked_arcs.append(arc)

        logger.success(f"Successfully linked arcs to sagas")
        return linked_arcs

    def auto_link_arcs_to_sagas(self, arcs: List[ArcModel], sagas_json_path: str) -> List[ArcModel]:
        """Automatically link arcs to sagas based on chapter ranges.

        Args:
            arcs: List of ArcModel objects
            sagas_json_path: Path to JSON file containing saga data

        Returns:
            List of ArcModel objects with saga_id populated
        """
        logger.info(f"Auto-linking {len(arcs)} arcs to sagas based on chapter ranges")

        try:
            # Load saga data
            with open(sagas_json_path, 'r', encoding='utf-8') as f:
                sagas_data = json.load(f)

            # Convert to SagaModel for validation (optional, but good practice)
            from ..models.data import SagaModel
            sagas = []
            for saga_data in sagas_data:
                try:
                    saga = SagaModel(**saga_data)
                    sagas.append(saga)
                except Exception as e:
                    logger.warning(f"Failed to load saga: {str(e)}")
                    continue

            logger.info(f"Loaded {len(sagas)} sagas for linking")

            # Link each arc to its saga based on chapter range
            linked_count = 0
            for arc in arcs:
                if arc.start_chapter is None or arc.end_chapter is None:
                    logger.debug(f"Arc '{arc.title}' has no chapter range, skipping")
                    continue

                # Find the saga that contains this arc's chapter range
                for saga in sagas:
                    if (saga.start_chapter <= arc.start_chapter and
                        saga.end_chapter >= arc.end_chapter):
                        arc.saga_id = saga.saga_id
                        linked_count += 1
                        logger.debug(f"Linked arc '{arc.title}' (ch {arc.start_chapter}-{arc.end_chapter}) to saga '{saga.title}' (ch {saga.start_chapter}-{saga.end_chapter})")
                        break
                else:
                    logger.warning(f"No saga found for arc '{arc.title}' (chapters {arc.start_chapter}-{arc.end_chapter})")

            logger.success(f"Successfully auto-linked {linked_count}/{len(arcs)} arcs to sagas")
            return arcs

        except Exception as e:
            logger.error(f"Failed to auto-link arcs to sagas: {str(e)}")
            return arcs

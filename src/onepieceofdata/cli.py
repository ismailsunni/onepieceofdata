"""Command-line interface for One Piece of Data pipeline."""

import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from .config import settings
from .utils import setup_logging, get_logger
from .scrapers.chapter import ChapterScraper
from .scrapers.arc import ArcScraper
from .scrapers.saga import SagaScraper
from .parsers.arc import ArcParser
from .parsers.saga import SagaParser
from .models import ScrapingResult


# Setup logging for CLI
setup_logging()
cli_logger = get_logger(__name__)


@click.group()
@click.option(
    "--log-level",
    default=None,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set logging level"
)
@click.option(
    "--log-file",
    type=click.Path(),
    help="Log file path"
)
@click.version_option(version="2.0.0")
def main(log_level: Optional[str], log_file: Optional[str]) -> None:
    """One Piece of Data - A pipeline for scraping and processing One Piece manga data."""
    if log_level or log_file:
        setup_logging(
            log_level=log_level,
            log_file=Path(log_file) if log_file else None
        )
    
    # Ensure required directories exist
    settings.ensure_directories()


@main.command()
@click.option(
    "--start-chapter",
    default=1,
    type=int,
    help="Starting chapter number to scrape"
)
@click.option(
    "--end-chapter", 
    default=None,
    type=int,
    help="Ending chapter number to scrape (default: from config)"
)
@click.option(
    "--parallel",
    is_flag=True,
    help="Enable parallel processing for faster scraping"
)
@click.option(
    "--workers",
    default=None,
    type=int,
    help="Number of parallel workers (default: from config)"
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output file path (default: from config)"
)
def scrape_chapters(start_chapter: int, end_chapter: Optional[int], parallel: bool, 
                   workers: Optional[int], output: Optional[str]) -> None:
    """Scrape chapter data from One Piece Fandom Wiki."""
    try:
        processing_mode = "parallel" if parallel else "sequential"
        cli_logger.info(f"Starting chapter scraping in {processing_mode} mode...")
        
        if parallel:
            actual_workers = workers or settings.max_workers
            cli_logger.info(f"Using parallel processing with {actual_workers} workers")
        
        # Validate inputs
        if start_chapter < 1:
            raise click.BadParameter("Start chapter must be positive")
        
        if end_chapter is None:
            end_chapter = settings.last_chapter
        
        if end_chapter < start_chapter:
            raise click.BadParameter("End chapter must be >= start chapter")
        
        # Determine output path
        output_path = Path(output) if output else settings.chapters_json_path
        
        # Initialize scraper and run
        scraper = ChapterScraper()
        
        if parallel:
            # Use the new parallel method
            chapters = scraper.scrape_chapters(
                start_chapter=start_chapter,
                end_chapter=end_chapter,
                use_parallel=True,
                max_workers=workers
            )
            failed_chapters = []  # Failed chapters are handled internally in parallel mode
        else:
            # Use sequential mode with progress bar
            with click.progressbar(
                length=end_chapter - start_chapter + 1,
                label=f"Scraping chapters {start_chapter}-{end_chapter}"
            ) as bar:
                chapters = []
                failed_chapters = []
                
                for chapter_num in range(start_chapter, end_chapter + 1):
                    result = scraper.scrape_chapter(chapter_num)
                    if result.success:
                        chapters.append(result.data)
                    else:
                        failed_chapters.append(chapter_num)
                        cli_logger.error(f"Failed to scrape chapter {chapter_num}")
                    
                    bar.update(1)
        
        # Save results
        scraper.save_chapters(chapters, output_path)
        
        # Report results
        click.echo(f"\n✅ Successfully scraped {len(chapters)} chapters")
        click.echo(f"📁 Saved to: {output_path}")
        
        if failed_chapters:
            click.echo(f"❌ Failed chapters: {failed_chapters}")
            sys.exit(1)
            
    except Exception as e:
        cli_logger.error(f"Chapter scraping failed: {e}")
        click.echo(f"❌ Error: {e}", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--input-file",
    type=click.Path(exists=True),
    help="Character CSV file path (default: from config)"
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output file path (default: from config)"
)
@click.option(
    "--parallel",
    is_flag=True,
    help="Enable parallel processing for faster character scraping"
)
@click.option(
    "--workers",
    default=None,
    type=int,
    help="Number of parallel workers (default: from config)"
)
def scrape_characters(input_file: Optional[str], output: Optional[str], 
                     parallel: bool, workers: Optional[int]) -> None:
    """Scrape character details from One Piece Fandom Wiki."""
    from .scrapers.character import CharacterScraper
    
    processing_mode = "parallel" if parallel else "sequential"
    click.echo(f"🏴‍☠️ Starting character scraping in {processing_mode} mode...")
    
    if parallel:
        actual_workers = workers or settings.max_workers
        click.echo(f"Using parallel processing with {actual_workers} workers")
    
    scraper = CharacterScraper()
    
    # Set default paths
    if input_file is None:
        input_file = settings.data_dir / "characters.csv"
    if output is None:
        output = settings.data_dir / "characters_detail.json"
    
    # Check if input file exists
    if not Path(input_file).exists():
        click.echo(f"❌ Input file not found: {input_file}")
        click.echo("💡 Try running 'uv run onepieceofdata scrape-chapters' first to generate character data")
        return
    
    try:
        # Load characters from CSV
        characters_data = scraper.load_characters_from_csv(str(input_file))
        if not characters_data:
            click.echo("❌ No characters found in input file")
            return
        
        if parallel:
            # Use parallel processing
            results = scraper.scrape_characters_parallel(characters_data, workers)
        else:
            # Use sequential processing with progress bar
            with click.progressbar(
                length=len(characters_data),
                label=f"Scraping {len(characters_data)} characters"
            ) as bar:
                results = []
                for i, character_data in enumerate(characters_data):
                    result = scraper.scrape_character(character_data)
                    results.append(result)
                    bar.update(1)
                    
                    # Brief pause to be respectful to the server
                    import time
                    time.sleep(0.5)
        
        # Export results
        success = scraper.export_to_json(results, str(output))
        
        # Show summary
        successful_count = sum(1 for r in results if r.success)
        click.echo(f"\n✅ Character scraping completed!")
        click.echo(f"📊 Results: {successful_count}/{len(results)} characters scraped successfully")
        
        if success:
            click.echo(f"� Data saved to: {output}")
        else:
            click.echo("❌ Failed to save results")
            
    except Exception as e:
        click.echo(f"❌ Character scraping failed: {str(e)}")


@main.command()
@click.option(
    "--start-volume",
    type=int,
    default=1,
    help="First volume to scrape"
)
@click.option(
    "--end-volume",
    type=int,
    help="Last volume to scrape (default: from config)"
)
@click.option(
    "--parallel",
    is_flag=True,
    help="Enable parallel processing (note: volumes are scraped from single page, so this has limited benefit)"
)
@click.option(
    "--workers",
    default=None,
    type=int,
    help="Number of parallel workers (default: from config)"
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output file path (default: from config)"
)
def scrape_volumes(start_volume: int, end_volume: Optional[int], parallel: bool,
                  workers: Optional[int], output: Optional[str]) -> None:
    """Scrape volume information from One Piece Fandom Wiki."""
    from .scrapers.volume import VolumeScraper
    
    processing_mode = "parallel" if parallel else "sequential"
    click.echo(f"📚 Starting volume scraping in {processing_mode} mode...")
    
    if parallel:
        click.echo("ℹ️  Note: Volume scraping uses a single page source, so parallel processing has limited benefit")
    
    scraper = VolumeScraper()
    
    # Set defaults
    if end_volume is None:
        end_volume = settings.last_volume
    if output is None:
        output = settings.data_dir / "volumes.json"
    
    click.echo(f"🎯 Scraping volumes {start_volume} to {end_volume}")
    
    try:
        # Show progress bar
        with click.progressbar(
            length=end_volume - start_volume + 1,
            label=f"Scraping volumes {start_volume}-{end_volume}"
        ) as bar:
            results = scraper.scrape_volumes(start_volume, end_volume)
            bar.update(len(results))
        
        # Export results
        success = scraper.export_to_json(results, str(output))
        
        # Show summary
        successful_count = sum(1 for r in results if r.success)
        click.echo(f"\n✅ Volume scraping completed!")
        click.echo(f"📊 Results: {successful_count}/{len(results)} volumes scraped successfully")
        
        if success:
            click.echo(f"💾 Data saved to: {output}")
        else:
            click.echo("❌ Failed to save results")
            
    except Exception as e:
        click.echo(f"❌ Volume scraping failed: {str(e)}")


@main.command()
@click.option(
    "--database-path",
    type=click.Path(),
    help="Database file path (default: from config)"
)
@click.option(
    "--chapters-file",
    type=click.Path(exists=True),
    help="Chapters JSON file path (default: from config)"
)
@click.option(
    "--volumes-file",
    type=click.Path(exists=True),
    help="Volumes JSON file path (default: from config)"
)
@click.option(
    "--characters-file",
    type=click.Path(exists=True),
    help="Characters JSON file path (default: from config)"
)
@click.option(
    "--create-tables",
    is_flag=True,
    help="Create database tables (will drop existing tables)"
)
def parse(
    database_path: Optional[str],
    chapters_file: Optional[str],
    volumes_file: Optional[str],
    characters_file: Optional[str],
    create_tables: bool
) -> None:
    """Parse scraped data and load into DuckDB database."""
    from .database.operations import DatabaseManager
    
    click.echo("�️  Starting data parsing and database loading...")
    
    # Set defaults
    if database_path is None:
        database_path = settings.database_path
    if chapters_file is None:
        chapters_file = settings.data_dir / "chapters.json"
    if volumes_file is None:
        volumes_file = settings.data_dir / "volumes.json"
    if characters_file is None:
        characters_file = settings.data_dir / "characters_detail.json"
    
    try:
        with DatabaseManager(str(database_path)) as db:
            # Create tables if requested
            if create_tables:
                click.echo("🏗️  Creating database tables...")
                db.create_tables()
                click.echo("✅ Database tables created")
            
            # Load data files
            success_count = 0
            total_files = 0
            
            # Load volumes first (required by foreign key constraints)
            if Path(volumes_file).exists():
                total_files += 1
                click.echo(f"� Loading volumes from {volumes_file}")
                if db.load_volumes_from_json(str(volumes_file)):
                    success_count += 1
                    click.echo("✅ Volumes loaded successfully")
                else:
                    click.echo("❌ Failed to load volumes")
            else:
                click.echo(f"⚠️  Volumes file not found: {volumes_file}")
            
            # Load chapters (references volumes)
            if Path(chapters_file).exists():
                total_files += 1
                click.echo(f"� Loading chapters from {chapters_file}")
                if db.load_chapters_from_json(str(chapters_file)):
                    success_count += 1
                    click.echo("✅ Chapters loaded successfully")
                else:
                    click.echo("❌ Failed to load chapters")
            else:
                click.echo(f"⚠️  Chapters file not found: {chapters_file}")
            
            # Load characters
            if Path(characters_file).exists():
                total_files += 1
                click.echo(f"� Loading characters from {characters_file}")
                if db.load_characters_from_json(str(characters_file)):
                    success_count += 1
                    click.echo("✅ Characters loaded successfully")
                else:
                    click.echo("❌ Failed to load characters")
            else:
                click.echo(f"⚠️  Characters file not found: {characters_file}")
            
            # Show database stats
            stats = db.get_database_stats()
            if stats:
                click.echo(f"\n📊 Database Statistics:")
                click.echo(f"   📖 Chapters: {stats.get('chapter', 0)}")
                click.echo(f"   📚 Volumes: {stats.get('volume', 0)}")
                click.echo(f"   👥 Characters: {stats.get('character', 0)}")
                click.echo(f"   🔗 Character-Chapter links: {stats.get('coc', 0)}")
            
            click.echo(f"\n✅ Data parsing completed!")
            click.echo(f"📊 Results: {success_count}/{total_files} files loaded successfully")
            click.echo(f"💾 Database saved to: {database_path}")
            
    except Exception as e:
        click.echo(f"❌ Data parsing failed: {str(e)}")


@main.command()
@click.option(
    "--format",
    "export_format",
    default="csv",
    type=click.Choice(["csv", "json"], case_sensitive=False),
    help="Export format"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    help="Output directory for exported files"
)
@click.option(
    "--database-path",
    type=click.Path(),
    help="Database file path (default: from config)"
)
def export(export_format: str, output_dir: Optional[str], database_path: Optional[str]) -> None:
    """Export data from database to various formats."""
    from .database.operations import DatabaseManager
    
    click.echo(f"� Exporting data in {export_format.upper()} format...")
    
    # Set defaults
    if database_path is None:
        database_path = settings.database_path
    if output_dir is None:
        output_dir = settings.data_dir / "exports"
    
    # Check if database exists
    if not Path(database_path).exists():
        click.echo(f"❌ Database not found: {database_path}")
        click.echo("� Try running 'uv run onepieceofdata parse --create-tables' first")
        return
    
    try:
        with DatabaseManager(str(database_path)) as db:
            if export_format == "csv":
                success = db.export_to_csv(str(output_dir))
                if success:
                    click.echo(f"✅ Data exported to CSV files in: {output_dir}")
                else:
                    click.echo("❌ Failed to export to CSV")
            else:
                click.echo(f"🚧 {export_format.upper()} export not yet implemented")
            
    except Exception as e:
        click.echo(f"❌ Export failed: {str(e)}")


@main.command()
def status() -> None:
    """Show pipeline status and configuration."""
    click.echo("📊 One Piece of Data - Pipeline Status")
    click.echo("=" * 40)
    
    click.echo(f"📁 Data Directory: {settings.data_dir}")
    click.echo(f"🗄️  Database Path: {settings.database_path}")
    click.echo(f"📖 Last Chapter: {settings.last_chapter}")
    click.echo(f"📚 Last Volume: {settings.last_volume}")
    
    click.echo("\n📂 Data Files:")
    
    # Check for existing data files
    data_files = {
        "Chapters JSON": settings.chapters_json_path,
        "Volumes JSON": settings.volumes_json_path,
        "Characters JSON": settings.characters_json_path,
        "Chapters CSV": settings.chapters_csv_path,
        "Characters CSV": settings.characters_csv_path,
        "CoC CSV": settings.coc_csv_path,
        "Database": settings.database_path,
    }
    
    for name, path in data_files.items():
        if path.exists():
            size = path.stat().st_size
            size_str = f"({size:,} bytes)"
            click.echo(f"  ✅ {name}: {path} {size_str}")
        else:
            click.echo(f"  ❌ {name}: {path} (missing)")
    
    click.echo(f"\n⚙️  Configuration:")
    click.echo(f"  🔄 Scraping Delay: {settings.scraping_delay}s")
    click.echo(f"  🔁 Max Retries: {settings.max_retries}")
    click.echo(f"  ⏱️  Request Timeout: {settings.request_timeout}s")
    click.echo(f"  📝 Log Level: {settings.log_level}")


@main.command()
def config() -> None:
    """Show current configuration settings."""
    click.echo("⚙️  One Piece of Data - Configuration")
    click.echo("=" * 40)
    
    # Display all settings
    config_items = [
        ("Last Chapter", settings.last_chapter),
        ("Last Volume", settings.last_volume),
        ("Data Directory", settings.data_dir),
        ("Database Path", settings.database_path),
        ("Log Level", settings.log_level),
        ("Log File", settings.log_file),
        ("Scraping Delay", f"{settings.scraping_delay}s"),
        ("Max Retries", settings.max_retries),
        ("Request Timeout", f"{settings.request_timeout}s"),
        ("Base Chapter URL", settings.base_chapter_url),
        ("Base Character URL", settings.base_character_url),
        ("Base Volume URL", settings.base_volume_url),
    ]
    
    for name, value in config_items:
        click.echo(f"{name:20}: {value}")


@main.command()
@click.option(
    "--output",
    type=click.Path(),
    help="Output JSON file path for scraped arc data"
)
@click.option(
    "--save-to-db/--no-save-to-db",
    default=True,
    help="Save scraped data to database"
)
def scrape_arcs(output: Optional[str], save_to_db: bool) -> None:
    """Scrape One Piece story arcs data."""
    cli_logger.info("Starting arc scraping process")
    
    try:
        # Initialize scraper
        scraper = ArcScraper()
        
        # Scrape arc data
        cli_logger.info("Scraping arc data from One Piece wiki")
        arc_results = scraper.scrape_all_arcs()
        
        # Process results
        successful_results = [r for r in arc_results if r.success]
        failed_results = [r for r in arc_results if not r.success]
        
        cli_logger.info(f"Scraping completed: {len(successful_results)} successful, {len(failed_results)} failed")
        
        if successful_results:
            # Initialize parser
            parser = ArcParser()
            
            # Process the data
            validated_arcs = parser.process_arc_data(
                arc_results,
                output_json=output,
                save_to_db=save_to_db
            )
            
            cli_logger.success(f"Successfully processed {len(validated_arcs)} arcs")
            
            if output:
                cli_logger.info(f"Arc data saved to: {output}")
                
            if save_to_db:
                cli_logger.info("Arc data saved to database")
        else:
            cli_logger.warning("No successful arc data found")
            
        # Clean up
        scraper.cleanup()
        
    except Exception as e:
        cli_logger.error(f"Arc scraping failed: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--output",
    type=click.Path(),
    help="Output JSON file path for scraped saga data"
)
@click.option(
    "--save-to-db/--no-save-to-db",
    default=True,
    help="Save scraped data to database"
)
def scrape_sagas(output: Optional[str], save_to_db: bool) -> None:
    """Scrape One Piece story sagas data."""
    cli_logger.info("Starting saga scraping process")
    
    try:
        # Initialize scraper
        scraper = SagaScraper()
        
        # Scrape saga data
        cli_logger.info("Scraping saga data from One Piece wiki")
        saga_results = scraper.scrape_all_sagas()
        
        # Process results
        successful_results = [r for r in saga_results if r.success]
        failed_results = [r for r in saga_results if not r.success]
        
        cli_logger.info(f"Scraping completed: {len(successful_results)} successful, {len(failed_results)} failed")
        
        if successful_results:
            # Initialize parser
            parser = SagaParser()
            
            # Process the data
            validated_sagas = parser.process_saga_data(
                saga_results,
                output_json=output,
                save_to_db=save_to_db
            )
            
            cli_logger.success(f"Successfully processed {len(validated_sagas)} sagas")
            
            if output:
                cli_logger.info(f"Saga data saved to: {output}")
                
            if save_to_db:
                cli_logger.info("Saga data saved to database")
        else:
            cli_logger.warning("No successful saga data found")
            
        # Clean up
        scraper.cleanup()
        
    except Exception as e:
        cli_logger.error(f"Saga scraping failed: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--arcs-output",
    type=click.Path(),
    help="Output JSON file path for scraped arc data"
)
@click.option(
    "--sagas-output", 
    type=click.Path(),
    help="Output JSON file path for scraped saga data"
)
@click.option(
    "--save-to-db/--no-save-to-db",
    default=True,
    help="Save scraped data to database"
)
def scrape_story_structure(arcs_output: Optional[str], sagas_output: Optional[str], save_to_db: bool) -> None:
    """Scrape both arcs and sagas story structure data."""
    cli_logger.info("Starting comprehensive story structure scraping")
    
    try:
        # Scrape sagas first
        cli_logger.info("Step 1: Scraping saga data")
        saga_scraper = SagaScraper()
        saga_results = saga_scraper.scrape_all_sagas()
        
        successful_saga_results = [r for r in saga_results if r.success]
        cli_logger.info(f"Saga scraping completed: {len(successful_saga_results)} successful")
        
        # Scrape arcs
        cli_logger.info("Step 2: Scraping arc data")
        arc_scraper = ArcScraper()
        arc_results = arc_scraper.scrape_all_arcs()
        
        successful_arc_results = [r for r in arc_results if r.success]
        cli_logger.info(f"Arc scraping completed: {len(successful_arc_results)} successful")
        
        # Process sagas
        if successful_saga_results:
            saga_parser = SagaParser()
            validated_sagas = saga_parser.process_saga_data(
                saga_results,
                output_json=sagas_output,
                save_to_db=save_to_db
            )
            cli_logger.success(f"Processed {len(validated_sagas)} sagas")
        else:
            validated_sagas = []
            cli_logger.warning("No successful saga data found")
            
        # Process arcs
        if successful_arc_results:
            arc_parser = ArcParser()
            validated_arcs = arc_parser.process_arc_data(
                arc_results,
                output_json=arcs_output,
                save_to_db=save_to_db
            )
            cli_logger.success(f"Processed {len(validated_arcs)} arcs")
        else:
            validated_arcs = []
            cli_logger.warning("No successful arc data found")
            
        # Summary
        cli_logger.success(f"Story structure scraping completed: {len(validated_sagas)} sagas, {len(validated_arcs)} arcs")
        
        if save_to_db:
            cli_logger.info("All data saved to database")
            
        # Clean up
        saga_scraper.cleanup()
        arc_scraper.cleanup()
        
    except Exception as e:
        cli_logger.error(f"Story structure scraping failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

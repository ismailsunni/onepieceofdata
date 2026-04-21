"""Command-line interface for One Piece of Data pipeline."""

import json
import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from ..config import settings
from ..utils import setup_logging, get_logger
from ..scrapers.chapter import ChapterScraper
from ..scrapers.arc import ArcScraper
from ..scrapers.saga import SagaScraper
from ..parsers.arc import ArcParser
from ..parsers.saga import SagaParser
from ..models import ScrapingResult
from ..database.operations import DatabaseManager


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
            # Use parallel processing
            chapters = scraper.scrape_chapters(
                start_chapter=start_chapter,
                end_chapter=end_chapter,
                use_parallel=True,
                max_workers=workers
            )
            failed_chapters = []  # Failed chapters are handled internally
        else:
            # Use batch API mode by default (much faster)
            # This scrapes 50 chapters per API request
            cli_logger.info("Using batch API mode for faster scraping")
            chapters = scraper.scrape_chapters(
                start_chapter=start_chapter,
                end_chapter=end_chapter,
                use_batch=True
            )
            failed_chapters = []  # Failed chapters are tracked internally
        
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
    "--batch",
    is_flag=True,
    default=True,
    help="Enable batch API queries (faster, default: True)"
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
def scrape_chapters_api(start_chapter: int, end_chapter: Optional[int], parallel: bool,
                       batch: bool, workers: Optional[int], output: Optional[str]) -> None:
    """Scrape chapter data using Fandom MediaWiki API (bypasses Cloudflare)."""
    from ..scrapers.chapter_api import ChapterScraperAPI

    try:
        # Determine processing mode
        if parallel:
            processing_mode = "parallel"
            actual_workers = workers or settings.max_workers
            cli_logger.info(f"Using parallel API processing with {actual_workers} workers")
        elif batch:
            processing_mode = "batch API"
            cli_logger.info("Using batch API queries (up to 50 chapters per request)")
        else:
            processing_mode = "sequential API"
            cli_logger.info("Using sequential API processing")

        cli_logger.info(f"Starting chapter scraping via API in {processing_mode} mode...")

        # Validate inputs
        if start_chapter < 1:
            raise click.BadParameter("Start chapter must be positive")

        if end_chapter is None:
            end_chapter = settings.last_chapter

        if end_chapter < start_chapter:
            raise click.BadParameter("End chapter must be >= start chapter")

        # Determine output path
        output_path = Path(output) if output else settings.chapters_json_path

        # Initialize API scraper and run
        scraper = ChapterScraperAPI()

        if parallel:
            # Parallel processing
            chapters = scraper.scrape_chapters(
                start_chapter=start_chapter,
                end_chapter=end_chapter,
                use_parallel=True,
                max_workers=workers
            )
        elif batch:
            # Batch API queries (faster)
            chapters = scraper.scrape_chapters(
                start_chapter=start_chapter,
                end_chapter=end_chapter,
                use_batch=True
            )
        else:
            # Sequential processing with progress bar
            total_chapters = end_chapter - start_chapter + 1
            with click.progressbar(
                length=total_chapters,
                label=f"Scraping chapters {start_chapter}-{end_chapter} via API"
            ) as bar:
                chapters = []
                for chapter_num in range(start_chapter, end_chapter + 1):
                    result = scraper.scrape_chapter(chapter_num)
                    if result.success:
                        chapters.append(result.data)
                    else:
                        cli_logger.error(f"Failed to scrape chapter {chapter_num}: {result.error}")
                    bar.update(1)

        # Save results
        scraper.save_chapters(chapters, output_path)

        # Report results
        click.echo(f"\n✅ Successfully scraped {len(chapters)} chapters via API")
        click.echo(f"📁 Saved to: {output_path}")

        total_expected = end_chapter - start_chapter + 1
        if len(chapters) < total_expected:
            failed_count = total_expected - len(chapters)
            click.echo(f"⚠️  {failed_count} chapters failed")
            sys.exit(1)

    except Exception as e:
        cli_logger.error(f"API chapter scraping failed: {e}")
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
    from ..scrapers.character import CharacterScraper
    
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
    from ..scrapers.volume import VolumeScraper
    
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
@click.option(
    "--auto-sync",
    is_flag=True,
    default=True,
    help="Auto-sync character appearances after loading CoC data (default: True)"
)
@click.option(
    "--no-auto-sync",
    is_flag=True,
    default=False,
    help="Skip auto-sync of character appearances"
)
def parse(
    database_path: Optional[str],
    chapters_file: Optional[str],
    volumes_file: Optional[str],
    characters_file: Optional[str],
    create_tables: bool,
    auto_sync: bool,
    no_auto_sync: bool
) -> None:
    """Parse scraped data and load into DuckDB database."""
    from ..database.operations import DatabaseManager
    
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
            coc_count = 0
            if stats:
                click.echo(f"\n📊 Database Statistics:")
                click.echo(f"   📖 Chapters: {stats.get('chapter', 0)}")
                click.echo(f"   📚 Volumes: {stats.get('volume', 0)}")
                click.echo(f"   👥 Characters: {stats.get('character', 0)}")
                coc_count = stats.get('coc', 0)
                click.echo(f"   🔗 Character-Chapter links: {coc_count}")

            # Auto-sync character appearances if CoC data was loaded
            should_sync = auto_sync and not no_auto_sync and coc_count > 0
            if should_sync:
                click.echo("\n🔄 Auto-syncing character appearances...")
                try:
                    sync_stats = db.sync_character_appearances()
                    click.echo(f"✅ Synced {sync_stats['characters_updated']:,} characters with {sync_stats['total_appearances']:,} appearances")
                except Exception as e:
                    click.echo(f"⚠️  Auto-sync failed: {str(e)}")
                    click.echo("   You can manually run: make sync-character-appearances")

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
    from ..database.operations import DatabaseManager
    
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

    # Show database table counts if database exists
    if settings.database_path.exists():
        click.echo("\n📊 Database Table Counts:")
        try:
            with DatabaseManager() as db:
                stats = db.get_database_stats()
                click.echo(f"  📖 Chapters: {stats.get('chapter', 0):,}")
                click.echo(f"  📚 Volumes: {stats.get('volume', 0):,}")
                click.echo(f"  👥 Characters: {stats.get('character', 0):,}")
                click.echo(f"  📝 CoC entries: {stats.get('coc', 0):,}")
                click.echo(f"  📦 CoV entries: {stats.get('cov', 0):,}")
                click.echo(f"  🏴‍☠️ Arcs: {stats.get('arc', 0):,}")
                click.echo(f"  📖 Sagas: {stats.get('saga', 0):,}")
        except Exception as e:
            click.echo(f"  ❌ Error reading database: {str(e)}")

    click.echo(f"\n⚙️  Configuration:")
    click.echo(f"  🔄 Scraping Delay: {settings.scraping_delay}s")
    click.echo(f"  🔁 Max Retries: {settings.max_retries}")
    click.echo(f"  ⏱️  Request Timeout: {settings.request_timeout}s")
    click.echo(f"  📝 Log Level: {settings.log_level}")


@main.command()
def db_status() -> None:
    """Show database content status and latest data."""
    click.echo("🗄️  One Piece of Data - Database Status")
    click.echo("=" * 45)
    
    try:
        with DatabaseManager() as db:
            # Get database stats
            stats = db.get_database_stats()
            
            click.echo("📊 Table Row Counts:")
            click.echo(f"  📖 Chapters: {stats.get('chapter', 0):,}")
            click.echo(f"  📚 Volumes: {stats.get('volume', 0):,}")
            click.echo(f"  👥 Characters: {stats.get('character', 0):,}")
            click.echo(f"  📝 CoC entries: {stats.get('coc', 0):,}")
            click.echo(f"  � CoV entries: {stats.get('cov', 0):,}")
            click.echo(f"  �🏴‍☠️ Arcs: {stats.get('arc', 0):,}")
            click.echo(f"  📖 Sagas: {stats.get('saga', 0):,}")
            
            click.echo("\n📖 Latest Chapter:")
            try:
                latest_chapter = db.query("""
                    SELECT number, title 
                    FROM chapter 
                    WHERE number IS NOT NULL 
                    ORDER BY number DESC 
                    LIMIT 1
                """)
                if not latest_chapter.empty:
                    chapter_num = latest_chapter.iloc[0]['number']
                    chapter_title = latest_chapter.iloc[0]['title']
                    click.echo(f"  Chapter {chapter_num}: {chapter_title}")
                else:
                    click.echo("  No chapters found")
            except Exception as e:
                click.echo(f"  Error retrieving latest chapter: {str(e)}")
            
            click.echo("\n📚 Latest Volume:")
            try:
                latest_volume = db.query("""
                    SELECT number, title 
                    FROM volume 
                    WHERE number IS NOT NULL 
                    ORDER BY number DESC 
                    LIMIT 1
                """)
                if not latest_volume.empty:
                    volume_num = latest_volume.iloc[0]['number']
                    volume_title = latest_volume.iloc[0]['title']
                    click.echo(f"  Volume {volume_num}: {volume_title}")
                else:
                    click.echo("  No volumes found")
            except Exception as e:
                click.echo(f"  Error retrieving latest volume: {str(e)}")
            
            click.echo("\n📝 Latest Chapter CoC:")
            try:
                latest_coc = db.query("""
                    SELECT c.number, coc.character
                    FROM coc 
                    JOIN chapter c ON coc.chapter = c.number
                    WHERE c.number IS NOT NULL 
                    ORDER BY c.number DESC, coc.character
                    LIMIT 1
                """)
                if not latest_coc.empty:
                    chapter_num = latest_coc.iloc[0]['number']
                    click.echo(f"  Chapter {chapter_num} CoC:")
                    
                    # Get all characters for the latest chapter
                    all_coc = db.query(f"""
                        SELECT coc.character
                        FROM coc 
                        JOIN chapter c ON coc.chapter = c.number
                        WHERE c.number = {chapter_num}
                        ORDER BY coc.character
                    """)
                    characters = all_coc['character'].tolist()
                    
                    # Display characters in a compact format
                    if len(characters) <= 10:
                        for char in characters:
                            click.echo(f"    - {char}")
                    else:
                        for char in characters[:8]:
                            click.echo(f"    - {char}")
                        click.echo(f"    ... and {len(characters) - 8} more")
                else:
                    click.echo("  No CoC entries found")
            except Exception as e:
                click.echo(f"  Error retrieving latest CoC: {str(e)}")
            
            click.echo("\n🏴‍☠️ Latest Arc:")
            try:
                latest_arc = db.query("""
                    SELECT title 
                    FROM arc 
                    ORDER BY end_chapter DESC 
                    LIMIT 1
                """)
                if not latest_arc.empty:
                    arc_title = latest_arc.iloc[0]['title']
                    click.echo(f"  {arc_title}")
                else:
                    click.echo("  No arcs found")
            except Exception as e:
                click.echo(f"  Error retrieving latest arc: {str(e)}")
            
            click.echo("\n📖 Latest Saga:")
            try:
                latest_saga = db.query("""
                    SELECT title 
                    FROM saga 
                    ORDER BY end_chapter DESC 
                    LIMIT 1
                """)
                if not latest_saga.empty:
                    saga_title = latest_saga.iloc[0]['title']
                    click.echo(f"  {saga_title}")
                else:
                    click.echo("  No sagas found")
            except Exception as e:
                click.echo(f"  Error retrieving latest saga: {str(e)}")
                
    except Exception as e:
        click.echo(f"❌ Error connecting to database: {str(e)}")
        click.echo("💡 Make sure the database exists. Run 'make run-parse' first.")


@main.command()
@click.option(
    "--format",
    "date_format",
    type=click.Choice(["mm_dd", "full_date"]),
    default="mm_dd",
    help="Format for birth_date column: mm_dd (MM-DD) or full_date (YYYY-MM-DD with year 2000)"
)
def migrate_birth_dates(date_format: str) -> None:
    """Parse birth strings and add birth_date column to character table."""
    click.echo("📅 Migrating birth dates...")
    click.echo(f"Format: {date_format}")
    
    if date_format == "mm_dd":
        click.echo("📝 Using MM-DD format (e.g., '03-09' for March 9th)")
        click.echo("💾 Column type: VARCHAR(5)")
    else:
        click.echo("📝 Using full date format with year 2000 (e.g., '2000-03-09' for March 9th)")
        click.echo("💾 Column type: DATE")
    
    try:
        with DatabaseManager() as db:
            success = db.migrate_birth_dates(date_format)
            
            if success:
                # Show some sample results
                sample_query = """
                    SELECT name, birth, birth_date 
                    FROM character 
                    WHERE birth_date IS NOT NULL 
                    ORDER BY name 
                    LIMIT 10
                """
                samples = db.query(sample_query)
                
                if not samples.empty:
                    click.echo("\n📋 Sample results:")
                    for _, row in samples.iterrows():
                        click.echo(f"  {row['name']}: '{row['birth']}' → '{row['birth_date']}'")
                
                # Show statistics
                stats_query = """
                    SELECT 
                        COUNT(*) as total_characters,
                        COUNT(birth) as with_birth_text,
                        COUNT(birth_date) as with_birth_date,
                        COUNT(birth) - COUNT(birth_date) as failed_parsing
                    FROM character
                """
                stats = db.query(stats_query)
                
                if not stats.empty:
                    row = stats.iloc[0]
                    click.echo(f"\n📊 Migration Statistics:")
                    click.echo(f"  Total characters: {row['total_characters']:,}")
                    click.echo(f"  With birth text: {row['with_birth_text']:,}")
                    click.echo(f"  Successfully parsed: {row['with_birth_date']:,}")
                    click.echo(f"  Failed parsing: {row['failed_parsing']:,}")
                
                click.echo("\n✅ Birth date migration completed!")
            else:
                click.echo("❌ Birth date migration failed!")
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"❌ Error during migration: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--alias-file",
    type=click.Path(exists=True),
    default="data/character_aliases.json",
    help="Path to character alias mapping JSON file"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview changes without modifying the database"
)
def merge_characters(alias_file: str, dry_run: bool) -> None:
    """Merge duplicate characters using alias mapping file.

    This command reads a JSON file with character alias mappings and merges
    duplicate characters in the database. For each alias->canonical pair:
    - Updates all CoC entries to use the canonical character ID
    - Deletes the alias character record

    Use --dry-run to preview changes before applying them.
    """
    click.echo("🔄 Merging duplicate characters...")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made")

    # Load alias mapping
    try:
        with open(alias_file, 'r') as f:
            alias_mapping = json.load(f)

        click.echo(f"📄 Loaded {len(alias_mapping)} alias mappings from {alias_file}")

    except Exception as e:
        click.echo(f"❌ Failed to load alias file: {str(e)}")
        sys.exit(1)

    # Perform merge
    try:
        click.echo(f"\n🔄 Processing {len(alias_mapping)} character merges...\n")

        with DatabaseManager() as db:
            # Progress callback to show what's happening
            def progress_callback(current: int, total: int, alias_id: str, canonical_id: str):
                click.echo(f"  [{current}/{total}] Merging {alias_id} → {canonical_id}")

            stats = db.merge_characters(alias_mapping, dry_run=dry_run, progress_callback=progress_callback)

            click.echo("\n📊 Merge Statistics:")
            click.echo(f"  Characters merged: {stats['characters_merged']:,}")
            click.echo(f"  CoC entries updated: {stats['coc_entries_updated']:,}")

            if stats['errors'] > 0:
                click.echo(f"  ⚠️  Errors encountered: {stats['errors']}")

            if dry_run:
                click.echo("\n💡 Run without --dry-run to apply these changes")
            else:
                click.echo("\n✅ Character merge completed!")

                # Show updated stats
                db_stats = db.get_database_stats()
                click.echo(f"\n📈 Updated Database Counts:")
                click.echo(f"  Characters: {db_stats.get('character', 0):,}")
                click.echo(f"  CoC entries: {db_stats.get('coc', 0):,}")

    except Exception as e:
        click.echo(f"❌ Error during merge: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--chapter",
    type=int,
    default=None,
    help="Chapter number to show characters from (default: latest chapter)"
)
def show_chapter_characters(chapter: Optional[int]) -> None:
    """Show all characters appearing in a specific chapter.

    This command is useful for checking if there are duplicate character IDs
    for the same character (e.g., Akainu vs Sakazuki).
    """
    try:
        with DatabaseManager() as db:
            # Get latest chapter if not specified
            if chapter is None:
                latest = db.query("SELECT MAX(number) as max_chapter FROM chapter")
                if latest.empty or latest.iloc[0]['max_chapter'] is None:
                    click.echo("❌ No chapters found in database")
                    sys.exit(1)
                chapter = int(latest.iloc[0]['max_chapter'])

            # Get chapter title
            chapter_info = db.query(f"SELECT title FROM chapter WHERE number = {chapter}")
            if chapter_info.empty:
                click.echo(f"❌ Chapter {chapter} not found in database")
                sys.exit(1)

            chapter_title = chapter_info.iloc[0]['title']

            # Get characters in this chapter
            result = db.query(f"""
                SELECT
                    coc.character as id,
                    c.name
                FROM coc
                LEFT JOIN character c ON coc.character = c.id
                WHERE coc.chapter = {chapter}
                ORDER BY coc.character
            """)

            if result.empty:
                click.echo(f"📖 Chapter {chapter}: {chapter_title}")
                click.echo("   No characters found")
                return

            click.echo(f"📖 Chapter {chapter}: {chapter_title}")
            click.echo(f"   {len(result)} character(s) appearing:\n")

            # Show character list
            for _, row in result.iterrows():
                char_id = row['id']
                char_name = row['name'] if row['name'] else '(unknown)'
                click.echo(f"   • {char_id:30} | {char_name}")

            # Check for potential duplicates (same name, different IDs)
            name_counts = result.groupby('name').size()
            duplicates = name_counts[name_counts > 1]

            if not duplicates.empty:
                click.echo("\n⚠️  Potential duplicates found (same name, different IDs):")
                for name in duplicates.index:
                    if name:  # Skip None names
                        dup_chars = result[result['name'] == name]
                        ids = ', '.join(dup_chars['id'].tolist())
                        click.echo(f"   • {name}: {ids}")
                click.echo("\n💡 Consider running: uv run onepieceofdata merge-characters --dry-run")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Show detailed progress for each character"
)
def sync_character_appearances(verbose: bool) -> None:
    """Sync character appearance analytics from CoC/CoV tables.

    This command populates appearance_count, chapter_list, arc_list, saga_list,
    and other analytics columns in the character table based on CoC/CoV data.

    Run this after:
    - Initial data load
    - Character merge operations
    - Any manual CoC/CoV changes

    This enables fast analytics queries without needing joins.
    """
    from ..database.operations import DatabaseManager

    click.echo("🔄 Syncing character appearance analytics...\n")

    try:
        with DatabaseManager() as db:
            # Progress callback to show what's happening
            if verbose:
                def progress_callback(current: int, total: int, character_id: str, stats: dict):
                    chapters = stats.get('chapters', 0)
                    volumes = stats.get('volumes', 0)
                    arcs = stats.get('arcs', 0)
                    sagas = stats.get('sagas', 0)
                    first = stats.get('first', '?')
                    last = stats.get('last', '?')
                    click.echo(f"  [{current:,}/{total:,}] {character_id}: {chapters} chapters, {volumes} volumes, {arcs} arcs, {sagas} sagas (#{first}-#{last})")
            else:
                # Show progress every 100 characters
                def progress_callback(current: int, total: int, character_id: str, stats: dict):
                    if current % 100 == 0 or current == total:
                        click.echo(f"  Progress: {current:,}/{total:,} characters...")

            sync_stats = db.sync_character_appearances(progress_callback=progress_callback)

            click.echo("\n📊 Sync Statistics:")
            click.echo(f"  Characters updated: {sync_stats['characters_updated']:,}")
            click.echo(f"  Total appearances: {sync_stats['total_appearances']:,}")

            if sync_stats['characters_updated'] > 0:
                avg_appearances = sync_stats['total_appearances'] / sync_stats['characters_updated']
                click.echo(f"  Average per character: {avg_appearances:.1f}")

            if sync_stats['characters_with_no_appearances'] > 0:
                click.echo(f"  ⚠️  Characters with no appearances: {sync_stats['characters_with_no_appearances']:,}")

            if sync_stats['errors'] > 0:
                click.echo(f"  ⚠️  Errors encountered: {sync_stats['errors']}")

            click.echo("\n✅ Character appearance sync complete!")
            click.echo("\n💡 You can now run analytics queries without joins:")
            click.echo("   SELECT name, appearance_count FROM character ORDER BY appearance_count DESC LIMIT 10;")

    except Exception as e:
        click.echo(f"❌ Error during sync: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Show detailed progress for each character"
)
def sync_cover_appearances(verbose: bool) -> None:
    """Sync character volume cover appearance analytics from CoV table.

    This command populates cover_volume_list and cover_appearance_count columns
    in the character table based on volume cover data from the CoV table.

    Run this after:
    - load-cov (to load cover character data from volumes.json)
    - Any manual CoV changes

    This enables fast analytics queries about which characters appear on volume covers.
    """
    from ..database.operations import DatabaseManager

    click.echo("🔄 Syncing character cover appearance analytics...\n")

    try:
        with DatabaseManager() as db:
            # Progress callback to show what's happening
            if verbose:
                def progress_callback(current: int, total: int, character_id: str, stats: dict):
                    covers = stats.get('covers', 0)
                    click.echo(f"  [{current:,}/{total:,}] {character_id}: {covers} cover appearances")
            else:
                # Show progress every 100 characters
                def progress_callback(current: int, total: int, character_id: str, stats: dict):
                    if current % 100 == 0 or current == total:
                        click.echo(f"  Progress: {current:,}/{total:,} characters...")

            sync_stats = db.sync_cover_appearances(progress_callback=progress_callback)

            click.echo("\n📊 Sync Statistics:")
            click.echo(f"  Total characters: {sync_stats['total_characters']:,}")
            click.echo(f"  Characters with covers: {sync_stats['characters_with_covers']:,}")
            click.echo(f"  Total cover appearances: {sync_stats['total_cover_appearances']:,}")

            if sync_stats['characters_with_covers'] > 0:
                avg_covers = sync_stats['total_cover_appearances'] / sync_stats['characters_with_covers']
                click.echo(f"  Average covers per character: {avg_covers:.1f}")

            if sync_stats['characters_without_covers'] > 0:
                click.echo(f"  Characters without covers: {sync_stats['characters_without_covers']:,}")

            if sync_stats['errors'] > 0:
                click.echo(f"  ⚠️  Errors encountered: {sync_stats['errors']}")

            click.echo("\n✅ Character cover appearance sync complete!")
            click.echo("\n💡 You can now run analytics queries about cover appearances:")
            click.echo("   SELECT name, cover_appearance_count, cover_volume_list FROM character")
            click.echo("   WHERE cover_appearance_count > 0 ORDER BY cover_appearance_count DESC LIMIT 10;")

    except Exception as e:
        click.echo(f"❌ Error during sync: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--database-path",
    default=None,
    help="Path to DuckDB database (uses default if not specified)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview stats without writing to database"
)
def parse_affiliations(database_path: str, dry_run: bool) -> None:
    """Parse character affiliations into structured data.

    Reads affiliation strings from characters_detail.json, parses group names,
    sub-groups, and membership status (current, former, defected, etc.),
    and saves to the character_affiliation table.
    """
    from ..postprocessors.parse_affiliations import parse_affiliations as do_parse

    if database_path is None:
        from ..config.settings import get_settings
        database_path = get_settings().database_path

    action = "Preview" if dry_run else "Parsing"
    click.echo(f"🏴 {action} character affiliations...\n")

    try:
        result = do_parse(database_path, dry_run=dry_run)

        click.echo(f"\n📊 Affiliation Statistics:")
        click.echo(f"  Characters with affiliations: {result['character_count']:,}")
        click.echo(f"  Unique groups: {result['group_count']:,}")
        click.echo(f"  Total entries: {result['entry_count']:,}")
        if result['aliases_merged'] > 0:
            click.echo(f"  Aliases merged: {result['aliases_merged']:,}")
            click.echo(f"  Duplicates removed: {result['duplicates_removed']:,}")
        if result['non_characters_skipped'] > 0:
            click.echo(f"  Non-characters skipped: {result['non_characters_skipped']:,}")

        if dry_run:
            click.echo(f"\n💡 Run without --dry-run to save to database")
        else:
            click.echo(f"\n✅ Affiliations saved to character_affiliation table!")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--mode",
    type=click.Choice(['full', 'incremental']),
    default='incremental',
    help="Export mode: 'full' (complete sync) or 'incremental' (only changes)"
)
@click.option(
    "--tables",
    type=str,
    default=None,
    help="Comma-separated list of tables to export (default: all tables)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be exported without making changes"
)
def export_postgres(mode: str, tables: Optional[str], dry_run: bool) -> None:
    """Export DuckDB database to PostgreSQL (works with local or Supabase).

    This command exports your One Piece data from DuckDB to PostgreSQL.
    Works with both local PostgreSQL and cloud PostgreSQL (Supabase).

    \b
    Export modes:
    - full: Complete sync - drops and recreates all data
    - incremental: Only updates tables that have changed

    \b
    Examples:
    # Full export to PostgreSQL
    uv run onepieceofdata export-postgres --mode full

    # Incremental export (only changed tables)
    uv run onepieceofdata export-postgres --mode incremental

    # Export specific tables only
    uv run onepieceofdata export-postgres --tables chapter,character

    # Preview without changes
    uv run onepieceofdata export-postgres --dry-run
    """
    from ..database.postgres_export import PostgresExporter

    click.echo(f"🚀 Exporting to PostgreSQL (mode: {mode})")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made")

    # Parse tables list
    table_list = None
    if tables:
        table_list = [t.strip() for t in tables.split(',')]
        click.echo(f"📋 Tables to export: {', '.join(table_list)}")

    try:
        # Check configuration
        try:
            postgres_url = settings.postgres_connection_url
            # Parse URL to display correct target (handles both URL and individual settings)
            from urllib.parse import urlparse
            parsed = urlparse(postgres_url)
            target_host = parsed.hostname or settings.postgres_host
            target_port = parsed.port or settings.postgres_port
            target_db = parsed.path.lstrip('/') or settings.postgres_db
            click.echo(f"📍 Target: {target_host}:{target_port}/{target_db}")
        except ValueError as e:
            click.echo(f"❌ Configuration error: {str(e)}")
            click.echo("\n💡 Set PostgreSQL connection details in .env:")
            click.echo("   POSTGRES_HOST=localhost")
            click.echo("   POSTGRES_PORT=5432")
            click.echo("   POSTGRES_DB=onepiece")
            click.echo("   POSTGRES_USER=postgres")
            click.echo("   POSTGRES_PASSWORD=your-password")
            sys.exit(1)

        if dry_run:
            click.echo("\n✅ Configuration valid. Run without --dry-run to proceed.")
            return

        # Perform export
        with PostgresExporter() as exporter:
            result = exporter.export_all(
                mode=mode,
                tables=table_list,
                show_progress=True
            )

            # Display results
            click.echo(f"\n📊 Export Results:")
            click.echo(f"  Status: {result['status']}")
            click.echo(f"  Mode: {result['mode']}")
            click.echo(f"  Tables exported: {result['tables_exported']}")
            click.echo(f"  Total rows: {result['total_rows']:,}")
            click.echo(f"  Duration: {result['duration']:.2f}s")

            if result.get('message'):
                click.echo(f"  {result['message']}")

            # Show per-table results
            if result.get('tables'):
                click.echo("\n📋 Per-table results:")
                for table_result in result['tables']:
                    status_icon = "✅" if table_result['status'] == 'success' else "❌"
                    table_name = table_result['table']
                    rows = table_result['rows_exported']
                    duration = table_result.get('duration', 0)
                    click.echo(f"  {status_icon} {table_name}: {rows:,} rows in {duration:.2f}s")

                    if table_result.get('error'):
                        click.echo(f"     Error: {table_result['error']}")

            click.echo("\n✅ Export completed!")
            click.echo("💡 Run 'uv run onepieceofdata sync-status' to verify")

    except Exception as e:
        click.echo(f"\n❌ Export failed: {str(e)}")
        import traceback
        click.echo(traceback.format_exc())
        sys.exit(1)


@main.command()
def sync_status() -> None:
    """Check PostgreSQL sync status.

    Shows the current state of data synchronization between DuckDB and PostgreSQL.
    Compares row counts to detect if tables are in sync.
    """
    from ..database.postgres_export import PostgresExporter

    click.echo("🔍 Checking PostgreSQL sync status...")

    try:
        with PostgresExporter() as exporter:
            status = exporter.get_sync_status()

            # Display sync status
            last_sync = status.get('last_sync')
            if last_sync:
                click.echo(f"\n⏰ Last sync: {last_sync}")
            else:
                click.echo("\n⚠️  No sync metadata found (never synced)")

            click.echo("\n📊 Table Status:")
            click.echo(f"{'Table':<15} {'DuckDB':<12} {'PostgreSQL':<12} {'Status'}")
            click.echo("-" * 55)

            for table_info in status['tables']:
                if 'error' in table_info:
                    click.echo(f"{table_info['name']:<15} ERROR: {table_info['error']}")
                    continue

                name = table_info['name']
                duckdb_rows = table_info['duckdb_rows']
                postgres_rows = table_info['postgres_rows']
                in_sync = table_info['in_sync']

                status_icon = "✅" if in_sync else "⚠️ "
                status_text = "In sync" if in_sync else "Out of sync"

                click.echo(
                    f"{name:<15} {duckdb_rows:<12,} {postgres_rows:<12,} {status_icon} {status_text}"
                )

            # Summary
            tables_in_sync = sum(1 for t in status['tables'] if t.get('in_sync'))
            total_tables = len(status['tables'])

            click.echo(f"\n📈 Summary: {tables_in_sync}/{total_tables} tables in sync")

            if tables_in_sync < total_tables:
                click.echo("\n💡 Run 'uv run onepieceofdata export-postgres --mode incremental' to sync")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--volumes-json",
    type=click.Path(exists=True),
    help="Path to volumes JSON file (defaults to data/volumes.json)"
)
def load_cov(volumes_json: Optional[str]) -> None:
    """Load character-on-volume (COV) data from volumes JSON file."""
    click.echo("🎨 Loading Character-on-Volume (COV) data...")
    
    # Use default path if not provided
    if volumes_json is None:
        volumes_json = str(settings.volumes_json_path)
    
    click.echo(f"📂 Source: {volumes_json}")
    
    try:
        with DatabaseManager() as db:
            success = db.load_cov_from_json(volumes_json)
            
            if success:
                # Show statistics
                stats_query = """
                    SELECT 
                        COUNT(*) as total_cov_entries,
                        COUNT(DISTINCT volume) as volumes_with_characters,
                        COUNT(DISTINCT character) as unique_characters
                    FROM cov
                """
                stats = db.query(stats_query)
                
                if not stats.empty:
                    row = stats.iloc[0]
                    click.echo(f"\n📊 COV Statistics:")
                    click.echo(f"  Total COV entries: {row['total_cov_entries']:,}")
                    click.echo(f"  Volumes with cover characters: {row['volumes_with_characters']:,}")
                    click.echo(f"  Unique characters on covers: {row['unique_characters']:,}")
                
                # Show some sample data
                sample_query = """
                    SELECT v.number, v.title, cov.character
                    FROM cov 
                    JOIN volume v ON cov.volume = v.number
                    ORDER BY v.number
                    LIMIT 10
                """
                samples = db.query(sample_query)
                
                if not samples.empty:
                    click.echo(f"\n📋 Sample COV entries:")
                    for _, row in samples.iterrows():
                        click.echo(f"  Volume {row['number']} ({row['title']}): {row['character']}")
                
                click.echo("\n✅ COV data loading completed!")
            else:
                click.echo("❌ COV data loading failed!")
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"❌ Error loading COV data: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--input-file",
    type=click.Path(exists=True),
    help="Chapters JSON file path (default: from config)"
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output CSV file path (default: from config)"
)
def extract_characters(input_file: Optional[str], output: Optional[str]) -> None:
    """Extract character list from chapters JSON file."""
    click.echo("👥 Starting character extraction from chapters...")
    
    # Set default paths
    if input_file is None:
        input_file = settings.data_dir / "chapters.json"
    if output is None:
        output = settings.data_dir / "characters.csv"
    
    # Check if input file exists
    if not Path(input_file).exists():
        click.echo(f"❌ Input file not found: {input_file}")
        click.echo("💡 Try running 'uv run onepieceofdata scrape-chapters' first")
        return
    
    try:
        with DatabaseManager() as db:
            success = db.extract_characters_from_chapters(str(input_file), str(output))
            
            if success:
                click.echo(f"✅ Character extraction completed!")
                click.echo(f"💾 Data saved to: {output}")
            else:
                click.echo("❌ Failed to extract characters")
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"❌ Error during character extraction: {str(e)}")
        sys.exit(1)

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
def scrape_story_structure(arcs_output: Optional[str], sagas_output: Optional[str]) -> None:
    """Scrape both arcs and sagas story structure data and save to JSON files."""
    cli_logger.info("Starting comprehensive story structure scraping")
    
    try:
        # Set default output paths if not provided
        if arcs_output is None:
            arcs_output = str(settings.data_dir / "arcs.json")
        if sagas_output is None:
            sagas_output = str(settings.data_dir / "sagas.json")
        
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
        
        # Process and save sagas to JSON
        if successful_saga_results:
            saga_parser = SagaParser()
            validated_sagas = saga_parser.process_saga_data(
                saga_results,
                output_json=sagas_output,
                save_to_db=False  # Don't save to DB in this step
            )
            cli_logger.success(f"Processed and saved {len(validated_sagas)} sagas to {sagas_output}")
        else:
            cli_logger.warning("No successful saga data found")
            
        # Process and save arcs to JSON
        if successful_arc_results:
            arc_parser = ArcParser()
            validated_arcs = arc_parser.process_arc_data(
                arc_results,
                output_json=arcs_output,
                save_to_db=False  # Don't save to DB in this step
            )
            cli_logger.success(f"Processed and saved {len(validated_arcs)} arcs to {arcs_output}")
        else:
            cli_logger.warning("No successful arc data found")
            
        # Clean up
        saga_scraper.cleanup()
        arc_scraper.cleanup()
        
    except Exception as e:
        cli_logger.error(f"Story structure scraping failed: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--arcs-json",
    type=click.Path(exists=True),
    help="Input JSON file path for arc data"
)
@click.option(
    "--sagas-json", 
    type=click.Path(exists=True),
    help="Input JSON file path for saga data"
)
def parse_story_structure(arcs_json: Optional[str], sagas_json: Optional[str]) -> None:
    """Parse arc and saga JSON files and load into database."""
    cli_logger.info("Starting story structure parsing")
    
    try:
        # Set default input paths if not provided
        if arcs_json is None:
            arcs_json = str(settings.data_dir / "arcs.json")
        if sagas_json is None:
            sagas_json = str(settings.data_dir / "sagas.json")
        
        # Validate input files exist
        if not Path(arcs_json).exists():
            cli_logger.error(f"Arcs JSON file not found: {arcs_json}")
            cli_logger.info("Run 'scrape-story-structure' first to generate the JSON files")
            return
        if not Path(sagas_json).exists():
            cli_logger.error(f"Sagas JSON file not found: {sagas_json}")
            cli_logger.info("Run 'scrape-story-structure' first to generate the JSON files")
            return
        
        # Load and parse sagas first (since arcs reference sagas)
        with open(sagas_json, 'r') as f:
            saga_data = json.load(f)
            
        with open(arcs_json, 'r') as f:
            arc_data = json.load(f)
            
        # Initialize parsers
        saga_parser = SagaParser()
        arc_parser = ArcParser()
        
        # Process sagas and save to DB
        validated_sagas = saga_parser.process_saga_data(
            [ScrapingResult(success=True, data=data) for data in saga_data],
            output_json=None,  # Don't save to JSON again
            save_to_db=True
        )
        cli_logger.success(f"Loaded {len(validated_sagas)} sagas into database")

        # Process arcs (without saving to DB yet)
        validated_arcs = arc_parser.process_arc_data(
            [ScrapingResult(success=True, data=data) for data in arc_data],
            output_json=None,  # Don't save to JSON again
            save_to_db=False  # Don't save yet - we need to link to sagas first
        )

        # Auto-link arcs to sagas based on chapter ranges
        cli_logger.info("Linking arcs to sagas based on chapter ranges...")
        linked_arcs = arc_parser.auto_link_arcs_to_sagas(validated_arcs, sagas_json)

        # Now save the linked arcs to database
        arc_parser.save_arcs_to_database(linked_arcs)
        cli_logger.success(f"Loaded {len(linked_arcs)} arcs into database")
        
        cli_logger.success(f"Story structure parsing completed: {len(validated_sagas)} sagas, {len(validated_arcs)} arcs loaded into database")
        
    except Exception as e:
        cli_logger.error(f"Story structure parsing failed: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--database-path",
    type=click.Path(),
    help="Database file path (default: from config)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview removals without modifying the database"
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Show each entry being processed"
)
def filter_non_characters(database_path: Optional[str], dry_run: bool, verbose: bool) -> None:
    """Remove non-character entries (crews, locations, titles, etc.) from character table.

    Removes entries that have neither origin nor status, which indicates they
    are not actual characters. A built-in whitelist preserves known characters
    that happen to lack both fields.

    Use --dry-run to preview what would be removed.
    """
    from ..postprocessors.filter_non_characters import filter_non_characters as do_filter

    click.echo("🧹 Filtering non-character entries...")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made")

    if database_path is None:
        database_path = str(settings.database_path)

    if verbose:
        def progress_cb(current: int, total: int, cid: str) -> None:
            click.echo(f"  [{current}/{total}] checking {cid}")
    else:
        progress_cb = None

    try:
        result = do_filter(database_path, dry_run=dry_run, progress_callback=progress_cb)

        click.echo(f"\n📊 Results:")
        click.echo(f"  Entries checked: {result['total_checked']}")
        click.echo(f"  Removed: {result['removed']}")
        click.echo(f"    - No origin & no status: {result['removed'] - result['non_living_removed']}")
        click.echo(f"    - Non-living entities (Active/Destroyed): {result['non_living_removed']}")
        click.echo(f"  Kept (whitelisted): {result['kept_whitelist']}")

        if result['whitelisted_entries']:
            click.echo("\n✅ Whitelisted characters kept:")
            for cid, name in result['whitelisted_entries']:
                click.echo(f"    • {name} ({cid})")

        if result['removed_entries']:
            click.echo(f"\n{'🔍 Would remove:' if dry_run else '🗑️  Removed:'}")
            for cid, name in result['removed_entries']:
                click.echo(f"    • {name} ({cid})")

        if dry_run:
            click.echo(f"\n💡 Run without --dry-run to apply changes")
        else:
            click.echo(f"\n✅ Non-character filtering complete!")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--database-path",
    type=click.Path(),
    help="Database file path (default: from config)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Classify origins but do not write results to the database"
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    help="Show the region assigned to each character"
)
def sync_origin_region(database_path: Optional[str], dry_run: bool, verbose: bool) -> None:
    """Populate the origin_region column based on each character's origin field.

    Classifies characters into broad geographic regions:
    East Blue, North Blue, West Blue, South Blue, Grand Line, New World,
    Sky Island, Red Line, Calm Belt, Underwater.

    The origin_region column is created automatically if it does not exist.
    Use --dry-run to preview classifications without writing to the database.
    """
    from ..postprocessors.sync_origin_region import sync_origin_region as do_sync

    click.echo("🗺️  Syncing character origin regions...\n")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made\n")

    if database_path is None:
        database_path = str(settings.database_path)

    if verbose:
        def progress_callback(current: int, total: int, char_id: str, stats: dict) -> None:
            region = stats.get("region") or "unclassified"
            click.echo(f"  [{current:,}/{total:,}] {char_id}: {region}")
    else:
        def progress_callback(current: int, total: int, char_id: str, stats: dict) -> None:
            if current % 100 == 0 or current == total:
                click.echo(f"  Progress: {current:,}/{total:,} characters...")

    try:
        result = do_sync(database_path, dry_run=dry_run, progress_callback=progress_callback)

        click.echo("\n📊 Results:")
        click.echo(f"  Total characters:   {result['total']:,}")
        click.echo(f"  Classified:         {result['classified']:,}")
        click.echo(f"  Unknown region:     {result['unknown_region']:,}  (has origin text, but region not recognised)")
        click.echo(f"  No origin info:     {result['no_origin']:,}  (origin field is empty)")

        if result["by_region"]:
            click.echo("\n  By region:")
            for region, count in sorted(result["by_region"].items(), key=lambda x: -x[1]):
                click.echo(f"    {region}: {count:,}")

        if dry_run:
            click.echo("\n💡 Run without --dry-run to apply changes")
        else:
            click.echo("\n✅ Origin region sync complete!")

    except Exception as e:
        click.echo(f"❌ Error: {str(e)}")
        sys.exit(1)


@main.command()
@click.option(
    "--bucket",
    default=None,
    help="Supabase Storage bucket name (default: SUPABASE_THUMBNAIL_BUCKET)"
)
@click.option(
    "--width",
    type=int,
    default=150,
    help="Thumbnail width in pixels (default: 150)"
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Only process the first N characters (useful for smoke tests)"
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Resolve images but do not download or upload"
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-upload even when the object already exists"
)
def upload_thumbnails(
    bucket: Optional[str],
    width: int,
    limit: Optional[int],
    dry_run: bool,
    force: bool,
) -> None:
    """Upload character thumbnails from the wiki to Supabase Storage."""
    # Script lives in scripts/ — import by path so we don't need it on PYTHONPATH.
    import importlib.util
    from pathlib import Path as _Path

    script_path = _Path(__file__).resolve().parents[3] / "scripts" / "upload_character_thumbnails.py"
    spec = importlib.util.spec_from_file_location("_upload_thumbnails", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Register in sys.modules before exec so dataclasses / typing can resolve
    # cls.__module__ back to the module object.
    sys.modules["_upload_thumbnails"] = module
    spec.loader.exec_module(module)

    resolved_bucket = bucket or settings.supabase_thumbnail_bucket
    try:
        module.run(
            db_path=settings.database_path,
            bucket=resolved_bucket,
            thumb_width=width,
            limit=limit,
            dry_run=dry_run,
            force=force,
        )
    except Exception as e:
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Parse but do not write to the database"
)
def parse_devil_fruits(dry_run: bool) -> None:
    """Parse devil fruit data from character details into a structured table."""
    from ..postprocessors.parse_devil_fruits import parse_devil_fruits as do_parse

    click.echo("🍎 Parsing devil fruit data...\n")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made\n")

    db_path = str(settings.database_path)

    try:
        result = do_parse(db_path, dry_run=dry_run)

        click.echo(f"\n📊 Results:")
        click.echo(f"  Devil fruit entries: {result['entry_count']:,}")
        click.echo(f"  Characters with DF:  {result['character_count']:,}")
        click.echo(f"  Dual-identity fruits: {result['dual_identity']:,}")
        click.echo(f"  Multi-fruit chars:    {result['multi_fruit']:,}")

        if dry_run:
            click.echo("\n💡 Run without --dry-run to apply changes")
        else:
            click.echo("\n✅ Devil fruit data saved!")

    except Exception as e:
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Fetch bios but do not update the database"
)
def sync_character_bios(dry_run: bool) -> None:
    """Fetch short character bios from wiki intro text."""
    from ..postprocessors.sync_character_bios import sync_character_bios as do_sync

    click.echo("📖 Syncing character bios from wiki...\n")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made\n")

    db_path = str(settings.database_path)

    try:
        result = do_sync(db_path, dry_run=dry_run)

        click.echo(f"\n📊 Results:")
        click.echo(f"  Total characters: {result['total']:,}")
        click.echo(f"  Bios updated:     {result['updated']:,}")
        click.echo(f"  Skipped (no bio): {result['skipped']:,}")
        if dry_run:
            click.echo("\n💡 Run without --dry-run to apply changes")
        else:
            click.echo("\n✅ Character bios synced!")

    except Exception as e:
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Fetch categories but do not update the database"
)
def sync_haki(dry_run: bool) -> None:
    """Sync haki abilities from wiki categories into boolean columns."""
    from ..postprocessors.sync_haki import sync_haki as do_sync

    click.echo("💪 Syncing haki data from wiki categories...\n")

    if dry_run:
        click.echo("🔍 DRY RUN MODE - No changes will be made\n")

    db_path = str(settings.database_path)

    try:
        result = do_sync(db_path, dry_run=dry_run)

        click.echo(f"\n📊 Results:")
        click.echo(f"  Total characters:     {result['total']:,}")
        click.echo(f"  Any haki:             {result['any_haki']:,}")
        click.echo(f"  Observation (Kenbun): {result['haki_observation']:,}")
        click.echo(f"  Armament (Buso):      {result['haki_armament']:,}")
        click.echo(f"  Conqueror (Hao):      {result['haki_conqueror']:,}")

        if dry_run:
            click.echo("\n💡 Run without --dry-run to apply changes")
        else:
            click.echo("\n✅ Haki data synced!")

    except Exception as e:
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Command-line interface for One Piece of Data pipeline."""

import sys
from pathlib import Path
from typing import Optional

import click
from loguru import logger

from .config import settings
from .utils import setup_logging, get_logger
from .scrapers.chapter import ChapterScraper
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
    "--output",
    type=click.Path(),
    help="Output file path (default: from config)"
)
def scrape_chapters(start_chapter: int, end_chapter: Optional[int], output: Optional[str]) -> None:
    """Scrape chapter data from One Piece Fandom Wiki."""
    try:
        cli_logger.info("Starting chapter scraping...")
        
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
    "--start-volume",
    default=1,
    type=int,
    help="Starting volume number to scrape"
)
@click.option(
    "--end-volume",
    default=None,
    type=int,
    help="Ending volume number to scrape (default: from config)"
)
@click.option(
    "--output",
    type=click.Path(),
    help="Output file path (default: from config)"
)
def scrape_volumes(start_volume: int, end_volume: Optional[int], output: Optional[str]) -> None:
    """Scrape volume data from One Piece Fandom Wiki."""
    click.echo("🚧 Volume scraping not yet implemented in v2.0")
    click.echo("📝 This feature will be available in a future update")


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
def scrape_characters(input_file: Optional[str], output: Optional[str]) -> None:
    """Scrape character details from One Piece Fandom Wiki."""
    click.echo("🚧 Character scraping not yet implemented in v2.0")
    click.echo("📝 This feature will be available in a future update")


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
def parse(
    database_path: Optional[str],
    chapters_file: Optional[str],
    volumes_file: Optional[str],
    characters_file: Optional[str]
) -> None:
    """Parse scraped data and load into database."""
    click.echo("🚧 Data parsing not yet implemented in v2.0")
    click.echo("📝 This feature will be available in a future update")


@main.command()
@click.option(
    "--format",
    "export_format",
    default="csv",
    type=click.Choice(["csv", "json", "parquet"], case_sensitive=False),
    help="Export format"
)
@click.option(
    "--output-dir",
    type=click.Path(),
    help="Output directory for exported files"
)
@click.option(
    "--tables",
    multiple=True,
    type=click.Choice(["chapters", "volumes", "characters", "coc"], case_sensitive=False),
    help="Tables to export (can specify multiple)"
)
def export(export_format: str, output_dir: Optional[str], tables: tuple) -> None:
    """Export data from database to various formats."""
    click.echo("🚧 Data export not yet implemented in v2.0")
    click.echo("📝 This feature will be available in a future update")


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


if __name__ == "__main__":
    main()

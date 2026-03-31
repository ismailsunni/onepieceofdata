"""Scrapers package for One Piece of Data."""

from .chapter import ChapterScraper
from .volume import VolumeScraper
from .character import CharacterScraper
from .arc import ArcScraper
from .saga import SagaScraper
from .wiki_text_scraper import WikiTextScraper
from .wiki_text_uploader import WikiTextUploader

__all__ = [
    "ChapterScraper",
    "VolumeScraper",
    "CharacterScraper",
    "ArcScraper",
    "SagaScraper",
    "WikiTextScraper",
    "WikiTextUploader",
]

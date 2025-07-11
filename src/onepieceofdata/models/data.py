"""Data models for One Piece of Data pipeline."""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator


class ChapterModel(BaseModel):
    """Model for One Piece chapter data."""
    
    chapter_number: int = Field(..., description="Chapter number")
    title: str = Field(..., description="Chapter title")
    japanese_title: Optional[str] = Field(None, description="Japanese title")
    romanized_title: Optional[str] = Field(None, description="Romanized title")
    volume: Optional[int] = Field(None, description="Volume number")
    pages: Optional[int] = Field(None, description="Number of pages")
    release_date: Optional[str] = Field(None, description="Release date")
    characters: List[str] = Field(default_factory=list, description="Characters appearing in chapter")
    
    @validator("chapter_number")
    def validate_chapter_number(cls, v):
        """Validate chapter number is positive."""
        if v <= 0:
            raise ValueError("Chapter number must be positive")
        return v
    
    @validator("pages")
    def validate_pages(cls, v):
        """Validate pages is positive if provided."""
        if v is not None and v <= 0:
            raise ValueError("Pages must be positive")
        return v


class VolumeModel(BaseModel):
    """Model for One Piece volume data."""
    
    volume_number: int = Field(..., description="Volume number")
    title: str = Field(..., description="Volume title")
    japanese_title: Optional[str] = Field(None, description="Japanese title")
    release_date: Optional[str] = Field(None, description="Release date")
    chapters: List[int] = Field(default_factory=list, description="Chapters in volume")
    
    @validator("volume_number")
    def validate_volume_number(cls, v):
        """Validate volume number is positive."""
        if v <= 0:
            raise ValueError("Volume number must be positive")
        return v


class CharacterModel(BaseModel):
    """Model for One Piece character data."""
    
    name: str = Field(..., description="Character name")
    japanese_name: Optional[str] = Field(None, description="Japanese name")
    romanized_name: Optional[str] = Field(None, description="Romanized name")
    epithet: Optional[str] = Field(None, description="Character epithet")
    affiliation: Optional[str] = Field(None, description="Character affiliation")
    occupation: Optional[str] = Field(None, description="Character occupation")
    first_appearance: Optional[str] = Field(None, description="First appearance")
    
    @validator("name")
    def validate_name(cls, v):
        """Validate name is not empty."""
        if not v.strip():
            raise ValueError("Character name cannot be empty")
        return v.strip()


class CharacterOfChapterModel(BaseModel):
    """Model for character appearances in chapters."""
    
    chapter_number: int = Field(..., description="Chapter number")
    character_name: str = Field(..., description="Character name")
    
    @validator("chapter_number")
    def validate_chapter_number(cls, v):
        """Validate chapter number is positive."""
        if v <= 0:
            raise ValueError("Chapter number must be positive")
        return v
    
    @validator("character_name")
    def validate_character_name(cls, v):
        """Validate character name is not empty."""
        if not v.strip():
            raise ValueError("Character name cannot be empty")
        return v.strip()


class ScrapingResult(BaseModel):
    """Model for scraping operation results."""
    
    success: bool = Field(..., description="Whether scraping was successful")
    data: Optional[Dict[str, Any]] = Field(None, description="Scraped data")
    error: Optional[str] = Field(None, description="Error message if failed")
    timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp of operation")
    url: Optional[str] = Field(None, description="URL that was scraped")
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

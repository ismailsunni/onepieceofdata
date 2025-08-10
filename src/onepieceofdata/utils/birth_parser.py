"""Parser for birth date strings in One Piece character data."""

import re
from typing import Optional, Tuple
from datetime import datetime


class BirthDateParser:
    """Parser for birth date strings like 'March 9th', 'April 1st (April Fool's Day)', etc."""
    
    # Month name to number mapping
    MONTH_MAP = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    
    # Pattern to match birth date strings
    # Matches: "Month Day[st/nd/rd/th]" with optional additional text in parentheses
    BIRTH_PATTERN = re.compile(
        r'^([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)(?:\s*\([^)]*\))?$',
        re.IGNORECASE
    )
    
    @classmethod
    def parse_birth_string(cls, birth_str: str) -> Optional[Tuple[int, int]]:
        """Parse a birth string into month and day numbers.
        
        Args:
            birth_str: String like "March 9th" or "April 1st (April Fool's Day)"
            
        Returns:
            Tuple of (month, day) as integers, or None if parsing fails
        """
        if not birth_str or not isinstance(birth_str, str):
            return None
            
        birth_str = birth_str.strip()
        if not birth_str:
            return None
            
        match = cls.BIRTH_PATTERN.match(birth_str)
        if not match:
            print(f"Warning: Could not parse birth string: '{birth_str}'")
            return None
            
        month_name, day_str = match.groups()
        
        # Convert month name to number
        month_num = cls.MONTH_MAP.get(month_name.lower())
        if month_num is None:
            print(f"Warning: Unknown month name: '{month_name}' in '{birth_str}'")
            return None
            
        # Convert day string to number
        try:
            day_num = int(day_str)
        except ValueError:
            print(f"Warning: Could not parse day number: '{day_str}' in '{birth_str}'")
            return None
            
        # Validate day number for the month (basic validation)
        if not cls._is_valid_day(month_num, day_num):
            print(f"Warning: Invalid day {day_num} for month {month_num} in '{birth_str}'")
            return None
            
        return (month_num, day_num)
    
    @classmethod
    def parse_to_mm_dd(cls, birth_str: str) -> Optional[str]:
        """Parse birth string to MM-DD format.
        
        Args:
            birth_str: String like "March 9th"
            
        Returns:
            String in MM-DD format like "03-09", or None if parsing fails
        """
        parsed = cls.parse_birth_string(birth_str)
        if parsed is None:
            return None
            
        month, day = parsed
        return f"{month:02d}-{day:02d}"
    
    @classmethod
    def parse_to_date_with_year(cls, birth_str: str, year: int = 2000) -> Optional[str]:
        """Parse birth string to full date with given year.
        
        Args:
            birth_str: String like "March 9th"
            year: Year to use (default: 2000)
            
        Returns:
            String in YYYY-MM-DD format like "2000-03-09", or None if parsing fails
        """
        parsed = cls.parse_birth_string(birth_str)
        if parsed is None:
            return None
            
        month, day = parsed
        
        try:
            # Validate the date is valid for the given year
            datetime(year, month, day)
            return f"{year:04d}-{month:02d}-{day:02d}"
        except ValueError as e:
            print(f"Warning: Invalid date {year}-{month:02d}-{day:02d}: {e}")
            return None
    
    @classmethod
    def _is_valid_day(cls, month: int, day: int) -> bool:
        """Basic validation for day number in a month.
        
        Args:
            month: Month number (1-12)
            day: Day number
            
        Returns:
            True if the day is potentially valid for the month
        """
        if not (1 <= month <= 12):
            return False
            
        if not (1 <= day <= 31):
            return False
            
        # Days in each month (not accounting for leap years)
        days_in_month = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        
        return day <= days_in_month[month - 1]


def test_birth_parser():
    """Test function for the birth date parser."""
    test_cases = [
        "March 9th",
        "April 1st (April Fool's Day)",
        "July 3rd",
        "December 31st",
        "February 29th",  # Leap day
        "November 11th",
        "Invalid Date",
        "",
        None
    ]
    
    print("Testing BirthDateParser:")
    print("=" * 50)
    
    for test_case in test_cases:
        print(f"\nInput: '{test_case}'")
        
        # Test basic parsing
        parsed = BirthDateParser.parse_birth_string(test_case)
        print(f"  Parsed: {parsed}")
        
        # Test MM-DD format
        mm_dd = BirthDateParser.parse_to_mm_dd(test_case)
        print(f"  MM-DD: {mm_dd}")
        
        # Test full date format
        full_date = BirthDateParser.parse_to_date_with_year(test_case)
        print(f"  YYYY-MM-DD: {full_date}")


if __name__ == "__main__":
    test_birth_parser()

"""
Pure Python UDF Implementation

This module contains the same text parsing logic as the Rust version,
implemented in pure Python for performance comparison.
"""

import re
from collections import Counter
from typing import Dict
import math

"""
Pure Python UDF Implementation

This module contains the same text parsing logic as the Rust version,
implemented in pure Python for performance comparison.
"""

import re
from collections import Counter
from typing import Dict
import math

# Pre-compile regex patterns once at module import - equivalent to Rust static patterns
EMAIL_PATTERN = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
URL_PATTERN = re.compile(r'https?://[^\s]+')
PHONE_PATTERN = re.compile(r'\b\d{3}-\d{3}-\d{4}\b|\b\(\d{3}\)\s*\d{3}-\d{4}\b')
DATE_PATTERN = re.compile(r'\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{4}-\d{2}-\d{2}\b')
NUMBER_PATTERN = re.compile(r'\b\d+\.?\d*\b')
WORD_PATTERN = re.compile(r'\b\w+\b')

def parse_text_complexity_python(text: str) -> float:
    """
    Pure Python implementation of the text complexity parsing UDF.
    Now using pre-compiled regex patterns for fair comparison with Rust.
    """
    if not text:
        return 0.0
    
    # Use pre-compiled regex patterns
    email_count = len(EMAIL_PATTERN.findall(text))
    url_count = len(URL_PATTERN.findall(text))
    phone_count = len(PHONE_PATTERN.findall(text))
    date_count = len(DATE_PATTERN.findall(text))
    number_count = len(NUMBER_PATTERN.findall(text))
    words = WORD_PATTERN.findall(text)
    word_count = len(words)
    
    # Calculate character-based metrics
    char_count = len(text)
    uppercase_count = sum(1 for c in text if c.isupper())
    digit_count = sum(1 for c in text if c.isdigit())
    special_char_count = sum(1 for c in text if not c.isalnum() and not c.isspace())
    
    # Calculate word frequency entropy
    if word_count > 0:
        word_freq = Counter(word.lower() for word in words)
        entropy = 0.0
        for freq in word_freq.values():
            p = freq / word_count
            if p > 0:
                entropy -= p * math.log2(p)
    else:
        entropy = 0.0
    
    # Compute complexity score with weighted components
    pattern_score = email_count * 2.0 + url_count * 1.5 + phone_count * 1.8 + date_count * 1.2
    
    density_score = 0.0
    if char_count > 0:
        density_score = (uppercase_count + digit_count + special_char_count) / char_count * 10.0
    
    length_score = max(0.0, math.log(char_count / 100.0)) if char_count > 0 else 0.0
    diversity_score = entropy
    
    numeric_density = 0.0
    if word_count > 0:
        numeric_density = number_count / word_count * 5.0
    
    # Final complexity score
    complexity = pattern_score + density_score + length_score + diversity_score + numeric_density
    
    return complexity

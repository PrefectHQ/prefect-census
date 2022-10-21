
"""Utilities for common interactions with the Census API"""
from httpx import HTTPStatusError
from typing import Optional

def extract_user_message(error: HTTPStatusError) -> Optional[str]:
    """
    Extract user message from an error response from the Census API.
    
    Args:
        error: An HTTPStatusError raised by httpx
        
    Returns:
        status from Census API response or None if a status cannot
        be extraacted.
    """
    response_payload = error.response.json()
    status = response_payload.get("status", {})
    return status

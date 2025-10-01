import asyncio
import httpx
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from pydantic import ValidationError

logger = logging.getLogger(__name__)


class HNAPIError(Exception):
    """Custom exception for HN API operations"""
    pass


class HNAPIService:
    """Service for interacting with Hacker News API"""

    BASE_URL = "https://hacker-news.firebaseio.com/v0/" 

    def __init__(self, rate_limit_delay: float = 0.1):
          self.rate_limit_delay = rate_limit_delay
          self.session = httpx.AsyncClient(
              base_url=self.BASE_URL,
              timeout=10.0,
              limits=httpx.Limits(max_connections=10)  # Connection pooling
          )
 
    async def fetch_story(self, story_id: int) -> Dict[str, Any]:
        """
        Fetch story details from HN API

        Args:
            story_id: HN story ID

        Returns:
            Story data including kids, descendants, score, etc.
        """
        try:
            # Rate limiting - be respectful to HN API
            await asyncio.sleep(self.rate_limit_delay)

            # Reuse the session!
            response = await self.session.get(f"/item/{story_id}.json")

            if response.status_code == 404:
              raise HNAPIError(f"Story {story_id} not found or deleted")

            response.raise_for_status()
            data = response.json()
            return data if data else None
        except httpx.RequestError as e:
            raise HNAPIError(f"API request failed: {str(e)}")



    async def fetch_comment(self, comment_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch individual comment from HN API

        Args:
            comment_id: HN comment ID

        Returns:
            Comment data or None if deleted/not found
        """
        try:
            # Rate limiting - be respectful to HN API
            await asyncio.sleep(self.rate_limit_delay)

            response = await self.session.get(f"/item/{comment_id}.json")

            if response.status_code == 404:
              raise HNAPIError(f"Comment {comment_id} not found or deleted")

            response.raise_for_status()
            data = response.json()
            return data if data else None

        except httpx.RequestError as e:
            raise HNAPIError(f"API request failed: {str(e)}")

    async def fetch_comments_batch(self, comment_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Fetch multiple comments with validation and transformation

        Args:
            comment_ids: List of HN comment IDs
            story_db_id: Database ID of the parent story (for foreign key)

        Returns:
            List of database-ready comment dictionaries (excludes invalid/deleted comments)
        """
        valid_comments = []

        for i, comment_id in enumerate(comment_ids):

            try:
                # Fetch raw comment from HN API
                raw_valid_comment = await self.fetch_comment(comment_id)
                if not raw_valid_comment:
                    continue  # Skip deleted/missing comments
                
                # Skip comments that are marked as deleted
                if raw_valid_comment.get("deleted") is True:
                    continue
                
                valid_comments.append(raw_valid_comment)

                # Progressf logging every 10 comments
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(comment_ids)} comments")

            except ValidationError as e:
                logger.warning(f"Invalid comment {comment_id}: {str(e)}")
                continue
            except HNAPIError as e:
                logger.warning(f"Failed to fetch comment {comment_id}: {str(e)}")
                continue

        logger.info(f"Successfully processed {len(valid_comments)}/{len(comment_ids)} comments")
        return valid_comments
        
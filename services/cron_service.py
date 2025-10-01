import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from pydantic import ValidationError
from services.hn_api_service import HNAPIService, HNAPIError
from database.db_layer import HNDatabase, DatabaseError
from models.hn_models import DatabaseCommentData, DatabaseStoryData, HNCommentResponse, HNStoryResponse
from datetime import datetime

logger = logging.getLogger(__name__)


class CronServiceError(Exception):
    """Custom exception for cron service operations"""
    pass


class HNCronService:
    """Cron service for fetching and processing HN data"""

    def __init__(self):
        self.hn_api = HNAPIService()
        self.database = HNDatabase()

    async def process_hiring_thread(self, story_id: int) -> Dict[str, Any]:
        """
        Main workflow: Process a 'Who is hiring' thread

        BUSINESS LOGIC: Complete pipeline from HN story ID to stored job postings
        1. Fetch story metadata from HN API
        2. Validate and save story to database
        3. Fetch all child comments (job postings)
        4. Validate and save comments to database

        Args:
            story_id: HN story ID for the hiring thread

        Returns:
            Summary of processing results
        """
        logger.info(f"Starting processing of hiring thread {story_id}")
        try:
            # Fetch and validate story from HN API
            response = await self.hn_api.fetch_story(story_id)
            hn_story = HNStoryResponse(**response)  # Validates API response structure

            # Transform HN API format to database format
            db_story_data = DatabaseStoryData.from_hn_story(hn_story)

            logger.info(f'Database payload: {db_story_data.model_dump()}')

            # Save story to database (with duplicate prevention)
            saved_story = self.database.create_story(**db_story_data.model_dump())
            story_db_id = saved_story['id']  # Need this for comment foreign keys

            # logger.info(f"Fetching the id {hn_story.kids}")

            # Fetch and process all job posting comments
            # kids array contains HN comment IDs for job postings
            raw_comments = await self.hn_api.fetch_comments_batch(hn_story.kids)

            validated_comments = []
            for comment in raw_comments:
                hn_comment = HNCommentResponse(**comment)
                db_comment = DatabaseCommentData.from_hn_comment(hn_comment, story_db_id)

                validated_comments.append(db_comment.model_dump())


            
            saved_count = self.database.batch_create_comments(validated_comments)
            logger.info(f"Saved  comments {saved_count} to database")

            # STEP 6: Return processing summary for monitoring
            return {
                "story_id": story_id,
                "story_db_id": story_db_id,
                "story_saved": True,
                "comments_fetched": len(validated_comments),
                "comments_saved": saved_count,
                "errors": []
            }

        except HNAPIError as e:
            raise HNAPIError(f"API request failed: {str(e)}")
        except Exception as e:
            # send out an email or something
            raise HNAPIError(f"API request failed: {str(e)}")

    def _convert_hn_timestamp(self, hn_time: int) -> datetime:
        """Convert HN Unix timestamp to datetime"""
        return datetime.fromtimestamp(hn_time)

    async def close(self):
        """Clean up resources"""
        await self.hn_api.session.aclose()
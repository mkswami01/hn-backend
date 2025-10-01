import logging
import select
from supabase import create_client, Client
from typing import List, Dict, Any, Optional
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database operations"""
    pass


class HNDatabase:
    """Database layer for HN Newsletter backend"""

    def __init__(self):
        """Initialize Supabase connection"""
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_ANON_KEY")

        if not supabase_url or not supabase_key:
            raise DatabaseError("SUPABASE_URL and SUPABASE_ANON_KEY must be set in environment")

        try:
            self.client: Client = create_client(supabase_url, supabase_key)
        except Exception as e:
            raise DatabaseError(f"Failed to connect to Supabase: {str(e)}")

    # Story operations
    def create_story(self, hn_id: int, title: str = None, kids_count: int = 0, month: str = "month",
                    descendants_count: int = 0, score: int = 0, created_time: datetime = None) -> Dict[str, Any]:
        """Create a new story record"""
        try:
            story_data = {
                "hn_id": hn_id,
                "title": title,
                "kids_count": kids_count,
                "month":month,
                "descendants_count": descendants_count,
                "score": score,
                "created_time": created_time.isoformat() if created_time else None
            }

            logger.info(f'what are we storing {story_data}')


            response = self.client.table('stories').select('*').eq("hn_id", hn_id).execute()
            if response.data:
                logger.info(f"Story {hn_id} already exists, returning existing record")
                return response.data[0]
            response = self.client.table('stories').insert(story_data).execute()
            if response.data:
                return response.data[0]
            raise DatabaseError("Failed to create story")
        except Exception as e:
            raise DatabaseError(f"Error creating story: {str(e)}")

    def get_story_by_hn_id(self, hn_id: int) -> Optional[Dict[str, Any]]:
        """Get story by HN ID"""
        try:
            response = self.client.table('stories').select('*').eq('hn_id', hn_id).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            raise DatabaseError(f"Error fetching story: {str(e)}")

    def create_comment(self, hn_id: int, story_id: int, story_text: str,
                      structured_data: Dict = None, created_time: datetime = None) -> Dict[str, Any]:
        """Create a new comment record"""
        try:

            comment_data = {
                "hn_id":hn_id, 
                "story_id":story_id,
                "story_text":story_text,
                "created_time": created_time.isoformat() if created_time else None, 
                "structured_data":structured_data,
                "fetched_time": created_time.isoformat() if created_time else None, 
                "processed_status":"pending"
            }
            response = self.client.table('comments').upsert(
                comment_data,
                on_conflict='hn_id',           # Conflict column (your unique field)
                ignore_duplicates=True         # Skip existing records
            ).execute()

            if response.data:
                return response.data[0]
            raise DatabaseError("Failed to create comment")
        except Exception as e:
            raise DatabaseError(f"Error creating comment: {str(e)}")


    def get_completed_jobs(self,month) ->  List[Dict[str, Any]]:
        try :
            # Get the single story for this month
            story_response = self.client.table("stories").select("id").eq("month", month).execute()

            if not story_response.data or len(story_response.data) == 0:
                return []  # No story found for this month

            story_id = story_response.data[0]["id"]  # Get the first (and only) story's ID

            # Get comments for that story
            response = self.client.table("comments").select("*").eq("processed_status", "completed").eq("story_id", story_id).execute()

            return response.data if response.data else []
        except Exception as e:
            raise DatabaseError(f"Error fetching comments: {str(e)}")


        
    def get_comments_by_story_id(self, story_id: int) -> List[Dict[str, Any]]:
        """Get all comments for a story"""
        try:
            response = self.client.table("comments").select("*").eq("story_id", story_id).execute()
            return response.data if response.data else []
        except Exception as e:
            raise DatabaseError(f"Error fetching comments: {str(e)}")


    def get_comments_by_hn_id(self, hn_id: int) -> List[Dict[str, Any]]:
        """Get comment by an id"""
        try:
            response = self.client.table("comments").select("*").eq("hn_id", hn_id).execute()
            return response.data if response.data else []
        except Exception as e:
            raise DatabaseError(f"Error fetching comments: {str(e)}")

    def get_pending_comments(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get comments with pending processing status"""
        try:
            response = self.client.table("comments").select("*").eq("processed_status", "pending").limit(limit).execute()
            return response.data if response.data else []
        except Exception as e:
            raise DatabaseError(f"Error fetching comments: {str(e)}")

    def update_comment_status(self, comment_id: int, status: str, structured_data: Dict = None) -> bool:
        """Update comment processing status and structured data"""
        try:
            update_data = {"processed_status": status} 

            if structured_data is not None:
                update_data["structured_data"] = structured_data
                if structured_data.get("email") is not None:
                    update_data["email"] = structured_data["email"]
            else:
                # Explicitly clear when processing fails
                update_data["structured_data"] = None
                update_data["email"] = None

            response = self.client.table("comments").update(update_data).eq("hn_id", comment_id).execute()
            return len(response.data) > 0
        except Exception as e:
            raise DatabaseError(f"Error updating comment status: {str(e)}")

    def batch_create_comments(self, comments_data: List[Dict[str, Any]]) -> int:
        """Bulk insert comments for efficiency"""
        try:
            if not comments_data:
                logger.info("No comments to insert - returning 0")
                return 0

            response = self.client.table('comments').insert(comments_data).execute()

            inserted_count = len(response.data) if response.data else 0

            return inserted_count
        except Exception as e:
            logger.error(f"Batch insert failed with error: {str(e)}")
            raise DatabaseError(f"Error batch creating comments: {str(e)}")

from fastapi import APIRouter, HTTPException
from typing import List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel

from database.db_layer import HNDatabase
from utils.months import Month


router = APIRouter()

class StoryResponse(BaseModel):
    id: int
    hn_id: int
    title: str
    url: Optional[str]
    author: str
    score: int
    comments_count: int
    created_time: datetime
    summary: Optional[str]
    category: Optional[str]
    relevance_score: Optional[float]

    class Config:
        from_attributes = True

@router.post("/process-hiring-thread/{story_id}")
async def test_process_hiring_thread(story_id: int):
    """Test endpoint to run the complete cron workflow"""
    from services.cron_service import HNCronService

    cron_service = HNCronService()

    try:
        result = await cron_service.process_hiring_thread(story_id)
        await cron_service.close()  # Clean up resources
        return {
            "success": True,
            "message": f"Processed hiring thread {story_id}",
            "data": result
        }
    except Exception as e:
        await cron_service.close()
        return {
            "success": False,
            "error": str(e),
            "story_id": story_id
        }
    #     comment_count = await hn_service.fetch_thread_comments(hiring_thread_id, db)
    #     return {"message": f"Successfully fetched {1234} job postings from September hiring thread"}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Failed to fetch hiring thread: {str(e)}")


@router.get("/jobs")
async def get_jobs(month: Optional[str] = None):
    database = HNDatabase()

    try:
        # If no month provided, default to current month
        if not month:
            current_date = datetime.now()
            month = f"{current_date.month:02d}-{str(current_date.year)[2:]}"  # Format as "12-25"

        # Parse MM-YY format to YYYY-MM format for database query
        # Expected input: "11-25", "12-25", "01-26"
        if "-" in month:
            try:
                month_part, year_part = month.split("-")

                # Validate format: MM-YY (month must be 2 digits, year must be 2 digits)
                if len(month_part) != 2 or len(year_part) != 2:
                    return {
                        "success": False,
                        "error": f"Invalid month format: {month}. Expected format: MM-YY (e.g., 11-25, 12-25, 01-26)"
                    }

                # Validate month is 01-12
                month_num = int(month_part)
                if not (1 <= month_num <= 12):
                    return {
                        "success": False,
                        "error": f"Invalid month: {month_part}. Must be between 01 and 12."
                    }

                # Convert YY to YYYY (assume 20YY for years 00-99)
                full_year = f"20{year_part}"

                # Convert to database format: "2025-09"
                db_month_format = f"{full_year}-{month_part}"

            except ValueError:
                return {
                    "success": False,
                    "error": f"Invalid month format: {month}. Expected format: MM-YY (e.g., 11-25, 12-25, 01-26)"
                }
        else:
            return {
                "success": False,
                "error": f"Invalid month format: {month}. Expected format: MM-YY (e.g., 11-25, 12-25, 01-26)"
            }

        jobs = database.get_completed_jobs(db_month_format)

        return {
            "success": True,
            "data": jobs,
            "count": len(jobs),
            "month": month,  # Return the original MM-YY format
            "db_month": db_month_format  # Also show the converted format for clarity
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.get("/{story_id}", response_model=StoryResponse)
async def get_story(story_id: int):
    pass
    # story = db.query(Story).filter(Story.id == story_id).first()
    # if not story:
    #     raise HTTPException(status_code=404, detail="Story not found")
    # return story

@router.post("/fetch-comments/{story_id}")
async def fetch_thread_comments(story_id: int):
    pass
    # # from services.hackernews import HackerNewsService
    # # hn_service = HackerNewsService()
    # try:
    #     comment_count = await hn_service.fetch_thread_comments(story_id, db)
    #     return {"message": f"Fetched {comment_count} comments from thread {story_id}"}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=f"Failed to fetch comments: {str(e)}")

@router.get("/comments/{story_id}")
async def get_comments(story_id: int):
    pass
    # from models import Comment
    # comments = db.query(Comment).filter(Comment.story_hn_id == story_id).all()
    # return comments

@router.post("/process-comments")
async def process_pending_comments():
    """Dedicated endpoint for Claude processing of all pending comments"""
    from services.processing_service import ClaudeProcessingService

    processing_service = ClaudeProcessingService()

    try:
        result = processing_service.process_pending_comments()
        return {
            "success": True,
            "message": f"Processed all pending comments with Claude",
            "data": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/process-comment/{hn_id}")
async def process_single_comment(hn_id: int):
    """Process a specific comment by HN ID"""
    from services.processing_service import ClaudeProcessingService

    processing_service = ClaudeProcessingService()

    try:
        result = processing_service.process_single_comment(hn_id)

        return {
            "success": True,
            "message": f"Processed comment {hn_id} with Claude",
            "data": result
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "hn_id": hn_id
        }
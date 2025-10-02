import logging
import re
from typing import List, Dict, Any, Optional
from anthropic import Anthropic
from pydantic import BaseModel, ValidationError
import json
import os
from models.hn_models import OpenAIProcessData

from database.db_layer import HNDatabase, DatabaseError

logger = logging.getLogger(__name__)


class ProcessingServiceError(Exception):
    """Custom exception for processing service operations"""
    pass


class ClaudeProcessingService:
    """Service for processing job postings with OpenAI"""

    prompt = """You are an expert at extracting job posting information from Hacker News "Who is hiring" posts.

        EXTRACTION RULES:
        - company: Company or organization name (required)
        - description: Description of what they do (required)
        - positions: All job titles mentioned (required - at least one)
        - location: City, state, country, or "Remote" if mentioned
        - salary: Any compensation/salary information
        - stack: All technologies, programming languages, frameworks mentioned
        - email: Contact email (convert "john at company dot com" to "john@company.com") 
        - application_url: Any URLs for applying or company careers pages
        - remote_friendly: true if remote work is explicitly supported
        - employment_type: Full-time, Part-time, Contract, Internship if mentioned

        IMPORTANT:
        - If you cannot identify a company name AND at least one job position, the posting is not useful
        - Be thorough with technology stack extraction
        - Convert common email obfuscations: "at"→"@", "dot"→".", "[at]"→"@"
        - Look for both direct emails and application URLs

        Return your response as a valid JSON object with these fields.
        """

    def __init__(self):
        """Initialize OpenAI client and database connection"""
        self.database = HNDatabase()

        self.client = Anthropic(api_key=os.getenv("CLAUDE_API_KEY"))
    

    def process_pending_comments(self) -> Dict[str, Any]:
        """
        Main method: Process all pending comments with Claude

        Returns:
            Summary of processing results
        """
        logger.info("Starting processing of all pending comments")

        pending_comments = self.database.get_pending_comments()

        logger.info(f"Pending comments are {len(pending_comments)}")

        processed_count = 0;
        successful_count = 0;
        failed_count = 0;
        errors = []

        for comment in pending_comments:
            logger.info(f'Processing id is {comment} here')
            try :
                cleaned_comment = self._clean_html_text(comment["story_text"])
                structured_data = self._extract_job_data_with_claude(cleaned_comment)

                if structured_data is None:
                    value = self._update_comment_with_results(comment["hn_id"], None, "error")
                    failed_count += 1
                else:
                    value = self._update_comment_with_results(comment["hn_id"], structured_data, "completed")
                    if value:
                        successful_count += 1
                    else:
                        failed_count += 1
                logger.info(f"data extracted by llm {json.dumps(structured_data, indent=4, sort_keys=True)}") 
            except Exception as e:
                logger.info(f"Any exception {e}")
                errors.append(e)
        results = {
            "processed_count": processed_count,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "errors": errors
        }

        return results

    def process_single_comment(self, hn_id: int) -> Dict[str, Any]:
        logger.info(f"=== Starting single comment processing for HN ID: {hn_id} ===")

        try:
            # Step 1: Get comment from database
            logger.info(f"Step 1: Fetching comment with HN ID {hn_id}")
            comments = self.database.get_comments_by_hn_id(hn_id)
            logger.info(f"Retrieved comments: {len(comments)} found")

            if not comments:
                raise ValueError(f"No comment found with HN ID {hn_id}")

            comment = comments[0]  # Get the first (should be only) comment
            logger.info(f"Processing comment: {comment}")

            # Step 2: Clean HTML text
            logger.info(f"Step 2: Cleaning HTML text")
            raw_text = comment["story_text"]
            logger.info(f"Raw text (first 200 chars): {raw_text[:200]}...")
            cleaned_comment = self._clean_html_text(raw_text)
            logger.info(f"Cleaned text (first 200 chars): {cleaned_comment[:200]}...")

            # Step 3: Extract data with Claude
            logger.info(f"Step 3: Processing with Claude")
            structured_data = self._extract_job_data_with_claude(cleaned_comment)
            logger.info(f"Claude extraction result: {structured_data}")

            # Step 4: Update database
            logger.info(f"Step 4: Updating database")
            update_success = self._update_comment_with_results(comment["hn_id"], structured_data)
            logger.info(f"Database update success: {update_success}")

            # Return result
            result = {
                "success": True,
                "hn_id": hn_id,
                "extracted_data": structured_data,
                "database_updated": update_success
            }
            logger.info(f"=== Completed processing for HN ID: {hn_id} ===")
            return result

        except Exception as e:
            logger.error(f"ERROR in single comment processing for HN ID {hn_id}: {e}")
            return {
                "success": False,
                "hn_id": hn_id,
                "error": str(e)
            }

    def _clean_html_text(self, raw_text: str) -> str:
      """
      Minimal cleaning - let Claude handle the HTML parsing
      """
      try:
          # Just handle the most basic HTML entities that might break parsing
          cleaned = raw_text.replace('&#x2F;', '/').replace('&amp;', '&')
          return cleaned

      except Exception as e:
          logger.error(f"Basic cleaning failed: {e}")
          return raw_text

    # def _clean_html_text(self, raw_text: str) -> str:
    #     """
    #     Clean HTML artifacts from comment text using BeautifulSoup

    #     Args:
    #         raw_text: Raw comment text with HTML entities

    #     Returns:
    #         Cleaned text ready for Claude processing with preserved URLs
    #     """
    #     try:
    #         logger.info(f"Raw HTML input: {raw_text}...")  # Log first 200 chars

    #         # Parse HTML with BeautifulSoup
    #         soup = BeautifulSoup(raw_text, 'html.parser')

    #         # Extract all URLs from links
    #         urls = []
    #         for link in soup.find_all('a', href=True):
    #             href = link.get('href')
    #             if href:
    #                 # Clean up HTML entities in URLs
    #                 clean_url = href.replace('&#x2F;', '/').replace('&amp;', '&')
    #                 urls.append(clean_url)

    #         # Get clean text content (removes all HTML tags)
    #         text = soup.get_text(separator=' ', strip=True)

    #         # Replace HTML entities in text
    #         text = text.replace('&#x2F;', '/').replace('&amp;', '&')

    #         # Add URLs back to the text for Claude to process
    #         for url in urls:
    #             if url not in text:  # Avoid duplicates
    #                 text += f" {url}"

    #         # Clean up multiple spaces
    #         cleaned_text = re.sub(r'\s+', ' ', text).strip()

    #         logger.info(f"Cleaned text: {cleaned_text}")  # Log first 200 chars
    #         logger.info(f"Extracted {len(urls)} URLs")

    #         return cleaned_text

    #     except Exception as e:
    #         logger.error(f"HTML cleaning failed: {e}")
    #         # Fallback to original text if cleaning fails
    #         return raw_text

    def _extract_job_data_with_claude(self, cleaned_text: str) -> Dict[str, Any]:
        try:
            # Claude API call
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",  # Most cost-effective
                max_tokens=1000,
                messages=[{
                    "role": "user",
                    "content": f"{self.prompt}\n\nJob posting text:\n{cleaned_text}"
                }]
            )

            # Extract JSON from Claude's response
            content = response.content[0].text
            raw_data = json.loads(content)

            # Validate with your Pydantic model
            validated_data = OpenAIProcessData(**raw_data)
            return validated_data.model_dump()

        except Exception as e:
            logger.error(f"Claude processing failed: {e}")
            return None

    
    def _update_comment_with_results(self, comment_id: int, structured_data: Dict[str, Any],
                                   status: str = 'completed') -> bool:
        """Update comment with processed results"""
        try:
            return self.database.update_comment_status(
                comment_id=comment_id,
                status=status,
                structured_data=structured_data
            )
        except (DatabaseError, Exception) as e:
            logger.error(f"Failed to update comment {comment_id}: {str(e)}")
            try:
                self.database.update_comment_status(
                    comment_id=comment_id,
                    status="error",
                    structured_data=None
                )
            except Exception:
                logger.error(f"Failed to mark comment {comment_id} as error")
            return False
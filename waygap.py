#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Wayback Gap Detector
Compares a local URL list against the Wayback Machine CDX API
to identify URLs that have not been archived.
"""

import argparse
import html
import json
import logging
import os
import sys
from pathlib import Path
from typing import Set, List, Optional, Generator, Iterable, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Logger setup
logger = logging.getLogger(__name__)


# ========================================
# Default settings
# ========================================
IGNORE_PROTOCOL = True
SORT_QUERY_PARAMS = False
TARGET_URL_FILE = "urls.txt"
OUTPUT_FILE = "not_archived.txt"
CACHE_FILE = "archived_cdx.jsonl"
ARCHIVED_FILE = "archived.txt"

USER_AGENT = "Wayback-Gap-Detector/1.0 (https://github.com/c3974/wayback-gap-detector)"

CDX_LIMIT = 25000

# ========================================
# Custom Exceptions
# ========================================
class WaybackGapDetectorError(Exception):
    """Base exception for Wayback Gap Detector"""
    pass


class CDXAPIError(WaybackGapDetectorError):
    """CDX API related errors (network, response parsing, etc.)"""
    pass


class InputFileError(WaybackGapDetectorError):
    """Input file related errors (not found, encoding issues, etc.)"""
    pass


# ========================================
# URL normalization
# ========================================
def normalize_url(url: str, ignore_protocol: bool = IGNORE_PROTOCOL,
                  sort_query: bool = SORT_QUERY_PARAMS) -> str:
    """
    Normalize a URL into a canonical form suitable for comparison.
    
    Args:
        url: The URL to normalize.
        ignore_protocol: If True, treat http and https as equivalent.
        sort_query: If True, sort query parameters alphabetically.
    
    Returns:
        The normalized URL string.
    """
    # 1. Strip leading/trailing whitespace
    url = url.strip()
    
    # 2. Decode HTML entities
    url = html.unescape(url)
    
    # 3. Prepend http:// if no scheme is present
    if '://' not in url:
        url = 'http://' + url
    
    # 4. Parse the URL
    parsed = urlparse(url)
    
    # 5. Normalize the scheme
    original_scheme = parsed.scheme.lower()
    scheme = original_scheme
    if ignore_protocol and scheme in ('http', 'https'):
        scheme = 'http'
    
    # 6. Normalize the hostname to lowercase
    netloc = parsed.hostname or ''
    netloc = netloc.lower()
    
    # 7. Handle port numbers
    port = parsed.port
    if port:
        # Remove default ports (checked against both original and normalized scheme)
        is_default_port = (
            (original_scheme == 'http' and port == 80) or
            (original_scheme == 'https' and port == 443) or
            (scheme == 'http' and port == 80) or
            (scheme == 'https' and port == 443)
        )
        if not is_default_port:
            netloc = f"{netloc}:{port}"
    
    # 8. Normalize the path
    path = parsed.path
    # Remove trailing slash, except for the root path "/"
    if path and path != '/' and path.endswith('/'):
        path = path.rstrip('/')
    
    # 9. Handle query parameters
    query = parsed.query
    if query:
        if sort_query:
            # Sort query parameters alphabetically
            params = parse_qs(query, keep_blank_values=True)
            sorted_params = sorted(params.items())
            query = urlencode(sorted_params, doseq=True)
    else:
        query = ''
    
    # 10. Always strip the fragment
    fragment = ''
    
    # 11. Reconstruct the normalized URL
    normalized = urlunparse((scheme, netloc, path, '', query, fragment))
    
    return normalized


# ========================================
# CDX API
# ========================================
def fetch_cdx_data(target_url: str, cache_file: str,
                   initial_resume_key: Optional[str] = None,
                   limit: int = CDX_LIMIT) -> Generator[List, None, None]:
    """
    Fetch CDX API data or load it from the local cache.
    Yields CDX records one by one.
    Uses resumeKey pagination for large datasets.
    
    Args:
        target_url: Wildcard URL to search in the CDX API.
        cache_file: Path to the local cache file.
        initial_resume_key: resumeKey to resume from (None to start from the beginning).
        limit: Maximum number of records to fetch per request (default: 25000).
    
    Yields:
        CDX record (List)
    """
    
    cache_valid = False
    # Skip cache if initial_resume_key is specified
    if initial_resume_key is None and os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
        logger.info(f"Loading from cache: {cache_file}")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if first_line:
                    json.loads(first_line)  # Check that the first line is valid JSON
                    cache_valid = True
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"Cache read error: {e}")
            logger.info("Falling back to CDX API...")
            cache_valid = False

    if cache_valid:
        def _read_cache():
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    stripped = line.strip()
                    if stripped:
                        yield json.loads(stripped)
            logger.info("Finished yielding all records from JSONL cache")
        
        # Return the generator
        return _read_cache()

    # API Fetch Mode - streaming fetch (lazy evaluation, memory-efficient)
    def _fetch_from_api():
        """Stream data from the CDX API (Generator)."""
        logger.info(f"Fetching data from CDX API: {target_url}")
        if initial_resume_key:
            logger.info(f"Resuming from resumeKey: {initial_resume_key}")
        
        api_url = "https://web.archive.org/cdx/search/cdx"
        params = {
            'url': target_url,
            'output': 'json',
            'filter': 'statuscode:200',
            'collapse': 'urlkey',
            'fl': 'original,timestamp',
            'showResumeKey': 'true',
            'limit': limit
        }
        headers = {'User-Agent': USER_AGENT}
    
        # Configure session and retry policy
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Create parent directories for the cache file if they don't exist
        Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
        
        cache_f = None
        try:
            # Append if resuming, otherwise create/overwrite
            mode = 'a' if initial_resume_key else 'w'
            cache_f = open(cache_file, mode, encoding='utf-8')
            resume_key = initial_resume_key
            seen_resume_keys = set()      # Keys used in requests (cycle detection)
            seen_response_keys = set()    # Keys seen in responses (duplicate prevention)
            total_yielded = 0

            while True:
                if resume_key:
                    # Detect resumeKey cycles
                    if resume_key in seen_resume_keys:
                        logger.warning(f"resumeKey cycle detected (request side): {resume_key}")
                        logger.info("Stopping pagination to prevent infinite loop")
                        break
                    seen_resume_keys.add(resume_key)
                    params['resumeKey'] = resume_key

                # Retry logic for 200 OK but non-JSON or empty responses
                data = None
                last_error = None
                max_retries = 3

                for attempt in range(max_retries):
                    try:
                        response = session.get(api_url, params=params, headers=headers, timeout=300)
                        response.raise_for_status()

                        # Also verify Content-Type and body even on 200 OK
                        if response.status_code == 200:
                            content_type = response.headers.get('Content-Type', '')
                            content_length = len(response.content)
                            response_text = response.text.strip()
                            
                            # Detect empty or non-JSON responses
                            if content_length == 0 or not response_text or 'application/json' not in content_type:
                                logger.warning(
                                    f"Non-JSON or empty response detected (attempt {attempt + 1}/{max_retries}): "
                                    f"status={response.status_code}, "
                                    f"Content-Type={content_type}, "
                                    f"content-length={content_length}"
                                )
                                
                                # Only log the response body in verbose (-v) mode
                                if logger.level == logging.DEBUG:
                                    preview = response.text[:4096] if response.text else "(empty)"
                                    logger.debug(f"Response preview: {preview!r}")
                                
                                # Retry if not at the last attempt
                                if attempt < max_retries - 1:
                                    wait_time = 2 ** attempt
                                    logger.info(f"Waiting {wait_time}s before retry...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    raise CDXAPIError("Non-JSON or empty response (max retries reached)")

                        data = response.json()
                        break
                        
                    except requests.exceptions.RequestException as e:
                        last_error = e
                        logger.warning(f"Request error (attempt {attempt + 1}/{max_retries}): {e}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue

                    except json.JSONDecodeError as e:
                        last_error = e
                        logger.warning(
                            f"JSON parse error (attempt {attempt + 1}/{max_retries}): {e}"
                        )
                        logger.warning(
                            f"HTTP status={response.status_code if 'response' in locals() else 'N/A'}, "
                            f"Content-Type={response.headers.get('Content-Type', 'N/A') if 'response' in locals() else 'N/A'}, "
                            f"content-length={len(response.content) if 'response' in locals() else 0}"
                        )
                        
                        # Only log the response body in verbose (-v) mode
                        if logger.level == logging.DEBUG and 'response' in locals():
                            preview = response.text[:4096] if response.text else "(no content)"
                            logger.debug(f"Response preview: {preview!r}")
                        
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logger.info(f"Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue

                if data is None:
                    if cache_f:
                        cache_f.close()
                    
                    failed_key = resume_key if resume_key else "(initial)"
                    logger.error(f"Max retries reached. Failed at resumeKey={failed_key}")
                    logger.error(f"You can resume with the following command:")
                    logger.error(f"  python waygap.py \"{target_url}\" --resume-key {failed_key}")
                    
                    raise CDXAPIError(f"CDX API fetch error (max retries reached): {last_error}") from last_error
            
                if not isinstance(data, list):
                    if cache_f:
                        cache_f.close()
                    raise CDXAPIError(
                        f"CDX API returned an unexpected type: {type(data).__name__}. "
                        f"Expected a list."
                    )

                # Remove all empty list rows (not just from the tail)
                data = [row for row in data if row != []]

                # Detect resumeKey robustly
                new_resume_key = None
                if data and isinstance(data[-1], list) and len(data[-1]) == 1:
                    # A single-element array is a resumeKey candidate
                    candidate = data[-1][0]
                    if isinstance(candidate, str):
                        new_resume_key = candidate
                        data = data[:-1]

                # Stop if a resumeKey seen in a previous response is returned again
                # (indicates we are receiving the same page/response again)
                if new_resume_key and new_resume_key in seen_response_keys:
                    logger.warning(f"Previously seen resumeKey returned again: {new_resume_key} -> stopping")
                    break
                
                # Record the new resumeKey from this response
                if new_resume_key:
                    seen_response_keys.add(new_resume_key)

                # Strip the header row from each chunk
                if data and isinstance(data[0], list) and data[0]:
                    first_elem = data[0][0] if len(data[0]) > 0 else None
                    if first_elem in ('urlkey', 'original'):
                        data = data[1:]

                # Write valid rows to cache and yield them immediately (streaming)
                valid_count = 0
                for row in data:
                    if isinstance(row, list) and len(row) >= 2:  # Only yield valid data rows
                        cache_f.write(json.dumps(row, ensure_ascii=False) + '\n')
                        yield row  # Yield immediately without buffering in memory
                        total_yielded += 1
                        valid_count += 1

                # Prevent infinite loops: stop if no valid rows were returned
                if new_resume_key and valid_count == 0:
                    logger.warning("No valid data rows in this chunk")
                    logger.info("Stopping pagination to prevent infinite loop")
                    break

                if new_resume_key:
                    resume_key = new_resume_key
                    logger.debug(f"Next chunk: resumeKey={resume_key}")
                else:
                    logger.info(f"All chunks fetched successfully ({total_yielded} records total)")
                    break
        finally:
            if cache_f:
                cache_f.close()

    return _fetch_from_api()


def extract_archived_urls(cdx_data: Iterable[List], ignore_protocol: bool,
                          sort_query: bool) -> Set[str]:
    """
    Extract a set of normalized URLs from CDX data.
    
    Args:
        cdx_data: Iterable of CDX API records.
        ignore_protocol: If True, treat http and https as equivalent.
        sort_query: If True, sort query parameters alphabetically.
    
    Returns:
        A set of normalized URL strings.
    """
    # Return early if cdx_data is not iterable
    try:
        iter(cdx_data)
    except TypeError:
        logger.warning(f"cdx_data is not iterable: {type(cdx_data).__name__}")
        return set()
    
    archived_urls = set()
    original_idx = None
    is_first_row = True
    
    for row in cdx_data:
        # Skip non-list rows (treat as malformed and ignore)
        if not isinstance(row, list):
            logger.debug(f"Skipping non-list row: {type(row).__name__}")
            continue
        
        # Skip empty rows
        if not row:
            continue
        
        # Detect and process the header row
        if is_first_row or (row and row[0] in ('original', 'urlkey')):
            is_first_row = False
            
            # Determine the column index of 'original' from the header
            try:
                original_idx = row.index('original')
                logger.debug(f"'original' column index: {original_idx}")
            except ValueError:
                # When fl=original,timestamp is used, index 0 is the default
                logger.warning("'original' column not found; falling back to index 0")
                original_idx = 0
            continue  # Skip the header row itself
        
        # Process a data row
        if original_idx is not None and len(row) > original_idx:
            original_url = row[original_idx]
            
            # Ensure the URL value is a string
            if isinstance(original_url, str):
                normalized = normalize_url(original_url, ignore_protocol, sort_query)
                archived_urls.add(normalized)
    
    return archived_urls


# ========================================
# Main logic
# ========================================
def detect_not_archived(input_file: str, archived_urls: Set[str],
                        ignore_protocol: bool, sort_query: bool,
                        collect_archived: bool = False) -> Tuple[List[str], Optional[List[str]], int]:
    """
    Detect URLs in the input file that have not been archived.
    
    Args:
        input_file: Path to the input URL file.
        archived_urls: Set of already-archived URLs (normalized).
        ignore_protocol: If True, treat http and https as equivalent.
        sort_query: If True, sort query parameters alphabetically.
        collect_archived: If True, also collect the list of archived URLs (default: False).
    
    Returns:
        tuple: (list of unarchived URLs, list of archived URLs or None, total input URL count)
    """
    if not os.path.exists(input_file):
        raise InputFileError(f"Input file not found: {input_file}")
    
    not_archived = []
    archived = [] if collect_archived else None
    total_count = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            original_url = line.strip()

            if not original_url:
                continue

            total_count += 1
            
            normalized = normalize_url(original_url, ignore_protocol, sort_query)
            
            if normalized not in archived_urls:
                not_archived.append(original_url)
            elif collect_archived:
                archived.append(original_url)
    
    return not_archived, archived, total_count


def main() -> int:
    """Entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description='Wayback Gap Detector - Find URLs that have not been archived',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
examples:
  python waygap.py "https://example.com/blog/*"
  python waygap.py "https://example.com/blog/*" --sort-query --output result.txt
        '''
    )
    
    parser.add_argument(
        'target_url',
        help='Wildcard URL to search in the CDX API (e.g. https://example.com/path/*)'
    )
    
    parser.add_argument(
        '--ignore-protocol',
        dest='ignore_protocol',
        action='store_true',
        default=IGNORE_PROTOCOL,
        help='treat http and https as equivalent (default: enabled)'
    )
    parser.add_argument(
        '--no-ignore-protocol',
        dest='ignore_protocol',
        action='store_false',
        help='distinguish between http and https'
    )
    
    parser.add_argument(
        '--sort-query',
        dest='sort_query',
        action='store_true',
        default=SORT_QUERY_PARAMS,
        help='sort query parameters alphabetically (default: disabled)'
    )
    parser.add_argument(
        '--no-sort-query',
        dest='sort_query',
        action='store_false',
        help='preserve the original query parameter order'
    )
    
    parser.add_argument(
        '--input',
        dest='input_file',
        default=TARGET_URL_FILE,
        help=f'path to the input URL file (default: {TARGET_URL_FILE})'
    )
    
    parser.add_argument(
        '--output',
        dest='output_file',
        default=OUTPUT_FILE,
        help=f'path to the output file (default: {OUTPUT_FILE})'
    )
    
    parser.add_argument(
        '--cache',
        dest='cache_file',
        default=CACHE_FILE,
        help=f'path to the CDX cache file (default: {CACHE_FILE})'
    )

    parser.add_argument(
        '--output-archived',
        dest='output_archived',
        nargs='?',
        const=ARCHIVED_FILE,
        default=None,
        help=f'path to write archived URLs to (default: {ARCHIVED_FILE})'
    )

    parser.add_argument(
        '--resume-key',
        dest='resume_key',
        default=None,
        help='resumeKey to resume an interrupted fetch (use the key logged at failure time)'
    )

    parser.add_argument(
        '--limit',
        dest='limit',
        type=int,
        default=CDX_LIMIT,
        help=f'maximum number of records per API request (default: {CDX_LIMIT})'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='enable verbose (DEBUG-level) logging'
    )
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )
    
    try:
        cdx_data = fetch_cdx_data(
            args.target_url, 
            args.cache_file, 
            initial_resume_key=args.resume_key,
            limit=args.limit
        )

        logger.info("CDX data extraction complete")
        
        archived_urls = extract_archived_urls(
            cdx_data,
            args.ignore_protocol,
            args.sort_query
        )
        
        logger.info(f"Archived URLs found in CDX: {len(archived_urls)}")
        
        not_archived, archived, total_count = detect_not_archived(
            args.input_file,
            archived_urls,
            args.ignore_protocol,
            args.sort_query,
            collect_archived=bool(args.output_archived)
        )
        
        # Create output directory if it doesn't exist
        Path(args.output_file).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_file, 'w', encoding='utf-8') as f:
            for url in not_archived:
                f.write(url + '\n')
        
        logger.info(f"Total input URLs: {total_count}")
        logger.info(f"Unarchived URLs: {len(not_archived)}")
        logger.info(f"Results written to {args.output_file}")

        if args.output_archived and archived is not None:
            Path(args.output_archived).parent.mkdir(parents=True, exist_ok=True)
            with open(args.output_archived, 'w', encoding='utf-8') as f:
                for url in archived:
                    f.write(url + '\n')
            logger.info(f"Archived URLs written to {args.output_archived}: {len(archived)} entries")
        
        return 0
    
    except CDXAPIError as e:
        logger.error(f"{e}")
        return 1
    except InputFileError as e:
        logger.error(f"{e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

"""BLS time-series ingestion logic.

All configuration (URLs, headers, paths, rate-limit settings) is owned by the
calling notebook and passed in as arguments — this module holds no constants
of its own.

Layout within this file:
    create_retry_session()   Infrastructure: builds a retrying requests.Session.
    get_directory_items()    Business logic: scrape one directory page.
    get_all_survey_codes()   Business logic: top-level survey code discovery.
    sync_file()              Business logic: idempotent single-file download,
                              via a HEAD-request size check against Content-Length.
    run_full_bls_ingestion() Orchestration: sequences surveys -> files, paces
                              requests, tracks stats, prints a summary.
"""

import os
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


# --------------------------------------------------------------------------
# Infrastructure
# --------------------------------------------------------------------------

def create_retry_session(
    headers: dict,
    retry_total: int = 3,
    retry_backoff_factor: int = 2,
    retry_status_forcelist: list[int] = [429, 500, 502, 503, 504],
) -> requests.Session:
    """Creates a requests.Session that auto-retries rate limits / transient server errors."""
    session = requests.Session()
    retries = Retry(
        total=retry_total,
        backoff_factor=retry_backoff_factor,
        status_forcelist=list(retry_status_forcelist),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.headers.update(headers)
    return session


# --------------------------------------------------------------------------
# Business logic
# --------------------------------------------------------------------------

def get_directory_items(session: requests.Session, url: str) -> list[str]:
    """Scrapes a single BLS directory page and returns its sorted, de-duplicated item names."""
    response = session.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    items = []

    for link in soup.find_all("a"):
        href = link.get("href", "")

        # Clean path into segments: '/pub/time.series/ap/' -> ['pub', 'time.series', 'ap']
        segments = [segment for segment in href.strip("/").split("/") if segment]
        if not segments:
            continue

        name = segments[-1]

        # Skip parent-directory navigation links and URL queries
        if name in ("pub", "time.series", "") or href.startswith("?") or "[To Parent Directory]" in link.text:
            continue

        items.append(name)

    return sorted(set(items))


def get_all_survey_codes(
    session: requests.Session,
    base_url: str,
    ignored_items: set[str] = frozenset({"overview.txt", "compressed", "sdmx"}),
) -> list[str]:
    """Returns valid top-level survey directory codes, filtering out non-survey entries."""
    items = get_directory_items(session, base_url)
    return [item for item in items if item not in ignored_items]


def sync_file(session: requests.Session, file_url: str, local_path: str) -> str:
    """Uses a HEAD request to check file sizes before downloading."""
    
    # 1. Fetch only the headers without downloading the body
    head_response = session.head(file_url)
    
    # Some web servers block HEAD requests. Fallback to streamed GET if needed.
    if head_response.status_code in [403, 405]:
        head_response = session.get(file_url, stream=True)
        
    head_response.raise_for_status()
    
    remote_size = int(head_response.headers.get('Content-Length', 0))
    
    # Close the fallback GET connection immediately if we opened one
    head_response.close() 

    # 2. Check local file size against remote size
    if os.path.exists(local_path):
        local_size = os.path.getsize(local_path)
        if remote_size > 0 and local_size == remote_size:
            return "SKIPPED"
            
    # 3. If missing or size changed, download the fresh file
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with session.get(file_url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    
    return "DOWNLOADED"


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_full_bls_ingestion(
    session: requests.Session,
    base_url: str,
    volume_root: str,
    request_delay: float = 0.5,
    limit_surveys: int | None = None,
) -> dict:
    """
    Discovers all BLS survey directories and syncs their files into the target volume.

    Args:
        session: a requests.Session (typically from create_retry_session()).
        base_url: root BLS time-series directory URL.
        volume_root: local/Databricks Volume root path to sync files into.
        request_delay: seconds to sleep between requests to stay within BLS rate limits.
        limit_surveys: if set, only process the first N surveys (handy for a test run).

    Returns:
        Dict of run counts: {"SKIPPED": int, "DOWNLOADED": int, "FAILED": int}.
    """
    print("Discovering all BLS survey codes...")
    surveys = get_all_survey_codes(session, base_url)
    target_surveys = surveys[:limit_surveys] if limit_surveys else surveys

    print(f"Found {len(surveys)} survey directories.")
    if limit_surveys:
        print(f"Processing first {len(target_surveys)} surveys (test mode).")
    print(f"Target surveys: {target_surveys}")

    stats = {"SKIPPED": 0, "DOWNLOADED": 0, "FAILED": 0}

    for idx, survey in enumerate(target_surveys, start=1):
        clean_survey = survey.replace("pub/time.series/", "").strip("/")
        survey_url = urljoin(base_url, f"{clean_survey}/")
        survey_dir = os.path.join(volume_root, clean_survey)

        try:
            files = get_directory_items(session, survey_url)
            print(f"Found files in survey '{clean_survey}': {files}")

            for filename in files:
                file_url = urljoin(survey_url, filename)
                local_path = os.path.join(survey_dir, filename)

                try:
                    status = sync_file(session, file_url, local_path)
                    stats[status] += 1
                except Exception as e:
                    print(f"Failed {clean_survey}/{filename}: {e}")
                    stats["FAILED"] += 1

                time.sleep(request_delay)  # polite delay between file requests

        except Exception as e:
            print(f"Failed processing survey directory '{clean_survey}': {e}")

        time.sleep(request_delay)  # polite delay between survey directories
        print(f"[{idx}/{len(target_surveys)}] Survey '{clean_survey}' synced.")

    _print_summary(stats)
    return stats


def _print_summary(stats: dict) -> None:
    print("\n--- INGESTION SUMMARY ---")
    print(f"Files Skipped (Unchanged): {stats['SKIPPED']}")
    print(f"Files Downloaded/Updated:  {stats['DOWNLOADED']}")
    print(f"Files Failed:              {stats['FAILED']}")

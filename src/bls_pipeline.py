"""Data ingestion logic for BLS and DataUSA Population API.

All configuration (URLs, headers, paths, rate-limit settings) is owned by the
calling notebook and passed in as arguments.
"""

import os
import time
import hashlib
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
# BLS Business Logic
# --------------------------------------------------------------------------

def get_directory_items(session: requests.Session, url: str) -> list[str]:
    """Scrapes a BLS directory page and returns its sorted, de-duplicated item names dynamically."""
    response = session.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    items = []

    for link in soup.find_all("a"):
        href = link.get("href", "")
        segments = [segment for segment in href.strip("/").split("/") if segment]
        if not segments:
            continue

        name = segments[-1]
        
        # Skip navigation links and URL queries
        if name in ("pub", "time.series", "pr", "") or href.startswith("?") or "[To Parent Directory]" in link.text:
            continue

        items.append(name)

    return sorted(set(items))


def sync_file(session: requests.Session, file_url: str, local_path: str) -> str:
    """Uses a HEAD request to check file sizes before downloading to ensure idempotency."""
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
# Population API Business Logic
# --------------------------------------------------------------------------

def sync_population_api(session: requests.Session, endpoint_url: str, local_path: str) -> str:
    """Fetches DataUSA JSON, checks for deltas via content hashing, and overwrites if changed."""
    response = session.get(endpoint_url)
    response.raise_for_status()
    new_data = response.content

    # Check for existing delta
    if os.path.exists(local_path):
        with open(local_path, 'rb') as f:
            old_data = f.read()
        
        # Skip if content hash matches exactly (no new additions/changes)
        if hashlib.md5(new_data).hexdigest() == hashlib.md5(old_data).hexdigest():
            return "SKIPPED"

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'wb') as f:
        f.write(new_data)
        
    return "DOWNLOADED"


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_ingestion(
    session: requests.Session,
    bls_pr_url: str,
    population_api_url: str,
    volume_root: str,
    request_delay: float = 0.5,
) -> dict:
    """
    Discovers all BLS /pr/ files and Population API records, syncing them into the target volume.
    """
    stats = {"SKIPPED": 0, "DOWNLOADED": 0, "FAILED": 0}

    # 1. Process BLS PR Directory
    print(f"Discovering files dynamically at {bls_pr_url}...")
    try:
        bls_files = get_directory_items(session, bls_pr_url)
        print(f"Found {len(bls_files)} files in BLS /pr/ directory.")
        
        bls_dir = os.path.join(volume_root, "pr")
        
        for filename in bls_files:
            file_url = urljoin(bls_pr_url, filename)
            local_path = os.path.join(bls_dir, filename)

            try:
                status = sync_file(session, file_url, local_path)
                stats[status] += 1
            except Exception as e:
                print(f"Failed BLS sync {filename}: {e}")
                stats["FAILED"] += 1

            time.sleep(request_delay)  # polite delay between file requests
    except Exception as e:
        print(f"Failed processing BLS directory: {e}")

    # 2. Process Population API
    print(f"Syncing DataUSA Population API data...")
    pop_local_path = os.path.join(volume_root, "population", "population_data.json")
    try:
        status = sync_population_api(session, population_api_url, pop_local_path)
        stats[status] += 1
        print(f"Population API sync status: {status}")
    except Exception as e:
        print(f"Failed Population API sync: {e}")
        stats["FAILED"] += 1

    print("\n--- INGESTION SUMMARY ---")
    print(f"Files Skipped (Unchanged): {stats['SKIPPED']}")
    print(f"Files Downloaded/Updated:  {stats['DOWNLOADED']}")
    print(f"Files Failed:              {stats['FAILED']}")
    
    return stats
import asyncio
import aiohttp
from urllib.parse import quote
import json
import csv
import re
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def _safe_get(obj, *indices, default=None):
    try:
        for idx in indices:
            obj = obj[idx]
        return obj
    except (IndexError, TypeError, KeyError):
        return default


def _extract_place(result, query):
    place_id = _safe_get(result, 78)
    if not place_id:
        return None

    obj = {
        "id": place_id,
        "url_place": f"https://www.google.com/maps/place/?q=place_id:{place_id}",
        "title": _safe_get(result, 11, default=""),
        "category": _safe_get(result, 13, 0, default=""),
        "address": _safe_get(result, 39, default=""),
        "phoneNumber": "",
        "completePhoneNumber": "",
        "domain": _safe_get(result, 7, 1, default=""),
        "url": _safe_get(result, 7, 0, default=""),
        "coor": "",
        "stars": _safe_get(result, 4, 7, default=""),
        "reviews": _safe_get(result, 37, 1, default=""),
        "source_query": query,
    }

    phone_local = _safe_get(result, 178, 0, 1, 0, 0)
    phone_intl = _safe_get(result, 178, 0, 1, 1, 0)
    if phone_local:
        obj["phoneNumber"] = phone_local
    if phone_intl:
        obj["completePhoneNumber"] = phone_intl

    lat = _safe_get(result, 9, 2)
    lng = _safe_get(result, 9, 3)
    if lat is not None and lng is not None:
        obj["coor"] = f"{lat},{lng}"

    return obj


async def _get_search_url(session, query, lang, country):
    encoded_query = quote(query)
    maps_url = f"https://www.google.com/maps/search/{encoded_query}?hl={lang}&gl={country}"

    try:
        async with session.get(maps_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.error(f"[{query}] Maps page returned HTTP {resp.status}")
                return None
            html = await resp.text()
    except Exception as e:
        logger.error(f"[{query}] Failed to fetch Maps page: {e}")
        return None

    pb_match = re.search(r'["\'](/search\?tbm=map[^"\']+)["\']', html, flags=re.IGNORECASE)
    if not pb_match:
        logger.error(f"[{query}] Could not find tbm=map URL. HTML snippet: {html[:300]!r}")
        return None

    search_path = pb_match.group(1).replace("&amp;", "&")
    return "https://www.google.com" + search_path


async def _fetch_results_page(session, search_url, query, start=0):
    url = search_url if start == 0 else f"{search_url}&start={start}"

    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                logger.error(f"[{query}] tbm=map URL returned HTTP {resp.status} (start={start})")
                return []
            raw = await resp.text()
    except Exception as e:
        logger.error(f"[{query}] Failed to fetch results page (start={start}): {e}")
        return []

    if raw.startswith(")]}'"):
        raw = raw[4:].strip()
    else:
        logger.error(f"[{query}] Unexpected response format at start={start}. First chars: {raw[:80]!r}")
        return []

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error(f"[{query}] JSON decode error at start={start}: {e}")
        return []

    results_array = _safe_get(data, 64)
    if results_array is None:
        return []

    places = []
    for entry in results_array:
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        result = entry[1]
        if not isinstance(result, list):
            continue
        place = _extract_place(result, query)
        if place:
            places.append(place)

    return places


async def search_async(query, lang, country, limit, semaphore):
    result = []
    seen_ids = set()
    pbar = tqdm(desc=f"Scraping '{query[:30]}'", unit="results", leave=False)

    async with semaphore:
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector) as session:
            search_url = await _get_search_url(session, query, lang, country)
            if not search_url:
                pbar.close()
                return result

            page_size = 20
            start = 0
            while True:
                places = await _fetch_results_page(session, search_url, query, start)
                if not places:
                    break

                new_in_page = 0
                for place in places:
                    pid = place.get("id", "")
                    if not pid or pid in seen_ids:
                        continue

                    seen_ids.add(pid)
                    result.append(place)
                    new_in_page += 1
                    pbar.update(1)
                    if limit and len(result) >= limit:
                        break

                if limit and len(result) >= limit:
                    break

                # Stop pagination if this page produced no new unique results
                if new_in_page == 0:
                    break

                start += page_size

    pbar.close()
    return result


async def search_multiple_async(queries, lang, country, limit, max_concurrent=3):
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = [search_async(query, lang, country, limit, semaphore) for query in queries]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_results = []
    for i, res in enumerate(results):
        if isinstance(res, BaseException):
            logger.error(f"Error in query '{queries[i]}': {res}")
        else:
            all_results.extend(res)
    return all_results


def search(query, lang, country, limit, mode="standard"):
    return asyncio.run(search_async(query, lang, country, limit, asyncio.Semaphore(1)))


def search_multiple_sync(queries, lang, country, limit_per_query=None, max_concurrent=3, mode="standard"):
    return asyncio.run(search_multiple_async(queries, lang, country, limit_per_query, max_concurrent))


def search_multiple(queries, lang, country, limit, max_concurrent=3):
    return asyncio.run(search_multiple_async(queries, lang, country, limit, max_concurrent))


def save_to_csv(data, filename="data/output.csv"):
    if not data:
        print("No data to save.")
        return

    column_order = [
        "id", "url_place", "title", "category", "address",
        "phoneNumber", "completePhoneNumber", "domain", "url",
        "coor", "stars", "reviews", "source_query",
    ]

    for record in data:
        for col in column_order:
            if col not in record:
                record[col] = ""

    seen_ids = set()
    deduped = []
    for record in data:
        rid = record.get("id", "")
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        deduped.append(record)

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=column_order)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"Data saved to {filename}")

#!/usr/bin/env python3
"""
Polite scraper for yellowpages.my.

Design goals
------------
* Be a good citizen: slow, single-threaded, cached, resumable, capped.
* Never try to bypass Cloudflare or any other protection.
* Stop immediately on 403/429/robot rules so the IP is not burned.

The site is an Angular SPA whose data comes from https://api.yellowpages.my/.
This tool talks to the same public endpoints the site itself uses.

Usage examples
--------------
    py -3 yp_scraper.py categories
    py -3 yp_scraper.py probe
    py -3 yp_scraper.py search --what restaurant --where "Kuala Lumpur" \
        --type services --parent food-dining --pages 3 --out data/restaurants

Read README.md before running a large job.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

SITE = "https://www.yellowpages.my"
API = "https://api.yellowpages.my/"
ROBOTS_URLS = ("https://www.yellowpages.my/robots.txt", "https://api.yellowpages.my/robots.txt")

log = logging.getLogger("yp")


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------
class ScraperError(RuntimeError):
    pass


class BlockedError(ScraperError):
    """403 / Cloudflare challenge / explicit block. Never retried."""


class CircuitOpenError(ScraperError):
    """Too many consecutive failures; stop before doing damage."""


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: str
    from_cache: bool = False

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise ScraperError(f"Expected JSON, got {self.body[:200]!r}") from exc


class RateLimiter:
    """One request at a time, with a human-ish gap between requests."""

    def __init__(self, delay: float, jitter: float) -> None:
        self.delay = max(0.0, delay)
        self.jitter = max(0.0, jitter)
        self._last = 0.0

    def wait(self) -> None:
        gap = self.delay + random.uniform(0, self.jitter)
        remaining = self._last + gap - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        self._last = time.monotonic()


class PoliteClient:
    def __init__(
        self,
        *,
        contact: str,
        delay: float = 6.0,
        jitter: float = 4.0,
        timeout: float = 45.0,
        max_retries: int = 3,
        backoff_base: float = 10.0,
        backoff_max: float = 120.0,
        max_requests: int = 300,
        max_consecutive_failures: int = 5,
        cache_dir: Path = Path("cache"),
        cache_ttl: float = 7 * 24 * 3600,
        use_cache: bool = True,
        respect_robots: bool = True,
    ) -> None:
        self.contact = contact
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.max_requests = max_requests
        self.max_consecutive_failures = max_consecutive_failures
        self.cache_dir = cache_dir
        self.cache_ttl = cache_ttl
        self.use_cache = use_cache
        self.respect_robots = respect_robots

        self.limiter = RateLimiter(delay, jitter)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "YP-PoliteScraper/1.0 (research; single-threaded; "
                    f"contact: {contact})"
                ),
                "Accept": "application/json, text/html;q=0.8",
                "Accept-Language": "en-MY,en;q=0.9",
                "Referer": SITE + "/",
                "Origin": SITE,
            }
        )

        self.requests_made = 0
        self._consecutive_failures = 0
        self._robots: dict[str, RobotFileParser] = {}
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    # -- cache -------------------------------------------------------------
    def _cache_key(self, method: str, url: str, payload: str) -> Path:
        digest = hashlib.sha256(f"{method} {url} {payload}".encode()).hexdigest()
        return self.cache_dir / f"{digest}.json"

    def _cache_read(self, key: Path) -> Response | None:
        if not self.use_cache or not key.exists():
            return None
        try:
            obj = json.loads(key.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if time.time() - obj.get("saved_at", 0) > self.cache_ttl:
            return None
        return Response(obj["status"], obj.get("headers", {}), obj["body"], from_cache=True)

    def _cache_write(self, key: Path, resp: Response) -> None:
        if not self.use_cache:
            return
        obj = {
            "status": resp.status,
            "headers": resp.headers,
            "body": resp.body,
            "saved_at": time.time(),
        }
        tmp = key.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        tmp.replace(key)

    # -- robots ------------------------------------------------------------
    def _robots_for(self, url: str) -> RobotFileParser:
        parts = urlparse(url)
        base = f"{parts.scheme}://{parts.netloc}"
        if base in self._robots:
            return self._robots[base]

        rp = RobotFileParser()
        rp.set_url(base + "/robots.txt")
        try:
            self.limiter.wait()
            self.requests_made += 1
            resp = self.session.get(rp.url, timeout=min(self.timeout, 20))
            if resp.status_code == 200:
                rp.parse(resp.text.splitlines())
            elif 400 <= resp.status_code < 500:
                # 404 = no published rules. Still throttled by RateLimiter.
                rp.allow_all = True
                log.info("%s/robots.txt -> %s (no rules published)", base, resp.status_code)
            else:
                # Server error on robots: be conservative, but do not deadlock.
                rp.allow_all = True
                log.warning("%s/robots.txt -> %s (treating as allow)", base, resp.status_code)
        except requests.RequestException as exc:
            rp.allow_all = True
            log.warning("robots.txt unreachable (%s); treating as allow", exc)
        self._robots[base] = rp
        return rp

    def allowed(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        ua = self.session.headers["User-Agent"]
        return self._robots_for(url).can_fetch(ua, url)

    # -- request -----------------------------------------------------------
    def request(
        self,
        method: str,
        url: str,
        *,
        json_body: Any = None,
        form: dict[str, Any] | None = None,
        check_robots: bool = True,
    ) -> Response:
        if self.requests_made >= self.max_requests:
            raise ScraperError(
                f"Per-run request cap reached ({self.max_requests}). "
                "Resume later with a higher --max-requests if you must."
            )
        if check_robots and not self.allowed(url):
            raise BlockedError(f"robots.txt disallows {url}")

        payload = ""
        if json_body is not None:
            payload = json.dumps(json_body, sort_keys=True)
        elif form is not None:
            payload = json.dumps(form, sort_keys=True)

        cache_key = self._cache_key(method, url, payload)
        cached = self._cache_read(cache_key)
        if cached is not None:
            log.debug("cache hit %s %s", method, url)
            return cached

        last_error: Exception | None = None
        last_status: int | None = None
        for attempt in range(1, self.max_retries + 1):
            if self.requests_made >= self.max_requests:
                raise ScraperError(
                    f"Per-run request cap reached ({self.max_requests}). "
                    "Resume later with a higher --max-requests if you must."
                )
            self.limiter.wait()
            self.requests_made += 1
            try:
                resp = self.session.request(
                    method,
                    url,
                    json=json_body,
                    data=form,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                self._note_failure()
                self._sleep_backoff(attempt, f"network error: {exc}")
                continue

            headers = {k.lower(): v for k, v in resp.headers.items()}
            body = resp.text
            if self._is_cloudflare_challenge(resp):
                raise BlockedError(
                    "Cloudflare challenge detected. This tool will not try to bypass it. "
                    "Stop and retry much later from the same IP, or use a browser manually."
                )

            if resp.status_code == 200:
                self._consecutive_failures = 0
                out = Response(200, headers, body)
                self._cache_write(cache_key, out)
                return out

            if resp.status_code in (403, 401):
                raise BlockedError(f"HTTP {resp.status_code} for {url}; stopping to protect the IP.")

            if resp.status_code == 429 or resp.status_code == 503:
                retry_after = self._retry_after(resp)
                last_status = resp.status_code
                self._note_failure()
                self._sleep_backoff(attempt, f"HTTP {resp.status_code}", retry_after)
                continue

            if 500 <= resp.status_code < 600:
                last_status = resp.status_code
                self._note_failure()
                self._sleep_backoff(attempt, f"HTTP {resp.status_code}")
                continue

            # Other 4xx: return so the caller can show the server message.
            return Response(resp.status_code, headers, body)

        reason = f"HTTP {last_status}" if last_status else str(last_error or "unknown error")
        raise ScraperError(
            f"Giving up on {url} after {self.max_retries} attempts ({reason})"
        )

    def _note_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.max_consecutive_failures:
            raise CircuitOpenError(
                f"{self._consecutive_failures} consecutive failures. "
                "The server may be rate-limiting or unhealthy. Stop now and resume later."
            )

    def _sleep_backoff(self, attempt: int, reason: str, retry_after: float | None = None) -> None:
        if attempt >= self.max_retries:
            return
        wait = min(self.backoff_max, self.backoff_base * (2 ** (attempt - 1)))
        if retry_after is not None:
            wait = max(wait, retry_after)
        wait += random.uniform(0, 2)
        log.warning("%s; backing off %.1fs (attempt %d/%d)", reason, wait, attempt, self.max_retries)
        time.sleep(wait)

    @staticmethod
    def _retry_after(resp: requests.Response) -> float | None:
        raw = resp.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
        try:
            dt = parsedate_to_datetime(raw)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_cloudflare_challenge(resp: requests.Response) -> bool:
        if resp.status_code not in (403, 503):
            return False
        head = resp.text[:4000].lower()
        return (
            "just a moment" in head
            or "cf-chl" in head
            or "__cf_chl" in head
            or "attention required" in head
        )

    # -- domain helpers ----------------------------------------------------
    def get_json(self, url: str) -> Any:
        resp = self.request("GET", url)
        if resp.status != 200:
            raise ScraperError(f"GET {url} -> HTTP {resp.status}: {resp.body[:300]}")
        obj = resp.json()
        if isinstance(obj, dict) and obj.get("status") is False:
            raise ScraperError(f"API error for {url}: {obj.get('message')}")
        return obj

    def post_search(self, payload: dict[str, Any], *, as_form: bool) -> dict[str, Any]:
        if as_form:
            clean = {
                k: v
                for k, v in payload.items()
                if v is not None and v != ""
            }
            resp = self.request("POST", API + "post/search", form=clean)
        else:
            resp = self.request("POST", API + "post/search", json_body=payload)
        if resp.status != 200:
            raise ScraperError(f"search -> HTTP {resp.status}: {resp.body[:300]}")
        obj = resp.json()
        if not obj.get("status"):
            raise ScraperError(f"search rejected: {obj.get('message')} {obj.get('errors')}")
        return obj["data"]


# --------------------------------------------------------------------------
# SSR transfer-state (used for the category tree and profile fallback)
# --------------------------------------------------------------------------
_STATE_RE = re.compile(r'<script id="serverApp-state"[^>]*>(.*?)</script>', re.S)


def decode_transfer_state(text: str) -> dict[str, Any]:
    # Angular Universal uses non-standard entities: &q; "  &a; &  &l; <  &g; >
    text = (
        text.replace("&q;", '"')
        .replace("&a;", "&")
        .replace("&l;", "<")
        .replace("&g;", ">")
    )
    return json.loads(text)


def fetch_state(client: PoliteClient, url: str) -> dict[str, Any]:
    resp = client.request("GET", url)
    if resp.status != 200:
        raise ScraperError(f"GET {url} -> HTTP {resp.status}")
    match = _STATE_RE.search(resp.body)
    if not match:
        raise ScraperError("No Angular transfer state found on page")
    return decode_transfer_state(match.group(1))


def find_state_value(state: dict[str, Any], needle: str) -> Any:
    for key, value in state.items():
        if needle in key:
            if isinstance(value, dict) and "body" in value:
                return value["body"]
            return value
    return None


def get_app_data(client: PoliteClient) -> dict[str, Any]:
    """Category tree, from the SSR page (the raw API route 500s sometimes)."""
    try:
        state = fetch_state(client, f"{SITE}/services/l")
        body = find_state_value(state, "/home/app_data")
        if body and isinstance(body, dict) and body.get("data"):
            return body["data"]
    except ScraperError as exc:
        log.warning("Could not read app data from SSR page: %s", exc)

    obj = client.get_json(API + "home/app_data")
    return obj["data"]


# --------------------------------------------------------------------------
# Scraping
# --------------------------------------------------------------------------
SEARCH_FIELDS = [
    "specialization",
    "parent_specialization",
    "search",
    "location",
    "user_location",
    "price",
    "distance",
    "job_qualification",
    "job_type",
    "job_level",
    "hireme_type",
    "hireme_rate",
    "hireme_expertise",
    "buysell_condition",
    "buysell_type",
    "buysell_deal_method",
    "buysell_method",
]


def build_payload(args: argparse.Namespace, page: int) -> dict[str, Any]:
    payload: dict[str, Any] = {field: None for field in SEARCH_FIELDS}
    payload.update(
        {
            "type": args.type,
            "specialization": args.sub,
            "parent_specialization": args.parent,
            "search": args.what,
            "location": args.where,
            "page": page,
            "limit": args.limit,
            "sort": args.sort,
        }
    )
    return payload


def profile_detail(client: PoliteClient, slug: str) -> dict[str, Any] | None:
    """Try the JSON route first, then the SSR page."""
    try:
        obj = client.get_json(f"{API}profile/slug/{slug}")
        if isinstance(obj, dict) and obj.get("data"):
            return obj["data"]
    except BlockedError:
        raise
    except ScraperError as exc:
        log.debug("profile API failed for %s: %s", slug, exc)

    try:
        state = fetch_state(client, f"{SITE}/{slug}")
        body = find_state_value(state, "/profile/slug/")
        if isinstance(body, dict) and body.get("data"):
            return body["data"]
    except ScraperError as exc:
        log.warning("profile fallback failed for %s: %s", slug, exc)
    return None


def flatten_record(record: dict[str, Any]) -> dict[str, Any]:
    coords = record.get("coordinates") or []
    first = coords[0] if coords and isinstance(coords[0], dict) else {}
    categories = record.get("categories") or []
    parents = record.get("parent_categories") or []
    return {
        "id": record.get("id"),
        "ref_id": record.get("ref_id"),
        "slug": record.get("slug"),
        "title": record.get("title"),
        "type": record.get("type"),
        "plan": record.get("plan"),
        "rating": record.get("rating"),
        "rating_count": record.get("rating_count"),
        "summary": record.get("summary"),
        "address": " | ".join(filter(None, record.get("addresses") or [])),
        "locations": " | ".join(filter(None, record.get("locations") or [])),
        "phones": " | ".join(filter(None, record.get("phone_numbers") or [])),
        "categories": " | ".join(c.get("name", "") for c in categories if isinstance(c, dict)),
        "parent_categories": " | ".join(
            c.get("name", "") for c in parents if isinstance(c, dict)
        ),
        "lat": first.get("lat"),
        "lon": first.get("lon"),
        "url": f"{SITE}/{record.get('slug', '')}",
        "scraped_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


CSV_FIELDS = list(flatten_record({}).keys())


def load_seen(jsonl_path: Path) -> set[str]:
    seen: set[str] = set()
    if not jsonl_path.exists():
        return seen
    with jsonl_path.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = obj.get("slug") or obj.get("id")
            if key:
                seen.add(str(key))
    return seen


def append_records(jsonl_path: Path, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    csv_path = jsonl_path.with_suffix(".csv")
    new_file = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        for record in records:
            writer.writerow(flatten_record(record))
    return len(records)


def load_state(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_categories(client: PoliteClient, _args: argparse.Namespace) -> int:
    app_data = get_app_data(client)
    categories = app_data.get("categories", {})
    for section, parents in categories.items():
        print(f"\n[{section}]")
        for parent in parents:
            print(f"  {parent.get('slug'):<32} {parent.get('name')}")
            for child in parent.get("child") or []:
                print(f"      - {child.get('slug'):<28} {child.get('name')}")
    return 0


def cmd_probe(client: PoliteClient, _args: argparse.Namespace) -> int:
    checks: list[tuple[str, str, Any]] = [
        ("HTTP", "GET", SITE + "/robots.txt"),
        ("HTTP", "GET", API + "states"),
        ("HTTP", "GET", API + "home"),
        ("SSR", "GET", SITE + "/services/l"),
    ]
    for label, method, url in checks:
        try:
            resp = client.request(method, url, check_robots=False)
            note = "cache" if resp.from_cache else "live"
            print(f"{label:4} {resp.status:>3}  {note:5}  {url}")
        except BlockedError as exc:
            print(f"{label:4} BLOCK {url}  {exc}")
            return 2
        except ScraperError as exc:
            print(f"{label:4} ERROR {url}  {exc}")
    return 0


def cmd_search(client: PoliteClient, args: argparse.Namespace) -> int:
    if args.type == "services" and not args.parent:
        log.error(
            "Services searches need --parent (e.g. --parent food-dining). "
            "Use `categories` to list valid slugs; without it the API returns HTTP 500."
        )
        return 1

    out = Path(args.out)
    jsonl_path = out.with_suffix(".jsonl")
    state_path = out.with_suffix(".state.json")
    state = {} if args.restart else load_state(state_path)
    seen = set() if args.restart else load_seen(jsonl_path)

    key = "|".join(
        str(x)
        for x in (args.type, args.parent, args.sub, args.what, args.where, args.sort)
    )
    start_page = int(state.get(key, {}).get("next_page", 1))
    total_written = 0

    for page in range(start_page, start_page + args.pages):
        payload = build_payload(args, page)
        if args.dry_run:
            print(json.dumps(payload, indent=2))
            continue

        log.info(
            "page %d: type=%s parent=%s sub=%s what=%r where=%r",
            page,
            args.type,
            args.parent,
            args.sub,
            args.what,
            args.where,
        )
        try:
            data = client.post_search(payload, as_form=args.form)
        except BlockedError:
            raise
        except CircuitOpenError:
            raise
        except ScraperError as exc:
            log.error("Search failed: %s", exc)
            log.error(
                "The site's search API is returning errors for this query. "
                "Do not retry in a loop; wait and try again later."
            )
            return 1

        results = data.get("results") or []
        total = data.get("total")
        log.info("page %d: %d results (total=%s)", page, len(results), total)

        fresh = []
        for record in results:
            key_id = str(record.get("slug") or record.get("id") or "")
            if not key_id or key_id in seen:
                continue
            if args.details and record.get("slug"):
                detail = profile_detail(client, record["slug"])
                if detail:
                    record = {**record, "detail": detail}
            seen.add(key_id)
            fresh.append(record)

        total_written += append_records(jsonl_path, fresh)
        state[key] = {"next_page": page + 1, "total": total, "updated": datetime.now().isoformat()}
        save_state(state_path, state)

        if not results:
            break
        if isinstance(total, int) and page * args.limit >= total:
            break
        if total_written >= args.max_records:
            log.info("Record cap reached (%d)", args.max_records)
            break

    print(f"Wrote {total_written} new records to {jsonl_path}")
    return 0


def build_client(args: argparse.Namespace) -> PoliteClient:
    return PoliteClient(
        contact=args.email,
        delay=args.delay,
        jitter=args.jitter,
        timeout=args.timeout,
        max_retries=args.retries,
        max_requests=args.max_requests,
        max_consecutive_failures=args.max_failures,
        cache_dir=Path(args.cache_dir),
        cache_ttl=args.cache_ttl,
        use_cache=not args.no_cache,
        respect_robots=not args.ignore_robots,
    )


GLOBAL_VALUE_FLAGS = {
    "--email",
    "--delay",
    "--jitter",
    "--timeout",
    "--retries",
    "--max-requests",
    "--max-failures",
    "--cache-dir",
    "--cache-ttl",
}
GLOBAL_BOOL_FLAGS = {"--no-cache", "--ignore-robots", "-v", "--verbose"}
COMMANDS = {"categories", "probe", "search"}


def reorder_global_args(argv: list[str]) -> list[str]:
    """Allow global flags after the subcommand, e.g. `search --delay 10`."""
    try:
        cmd_index = next(i for i, arg in enumerate(argv) if arg in COMMANDS)
    except StopIteration:
        return argv

    before, after = argv[:cmd_index], argv[cmd_index + 1 :]
    moved: list[str] = []
    kept: list[str] = []
    i = 0
    while i < len(after):
        arg = after[i]
        name = arg.split("=", 1)[0]
        if name in GLOBAL_VALUE_FLAGS:
            moved.append(arg)
            if "=" not in arg and i + 1 < len(after):
                moved.append(after[i + 1])
                i += 2
                continue
        elif arg in GLOBAL_BOOL_FLAGS:
            moved.append(arg)
        else:
            kept.append(arg)
        i += 1
    return before + moved + [argv[cmd_index]] + kept


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Polite single-threaded scraper for yellowpages.my",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--email", default="set-your-contact@example.com",
                        help="Contact address embedded in the User-Agent")
    parser.add_argument("--delay", type=float, default=6.0, help="Seconds between requests")
    parser.add_argument("--jitter", type=float, default=4.0, help="Random extra seconds added")
    parser.add_argument("--timeout", type=float, default=45.0, help="Per-request timeout")
    parser.add_argument("--retries", type=int, default=3, help="Attempts per request on 5xx")
    parser.add_argument("--max-requests", type=int, default=300, help="Hard cap per run")
    parser.add_argument("--max-failures", type=int, default=5,
                        help="Abort after this many consecutive failures")
    parser.add_argument("--cache-dir", default="cache")
    parser.add_argument("--cache-ttl", type=float, default=7 * 24 * 3600,
                        help="Cache lifetime in seconds")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--ignore-robots", action="store_true",
                        help="Not recommended; robots.txt currently has no rules anyway")
    parser.add_argument("-v", "--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    p_cat = sub.add_parser("categories", help="Print the site's category tree")
    p_cat.set_defaults(func=cmd_categories)

    p_probe = sub.add_parser("probe", help="Check endpoints without retrying much")
    p_probe.set_defaults(func=cmd_probe)

    p_search = sub.add_parser("search", help="Search listings and save them")
    p_search.add_argument("--what", default=None, help="Keyword, e.g. restaurant")
    p_search.add_argument("--where", default=None, help="Location, e.g. 'Kuala Lumpur'")
    p_search.add_argument("--type", default="services",
                          choices=["services", "jobs", "hireme", "buysell", "education"])
    p_search.add_argument("--parent", default=None,
                          help="Parent category slug, e.g. food-dining")
    p_search.add_argument("--sub", default=None,
                          help="Child category slug, e.g. restaurants")
    p_search.add_argument("--sort", default="recommended",
                          choices=["recommended", "rating"])
    p_search.add_argument("--limit", type=int, default=12, help="Results per page")
    p_search.add_argument("--pages", type=int, default=1, help="Max pages to fetch this run")
    p_search.add_argument("--max-records", type=int, default=200)
    p_search.add_argument("--details", action="store_true",
                          help="Also fetch each profile page (doubles requests)")
    p_search.add_argument("--out", default="data/listings", help="Output path without extension")
    p_search.add_argument("--restart", action="store_true", help="Ignore saved progress")
    p_search.add_argument("--form", action="store_true",
                          help="Send form-encoded body instead of JSON")
    p_search.add_argument("--dry-run", action="store_true", help="Print payloads only")
    p_search.set_defaults(func=cmd_search)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw = list(sys.argv[1:] if argv is None else argv)
    args = parser.parse_args(reorder_global_args(raw))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    if "set-your-contact" in args.email:
        log.warning(
            "Set --email to a real contact address so the site owner can reach you."
        )

    client = build_client(args)
    try:
        return int(args.func(client, args) or 0)
    except BlockedError as exc:
        log.error("Stopped: %s", exc)
        return 2
    except CircuitOpenError as exc:
        log.error("Stopped: %s", exc)
        return 3
    except KeyboardInterrupt:
        log.error("Interrupted; progress has been saved.")
        return 130


if __name__ == "__main__":
    sys.exit(main())

import os
import re
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

INPUT_CSV = os.environ.get("INPUT_CSV", "direct job pages.csv")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", INPUT_CSV)  # overwrite in place by default
MAX_ROWS = int(os.environ.get("MAX_ROWS", "0"))  # 0 = no limit

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LeadFinder/1.0)"}
TIMEOUT = 12

KEYWORDS_HINT = [
    "career", "careers", "job", "jobs", "vacanc", "recruit", "work-with-us",
    "work for us", "working-for-us", "join", "join-our-team", "opportunit",
]

COMMON_PATHS = [
    "/careers", "/jobs", "/vacancies", "/recruitment", "/work-with-us", "/join-our-team",
    "/work-for-us", "/about/work-with-us", "/about-us/join-our-team", "/vacancy", "/vacancies/",
]

def norm_root(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    u = url.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    p = urlparse(u)
    host = p.netloc
    if host.startswith("www."):
        host = host[4:]
    return f"{p.scheme}://{host}"

def safe_get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text

def safe_head(url: str) -> str:
    r = requests.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code < 400:
        return r.url
    return ""

def looks_like_jobs_url(u: str) -> bool:
    lu = u.lower()
    return any(k in lu for k in KEYWORDS_HINT)

def score_url(u: str) -> int:
    lu = u.lower()
    s = 0
    for k in KEYWORDS_HINT:
        if k in lu:
            s += 3
    # shorter paths slightly preferred
    s += max(0, 10 - lu.count("/"))
    return s

def find_from_sitemap(root: str) -> str:
    sitemaps = []
    # robots -> sitemap
    try:
        robots = safe_get(root + "/robots.txt")
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
    except Exception:
        pass

    if not sitemaps:
        sitemaps = [root + "/sitemap.xml", root + "/sitemap_index.xml"]

    urls = []
    for sm in sitemaps:
        try:
            xml = safe_get(sm)
            urls += re.findall(r"<loc>(.*?)</loc>", xml, flags=re.I)
        except Exception:
            continue

    jobish = [u for u in urls if isinstance(u, str) and looks_like_jobs_url(u)]
    if jobish:
        return sorted(jobish, key=score_url, reverse=True)[0]
    return ""

def find_from_homepage(root: str) -> str:
    try:
        html = safe_get(root)
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        for a in soup.select("a[href]"):
            text = (a.get_text(" ", strip=True) or "").lower()
            href = a["href"].strip()
            absu = urljoin(root + "/", href)

            if any(k in text for k in ["jobs", "job", "vacanc", "recruit", "careers", "work with us", "join our team"]) or looks_like_jobs_url(absu):
                # keep only same-site links
                if urlparse(absu).netloc.replace("www.", "") == urlparse(root).netloc.replace("www.", ""):
                    candidates.append(absu)

        candidates = list(dict.fromkeys(candidates))
        if candidates:
            return sorted(candidates, key=score_url, reverse=True)[0]
    except Exception:
        pass
    return ""

def try_common_paths(root: str) -> str:
    for path in COMMON_PATHS:
        cand = root + path
        try:
            found = safe_head(cand)
            if found:
                return found
        except Exception:
            pass
    return ""

def find_jobs_page(url: str) -> str:
    root = norm_root(url)
    if not root:
        return ""

    # 1) sitemap
    u = find_from_sitemap(root)
    if u:
        return u

    # 2) homepage links
    u = find_from_homepage(root)
    if u:
        return u

    # 3) common paths
    u = try_common_paths(root)
    if u:
        return u

    return ""

def main():
    df = pd.read_csv(INPUT_CSV)
    if "Vacancies" not in df.columns:
        raise ValueError("Expected a 'Vacancies' column (column H).")

    n = len(df)
    limit = n if MAX_ROWS <= 0 else min(n, MAX_ROWS)

    updated = 0
    for i in range(limit):
        row = df.iloc[i]
        existing = row.get("Vacancies")
        if isinstance(existing, str) and existing.strip():
            continue

        url = str(row.get("URL", "") or "")
        jobs = find_jobs_page(url)

        if jobs:
            df.at[i, "Vacancies"] = jobs
            updated += 1

        # be polite + reduce blocking/rate-limits
        time.sleep(0.25 + random.random() * 0.35)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Updated {updated} rows. Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

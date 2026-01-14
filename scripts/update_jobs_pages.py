import os
import re
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

INPUT_CSV = os.environ.get("INPUT_CSV", "direct job pages.csv")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", INPUT_CSV)
MAX_ROWS = int(os.environ.get("MAX_ROWS", "750"))  # 0 = no limit

# If you ever want batching by offset later:
START_ROW = int(os.environ.get("START_ROW", "0"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IntelligenceHub/1.0)"}
TIMEOUT = 12

# Good and bad signals to reduce false positives like "membership" pages
GOOD_URL_HINTS = [
    "career", "careers", "job", "jobs", "vacanc", "recruit", "work-with-us",
    "work-for-us", "working-for-us", "join-our-team", "join-us", "opportunit",
    "vacancy", "our-people", "people-and-culture"
]
BAD_URL_HINTS = [
    "membership", "member", "join-our-community", "donate", "shop", "volunteer",
    "training", "event", "news", "blog", "press", "privacy", "cookie", "terms"
]

GOOD_TEXT_HINTS = [
    "vacancies", "jobs", "careers", "work with us", "join our team", "recruitment",
    "current opportunities", "latest vacancies", "apply now", "closing date", "salary"
]
BAD_TEXT_HINTS = [
    "membership", "become a member", "join the network", "sign up", "subscription",
    "donate", "volunteer", "fundraise", "newsletter"
]

COMMON_PATHS = [
    "/careers", "/jobs", "/vacancies", "/recruitment", "/work-with-us",
    "/work-for-us", "/working-for-us", "/join-our-team", "/join-us",
    "/about/work-with-us", "/about-us/join-our-team"
]

def norm_root(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    u = url.strip()
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    p = urlparse(u)
    host = p.netloc.replace("www.", "")
    return f"{p.scheme}://{host}"

def same_site(root: str, candidate: str) -> bool:
    try:
        r = urlparse(root).netloc.replace("www.", "")
        c = urlparse(candidate).netloc.replace("www.", "")
        return r == c or c.endswith("." + r)
    except Exception:
        return False

def safe_get(session: requests.Session, url: str) -> str:
    r = session.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    return r.text

def safe_head(session: requests.Session, url: str) -> str:
    r = session.head(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
    if r.status_code < 400:
        return r.url
    return ""

def looks_jobish(url: str) -> bool:
    lu = url.lower()
    if any(bad in lu for bad in BAD_URL_HINTS):
        return False
    return any(g in lu for g in GOOD_URL_HINTS)

def score_candidate(url: str, anchor_text: str = "") -> int:
    """
    Score candidates so we prefer real jobs pages over "membership/join" pages.
    """
    lu = (url or "").lower()
    lt = (anchor_text or "").lower()

    score = 0

    # Strong positives
    for good in ["vacanc", "career", "jobs", "recruit", "work-with-us", "work for us", "join our team"]:
        if good in lu:
            score += 12
        if good in lt:
            score += 10

    # Mild positives
    if "opportun" in lu or "opportun" in lt:
        score += 6

    # Hard negatives
    for bad in BAD_URL_HINTS:
        if bad in lu or bad in lt:
            score -= 40

    # Prefer shorter, cleaner paths
    score += max(0, 10 - lu.count("/"))

    return score

def validate_jobs_page(session: requests.Session, url: str) -> tuple[int, str]:
    """
    Fetch the page and check for hiring language.
    Returns (confidence 0-100, type_label)
    """
    try:
        html = safe_get(session, url)
        soup = BeautifulSoup(html, "html.parser")

        title = (soup.title.get_text(" ", strip=True) if soup.title else "").lower()
        body_text = soup.get_text(" ", strip=True).lower()

        confidence = 30  # base if page loads

        # Strong positive signals
        if any(x in title for x in ["vacanc", "career", "jobs", "recruit"]):
            confidence += 25
        if any(x in body_text for x in ["apply now", "closing date", "salary", "job description", "specification"]):
            confidence += 25
        if any(x in body_text for x in ["current vacancies", "vacancies", "our vacancies", "latest vacancies"]):
            confidence += 20

        # Negative signals
        if any(x in body_text for x in BAD_TEXT_HINTS):
            confidence -= 35

        # Clamp
        confidence = max(0, min(100, confidence))

        # Type classification
        if confidence >= 70:
            return confidence, "jobs_page"
        if confidence >= 45:
            return confidence, "maybe_jobs"
        return confidence, "unlikely_jobs"

    except Exception:
        return 0, "unreachable"

def extract_sitemap_urls(xml_text: str) -> list[str]:
    return re.findall(r"<loc>(.*?)</loc>", xml_text, flags=re.I)

def find_from_sitemap(session: requests.Session, root: str) -> tuple[str, str]:
    """
    Returns (jobs_page_url, sitemap_url_used)
    """
    sitemaps = []
    sitemap_used = ""

    # Try robots.txt first
    try:
        robots = safe_get(session, root + "/robots.txt")
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
    except Exception:
        pass

    # fallback
    if not sitemaps:
        sitemaps = [root + "/sitemap.xml", root + "/sitemap_index.xml"]

    all_urls = []
    for sm in sitemaps:
        try:
            xml = safe_get(session, sm)
            urls = extract_sitemap_urls(xml)
            if urls:
                sitemap_used = sm
                all_urls.extend(urls)
        except Exception:
            continue

    # Filter to same site and jobish urls
    candidates = []
    for u in all_urls:
        if not isinstance(u, str):
            continue
        if not same_site(root, u):
            continue
        if looks_jobish(u):
            candidates.append(u)

    candidates = list(dict.fromkeys(candidates))
    if not candidates:
        return "", sitemap_used

    # Score and validate top few to avoid false positives
    candidates_sorted = sorted(candidates, key=lambda u: score_candidate(u), reverse=True)
    top = candidates_sorted[:6]

    best_url = ""
    best_score = -999
    for u in top:
        conf, _typ = validate_jobs_page(session, u)
        combined = score_candidate(u) + conf
        if combined > best_score:
            best_score = combined
            best_url = u

    return best_url, sitemap_used

def find_from_homepage(session: requests.Session, root: str) -> str:
    try:
        html = safe_get(session, root)
        soup = BeautifulSoup(html, "html.parser")

        candidates = []
        for a in soup.select("a[href]"):
            text = (a.get_text(" ", strip=True) or "")
            href = (a.get("href") or "").strip()
            if not href:
                continue

            absu = urljoin(root + "/", href)

            if not same_site(root, absu):
                continue

            # quick filter to reduce junk
            if any(k in (text or "").lower() for k in GOOD_TEXT_HINTS) or looks_jobish(absu):
                candidates.append((absu, text))

        candidates = list(dict.fromkeys(candidates))
        if not candidates:
            return ""

        # pick best by score + a quick validation
        candidates_sorted = sorted(candidates, key=lambda x: score_candidate(x[0], x[1]), reverse=True)
        top = candidates_sorted[:8]

        best_url = ""
        best_score = -999
        for (u, t) in top:
            conf, _typ = validate_jobs_page(session, u)
            combined = score_candidate(u, t) + conf
            if combined > best_score:
                best_score = combined
                best_url = u

        return best_url
    except Exception:
        return ""

def try_common_paths(session: requests.Session, root: str) -> str:
    for path in COMMON_PATHS:
        cand = root + path
        try:
            found = safe_head(session, cand)
            if found:
                # validate quickly
                conf, typ = validate_jobs_page(session, found)
                if typ != "unlikely_jobs":
                    return found
        except Exception:
            pass
    return ""

def find_jobs_page(session: requests.Session, org_url: str) -> dict:
    root = norm_root(org_url)
    if not root:
        return {"vacancies": "", "confidence": 0, "type": "no_url", "sitemap": ""}

    # 1) sitemap
    jobs_url, sitemap_used = find_from_sitemap(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    # 2) homepage links
    jobs_url = find_from_homepage(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    # 3) common paths
    jobs_url = try_common_paths(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    return {"vacancies": "", "confidence": 0, "type": "not_found", "sitemap": sitemap_used}

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Keep Vacancies exactly as you want (column H in your file)
    if "Vacancies" not in df.columns:
        df["Vacancies"] = ""

    # Optional helpful columns (won't break anything)
    if "Vacancies_Confidence" not in df.columns:
        df["Vacancies_Confidence"] = ""
    if "Vacancies_Type" not in df.columns:
        df["Vacancies_Type"] = ""
    if "Vacancies_Sitemap" not in df.columns:
        df["Vacancies_Sitemap"] = ""

    return df

def main():
    df = pd.read_csv(INPUT_CSV)
    df = ensure_cols(df)

    n = len(df)
    start = max(0, START_ROW)

    if MAX_ROWS <= 0:
        end = n
    else:
        end = min(n, start + MAX_ROWS)

    session = requests.Session()

    updated = 0
    checked = 0

    for i in range(start, end):
        row = df.iloc[i]
        existing = row.get("Vacancies", "")

        # Skip already filled
        if isinstance(existing, str) and existing.strip():
            continue

        org_url = str(row.get("URL", "") or "").strip()
        res = find_jobs_page(session, org_url)

        if res["vacancies"]:
            df.at[i, "Vacancies"] = res["vacancies"]
            df.at[i, "Vacancies_Confidence"] = res["confidence"]
            df.at[i, "Vacancies_Type"] = res["type"]
            df.at[i, "Vacancies_Sitemap"] = res["sitemap"]
            updated += 1

        checked += 1

        # polite delay (much faster than before)
        time.sleep(0.06 + random.random() * 0.10)

        # progress log (shows in Actions)
        if checked % 50 == 0:
            print(f"Processed {checked} rows in this run, updated {updated} so far...")

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Done. Checked {checked} rows, updated {updated}. Wrote {OUTPUT_CSV}")

if __name__ == "__main__":
    main()

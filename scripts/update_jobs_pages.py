import os
import re
import json
import time
import random
import datetime as dt
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

# ----------------------------
# Config
# ----------------------------
INPUT_CSV = os.environ.get("INPUT_CSV", "direct job pages.csv")
OUTPUT_CSV = os.environ.get("OUTPUT_CSV", INPUT_CSV)
MAX_ROWS = int(os.environ.get("MAX_ROWS", "750"))  # 0 = no limit
STATE_PATH = os.environ.get("STATE_PATH", ".state/jobs_pages_state.json")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IntelligenceHub/1.0)"}
TIMEOUT = 12

GOOD_URL_HINTS = [
    "career", "careers", "job", "jobs", "vacanc", "recruit", "work-with-us",
    "work-for-us", "working-for-us", "join-our-team", "join-us", "opportunit",
    "vacancy"
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

# Faster but still polite
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.06"))
SLEEP_RAND = float(os.environ.get("SLEEP_RAND", "0.10"))


# ----------------------------
# Helpers
# ----------------------------
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
    lu = (url or "").lower()
    lt = (anchor_text or "").lower()
    score = 0

    for good in ["vacanc", "career", "jobs", "recruit", "work-with-us", "work for us", "join our team"]:
        if good in lu:
            score += 12
        if good in lt:
            score += 10

    if "opportun" in lu or "opportun" in lt:
        score += 6

    for bad in BAD_URL_HINTS:
        if bad in lu or bad in lt:
            score -= 40

    score += max(0, 10 - lu.count("/"))
    return score


def validate_jobs_page(session: requests.Session, url: str) -> tuple[int, str]:
    try:
        html = safe_get(session, url)
        soup = BeautifulSoup(html, "html.parser")

        title = (soup.title.get_text(" ", strip=True) if soup.title else "").lower()
        body_text = soup.get_text(" ", strip=True).lower()

        confidence = 30

        if any(x in title for x in ["vacanc", "career", "jobs", "recruit"]):
            confidence += 25
        if any(x in body_text for x in ["apply now", "closing date", "salary", "job description", "specification"]):
            confidence += 25
        if any(x in body_text for x in ["current vacancies", "vacancies", "our vacancies", "latest vacancies"]):
            confidence += 20

        if any(x in body_text for x in BAD_TEXT_HINTS):
            confidence -= 35

        confidence = max(0, min(100, confidence))

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
    sitemaps = []
    sitemap_used = ""

    try:
        robots = safe_get(session, root + "/robots.txt")
        for line in robots.splitlines():
            if line.lower().startswith("sitemap:"):
                sitemaps.append(line.split(":", 1)[1].strip())
    except Exception:
        pass

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

            if any(k in (text or "").lower() for k in GOOD_TEXT_HINTS) or looks_jobish(absu):
                candidates.append((absu, text))

        candidates = list(dict.fromkeys(candidates))
        if not candidates:
            return ""

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

    jobs_url, sitemap_used = find_from_sitemap(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    jobs_url = find_from_homepage(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    jobs_url = try_common_paths(session, root)
    if jobs_url:
        conf, typ = validate_jobs_page(session, jobs_url)
        return {"vacancies": jobs_url, "confidence": conf, "type": typ, "sitemap": sitemap_used}

    return {"vacancies": "", "confidence": 0, "type": "not_found", "sitemap": sitemap_used}


def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "Vacancies" not in df.columns:
        df["Vacancies"] = ""

    # optional extras
    for col in ["Vacancies_Confidence", "Vacancies_Type", "Vacancies_Sitemap"]:
        if col not in df.columns:
            df[col] = ""

    return df


def load_state() -> dict:
    try:
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {"next_start_row": 0}


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def main():
    df = pd.read_csv(INPUT_CSV)
    df = ensure_cols(df)

    n = len(df)
    if n == 0:
        print("CSV is empty, nothing to do.")
        return

    state = load_state()
    start = int(state.get("next_start_row", 0)) % n

    # run size
    if MAX_ROWS <= 0:
        run_len = n
    else:
        run_len = min(MAX_ROWS, n)

    # We'll do a wrap-around slice so we keep moving forward
    indices = list(range(start, min(n, start + run_len)))
    if start + run_len > n:
        indices += list(range(0, (start + run_len) - n))

    session = requests.Session()

    updated = 0
    checked = 0

    for i in indices:
        existing = df.at[i, "Vacancies"]
        if isinstance(existing, str) and existing.strip():
            checked += 1
            continue

        org_url = str(df.at[i, "URL"] or "").strip()
        res = find_jobs_page(session, org_url)

        if res["vacancies"]:
            df.at[i, "Vacancies"] = res["vacancies"]
            df.at[i, "Vacancies_Confidence"] = res["confidence"]
            df.at[i, "Vacancies_Type"] = res["type"]
            df.at[i, "Vacancies_Sitemap"] = res["sitemap"]
            updated += 1

        checked += 1

        # polite delay
        time.sleep(SLEEP_MIN + random.random() * SLEEP_RAND)

        if checked % 50 == 0:
            print(f"Processed {checked}/{len(indices)} rows this run; updated {updated} so far...")

    # Advance the pointer
    next_start = (start + run_len) % n
    state_out = {
        "next_start_row": next_start,
        "last_run_utc": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "rows_processed_last_run": len(indices),
        "rows_updated_last_run": updated,
    }
    save_state(state_out)

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"Done. Checked {checked}, updated {updated}.")
    print(f"Next start row will be {next_start}.")
    print(f"Wrote {OUTPUT_CSV} and {STATE_PATH}.")


if __name__ == "__main__":
    main()

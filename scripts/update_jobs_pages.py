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

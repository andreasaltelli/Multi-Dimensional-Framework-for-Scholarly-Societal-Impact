#!/usr/bin/env python3
"""
Beyond Citations: Towards an Experimental Multi-Dimensional Framework
for Scholarly Societal Impact - pipeline reproduction code
======================================================

HOW TO USE
----------
1. Change SM_FIELD below to the desired field (must match the Stanford xlsx).
2. Run:  python pipeline.py
3. The script produces a ready-to-analyse xlsx in OUTPUT_DIR.


OUTPUT COLUMNS
--------------
  rank (ns), c (ns), h_index,
  tot_docs, top_docs, top_jrnl_cit, %_top_jrnl_cit,
  wiki, epo, rsd, news, overton_policy_docs, rcr_median

PIPELINE STEPS
--------------
  1. Sample 250 scientists (stratified uniform sampling, common-surname
     filtered to reduce homonymy risk)
  2. Retrieve Scopus Author ID and h-index (Scopus Author Search &
     Retrieval APIs)
  3. Compute journal-based metrics: total documents, documents in top
     journals (SciMago top-100k), citations from top journals, and the
     fraction of total citations coming from top journals (Scopus +
     SciMago 2024 ranking)
  4. Retrieve Wikipedia average daily page-views over 30 days
     (Wikimedia Pageviews REST API)
  5. Retrieve EPO inventor patent count (EPO Open Patent Services v3.2)
  6. Compute Rao-Stirling Diversity index from Scopus ASJC subject codes
     (up to 50 most-recent publications per author)
  7. Count Google News RSS mentions (Google News RSS, capped at 100)
  8. Retrieve Overton policy document citations (Overton API)
  9. Retrieve NIH Relative Citation Ratio — median RCR across eligible
     PubMed-indexed publications (PubMed eSearch + NIH iCite API)
 10. Rename columns to final output names and export xlsx

NOTE: Clinical trials step is intentionally omitted — it is
medicine-specific and not meaningful for other fields.
"""

# ══════════════════════════════════════════════════════════════════════════════
#  USER CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SM_FIELD   = "Economics & Business"   # change to your desired Stanford field
                                       # e.g. "Physics & Astronomy"
N_SAMPLE   = 250
RANDOM_SEED = 42

# Paths
STANFORD_XLSX  = "" #update with the path to STenford's source file, also available at: https://elsevier.digitalcommonsdata.com/datasets/btchxktzyw/8/files/39704d33-d2f8-4983-9cd6-d13919025b8c
SCIMAGO_CSV    = "" #update with the path to Scimago's top journal ranking file, also available at https://www.scimagojr.com/journalrank.php?out=xls
BASE_OUTPUT_DIR= "" #update with the path where to save the output file

SCOPUS_API_KEY  = ""   
OVERTON_API_KEY   = ""
EPO_CONSUMER_KEY  = ""
EPO_CONSUMER_SECRET=""

# Step-level switches — set False to skip a step on re-runs
RUN_STEP_1_SAMPLE   = True
RUN_STEP_2_HINDEX   = True
RUN_STEP_3_JOURNALS = True
RUN_STEP_4_WIKI     = True
RUN_STEP_5_PATENTS  = True
RUN_STEP_6_RSD      = True
RUN_STEP_7_NEWS     = True
RUN_STEP_8_OVERTON  = True
RUN_STEP_9_RCR      = True

# ══════════════════════════════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import re
import time
import json
import base64
import unicodedata
import datetime
import pathlib
import xml.etree.ElementTree as ET
from collections import Counter
from itertools   import combinations_with_replacement

import numpy  as np
import pandas as pd
import requests

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING — writes simultaneously to stdout AND a persistent log file
# ══════════════════════════════════════════════════════════════════════════════

import logging
import sys
import traceback as _traceback

_LOG_FORMAT  = "%(asctime)s  %(levelname)-8s  %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.DEBUG,
    format=_LOG_FORMAT,
    datefmt=_DATE_FORMAT,
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("pipeline")

def _attach_file_log(log_path):
    fh = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
    log.addHandler(fh)
    log.info(f"Log file: {log_path}")


# ══════════════════════════════════════════════════════════════════════════════
#  DERIVED PATHS
# ══════════════════════════════════════════════════════════════════════════════

_FIELD_SLUG  = re.sub(r"[^a-z0-9]+", "_", SM_FIELD.lower()).strip("_")
FIELD_DIR    = pathlib.Path(BASE_OUTPUT_DIR) / _FIELD_SLUG.upper()
FIELD_DIR.mkdir(parents=True, exist_ok=True)
WORKING_CSV  = FIELD_DIR / f"sampled_{_FIELD_SLUG}_{N_SAMPLE}.csv"
FINAL_XLSX   = FIELD_DIR / f"{_FIELD_SLUG}_final.xlsx"

# Internal column names used throughout the pipeline.
# These are renamed to the final output names in step10_export().
FINAL_COLUMNS = [
    "Surname", "Name", "inst_name", "cntry",
    "rank (ns)", "c (ns)",
    "h_index",
    "tot_doc", "tot_top_100_doc", "citations_top_100", "fraction_%",
    "wiki_avg_views_30d",
    "patent_count_epo",
    "rsd_score",
    "news_count",
    "overton_policy_docs",
    "rcr_median",
]

# Mapping from internal pipeline names → final output column names
COLUMN_RENAME = {
    "tot_doc":            "tot_docs",
    "tot_top_100_doc":    "top_docs",
    "citations_top_100":  "top_jrnl_cit",
    "fraction_%":         "%_top_jrnl_cit",
    "wiki_avg_views_30d": "wiki",
    "patent_count_epo":   "epo",
    "rsd_score":          "rsd",
    "news_count":         "news",
}


def _save(df: pd.DataFrame) -> None:
    df.to_csv(WORKING_CSV, sep=",", index=False, encoding="utf-8")


def _load() -> pd.DataFrame:
    return pd.read_csv(WORKING_CSV, sep=",", encoding="utf-8", low_memory=False)


def _banner(msg: str) -> None:
    log.info(f"\n{'═'*70}")
    log.info(f"  {msg}")
    log.info(f"{'═'*70}")


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED HTTP HELPER
# ══════════════════════════════════════════════════════════════════════════════

_SESSION = requests.Session()

def _get(url, params=None, headers=None, timeout=20, max_retries=3, retry_wait=8):
    for attempt in range(max_retries):
        try:
            resp = _SESSION.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code in (429,) or resp.status_code >= 500:
                w = retry_wait * (attempt + 1)
                log.info(f"    [HTTP {resp.status_code}] retry in {w}s …")
                time.sleep(w); continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            w = retry_wait * (attempt + 1)
            log.info(f"    [Req error] {e}. Retry in {w}s …"); time.sleep(w)
    return None



# ══════════════════════════════════════════════════════════════════════════════
#  STEP 1 — SAMPLE SCIENTISTS FROM STANFORD DATABASE
# ══════════════════════════════════════════════════════════════════════════════

COMMON_SURNAMES = {
    "wang","li","zhang","chen","liu","yang","huang","wu","zhao","zhou","xu","ma",
    "lu","zhu","sun","yu","lin","he","hu","jiang","guo","luo","gao","zheng","tang",
    "liang","wei","shi","song","xie","han","tan","deng","bai","yan","feng","cao",
    "cai","xiao","pan","cheng","yuan","su","fan","dong","ye","tian","fu","du","jin",
    "xia","fang","peng","zeng","long","nie","ceng","qian","mao","shao","kong","dai",
    "jia","ding","yi","yao","shen","ren","yin","min",
    "lee","chang","chung","park","lim","kwon","cho","jung","yoon","oh","shin","ahn",
    "kim","choi","ko",
    "nguyen","tran","le","pham","hoang","huynh","phan","vu","vo","dang","bui","do",
    "ngo","duong","ly",
    "sato","suzuki","takahashi","tanaka","watanabe","ito","yamamoto","nakamura",
    "kobayashi","kato","yoshida","yamada","sasaki","yamaguchi","matsumoto","inoue",
    "kimura","hayashi","shimizu","yamazaki","mori","abe","ikeda","hashimoto",
    "yamashita","ishikawa","nakajima","ogawa","maeda","okamoto","fujita",
    "singh","kumar","devi","das","yadav","kaur","kumari","sharma","ram","patel",
    "gupta","joshi","mehta","mishra","pandey","chauhan","thakur","verma","nair",
    "pillai","reddy","rao","iyer","menon","bose","ghosh","banerjee","chakraborty",
    "mukherjee","chatterjee","roy","sen","choudhury","khan","maung",
    "ali","ahmed","mohamed","mohammad","hussain","hassan","ibrahim","rahman",
    "akhtar","akter","khatun","hossain","islam","chowdhury","alhassan","alali",
    "mohammadi","hosseini","ahmadi","moradi","rezaei","karimi","mousavi","rahimi",
    "sadeghi","najafi","bagheri","esmaili","heidari","ebrahimi","nazari","khani",
    "ghorbani","shahbazi","rostami","jafari","abbasi","mirzaei","taheri",
    "garcia","hernandez","rodriguez","lopez","gonzalez","martinez","perez","sanchez",
    "da silva","dos santos","silva","santos","oliveira","souza","sousa","ferreira",
    "alves","lima","pereira","carvalho","costa","fernandez","gomez","diaz","torres",
    "ramirez","flores","vargas","reyes","moreno","ruiz","jimenez","morales","castro",
    "romero","gutierrez","ortiz","chavez","ramos","herrera","medina","aguilar",
    "suarez","mendez","vega","guerrero","castillo","nunez",
    "smith","jones","brown","johnson","williams","taylor","davies","evans","wilson",
    "thomas","roberts","robinson","thompson","white","walker","wright","jackson",
    "green","harris","king","martin","clark","lewis","hall","young","allen","scott",
    "moore","hill","turner","campbell","anderson","mitchell","carter","phillips",
    "edwards","collins","stewart","morris","rogers","ward","cook","morgan","bailey",
    "cooper","bell","shaw","miller","davis","baker","adams","nelson","price","james",
    "parker","wood",
    "muller","müller","schmidt","schneider","fischer","meyer","maier","weber","wagner",
    "becker","schulz","schulze","hoffmann","schafer","schäfer","bauer","richter",
    "klein","wolf","schroder","schröder","neumann","schwarz","zimmermann","braun",
    "kruger","krüger","hofmann","hartmann","lange","lehmann","schubert","frank",
    "walter","kaiser","fuchs","krause","peters","jung",
    "bernard","petit","robert","richard","durand","dubois","moreau","simon","laurent",
    "lefebvre","michel","david","bertrand","roux","vincent","fournier","morel",
    "ferrari","russo","esposito","bianchi","romano","colombo","ricci","marino",
    "greco","bruno","gallo","conti","de luca","giordano","mancini","rizzo",
    "lombardi","moretti","barbieri","fontana","santoro","marini","farina","vitale",
    "pellegrini","caruso","palumbo",
    "ivanov","smirnov","kuznetsov","popov","vasiliev","petrov","sokolov","mikhailov",
    "fedorov","morozov","volkov","alekseyev","lebedev","semyonov","egorov","pavlov",
    "kozlov","stepanov","kowalski","nowak",
    "hansen","nielsen","jensen","andersen","pedersen","christensen","larsen",
    "sorensen","rasmussen","johansson","eriksson","nilsson","karlsson","persson",
    "svensson","lindgren","lindberg",
    "yilmaz","kaya","demir","celik","sahin","yildiz","ozturk","aydin","arslan","dogan",
    "diallo","traore","coulibaly","diop","ba","ndiaye","okonkwo","adeyemi","okafor","eze",
    "ma","he","le","du","fu","su","wu","yu","lu","li","xu",
}


def _extract_surname(authfull):
    if not isinstance(authfull, str): return ""
    return authfull.split(",", 1)[0].strip().lower()


def _is_common(surname):
    if not surname: return True
    if surname in COMMON_SURNAMES: return True
    return any(t in COMMON_SURNAMES for t in re.split(r"[\s\-]+", surname) if len(t) > 1)


def step1_sample():
    _banner(f"STEP 1 — Sampling {N_SAMPLE} scientists: '{SM_FIELD}'")
    if WORKING_CSV.exists():
        df = _load()
        log.info(f"  CSV already exists (n={len(df)}), skipping sampling.")
        return

    log.info(f"  Reading Stanford xlsx …")
    df_full = pd.read_excel(STANFORD_XLSX, sheet_name="Data", engine="openpyxl")
    log.info(f"  Total rows: {len(df_full):,}")

    # Filter field — try both column names used across Stanford versions
    for col in ["sm-field", "fsm-field"]:
        if col in df_full.columns:
            mask = df_full[col].str.strip().str.lower() == SM_FIELD.lower()
            df_field = df_full[mask].copy()
            log.info(f"  '{SM_FIELD}' rows: {len(df_field):,}")
            break
    else:
        raise ValueError(f"Could not find field column in Stanford xlsx. "
                         f"Columns: {list(df_full.columns)}")

    # Surname filter
    df_field["_surname"] = df_field["authfull"].apply(_extract_surname)
    df_field["_common"]  = df_field["_surname"].apply(_is_common)
    df_eligible = df_field[~df_field["_common"]].copy().reset_index(drop=True)
    log.info(f"  After surname filter: {len(df_eligible):,}")

    if len(df_eligible) < N_SAMPLE:
        raise ValueError(f"Only {len(df_eligible)} eligible rows; need {N_SAMPLE}.")

    # Stratified uniform sample
    df_eligible["_stratum"] = pd.cut(df_eligible.index, bins=N_SAMPLE,
                                      labels=False, include_lowest=True)
    rng = np.random.default_rng(RANDOM_SEED)
    sampled = (df_eligible
               .groupby("_stratum", group_keys=False)
               .apply(lambda g: g.sample(n=1, random_state=int(rng.integers(0, 1_000_000)))))
    sampled = sampled.drop(columns=[c for c in sampled.columns if c.startswith("_")])

    # Split authfull → Surname, Name
    def _split(authfull):
        if not isinstance(authfull, str) or not authfull.strip():
            return pd.Series({"Surname": "", "Name": ""})
        parts = authfull.split(",", 1)
        return pd.Series({"Surname": parts[0].strip(), "Name": parts[1].strip() if len(parts)>1 else ""})

    sampled[["Surname","Name"]] = sampled["authfull"].apply(_split)
    cols = list(sampled.columns)
    for c in ["Surname","Name"]:
        cols.remove(c)
    ai = cols.index("authfull")
    cols.insert(ai+1, "Surname"); cols.insert(ai+2, "Name")
    sampled = sampled[cols]

    _save(sampled)
    log.info(f"  Saved {len(sampled)} rows → {WORKING_CSV}")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 2 — SCOPUS ID + H-INDEX
# ══════════════════════════════════════════════════════════════════════════════

_SCOPUS_HEADERS_1 = {"X-ELS-APIKey": SCOPUS_API_KEY, "Accept": "application/json"}


def _get_scopus_id(surname, name, affil=None):
    first = name.split()[0] if name else ""
    for query in [
        f'AUTHLASTNAME("{surname}") AND AUTHFIRST("{first}")'
        + (f' AND AFFIL("{affil}")' if affil else ""),
        f'AUTHLASTNAME("{surname}") AND AUTHFIRST("{first}")',
        f'AUTHLASTNAME("{surname}")',
    ]:
        data = _get("https://api.elsevier.com/content/search/author",
                    params={"query": query, "count": 1},
                    headers=_SCOPUS_HEADERS_1)
        if not data: continue
        entries = data.get("search-results", {}).get("entry", [])
        if not entries: continue
        raw = entries[0].get("dc:identifier", "")
        if raw.startswith("AUTHOR_ID:"):
            return raw.split(":")[1]
    return None


def _get_h_index(scopus_id):
    data = _get(f"https://api.elsevier.com/content/author/author_id/{scopus_id}",
                params={"view": "ENHANCED"}, headers=_SCOPUS_HEADERS_1)
    if not data: return None
    try:
        h = data["author-retrieval-response"][0].get("h-index")
        return int(h) if h is not None else None
    except Exception:
        return None


def _clean_id(raw):
    if pd.isna(raw) or raw in ("", None): return None
    try: return str(int(float(str(raw))))
    except: return None


def step2_hindex():
    _banner("STEP 2 — Scopus ID + h-index")
    df = _load()
    for col in ["Scopus_ID", "h_index"]:
        if col not in df.columns: df[col] = None
    n = len(df)
    for idx, row in df.iterrows():
        surname = str(row.get("Surname","")).strip()
        name    = str(row.get("Name","")).strip()
        tag     = f"[{idx+1}/{n}] {surname}, {name}"
        sid     = _clean_id(row.get("Scopus_ID"))
        if not sid:
            affil = str(row.get("inst_name","")).strip() or None
            sid   = _get_scopus_id(surname, name, affil)
            df.at[idx, "Scopus_ID"] = sid
            log.info(f"{tag}  Scopus_ID={sid}"); time.sleep(1.0)
        else:
            log.info(f"{tag}  Scopus_ID already: {sid}")
        h_missing = pd.isna(row.get("h_index")) or row.get("h_index") in ("",None)
        if sid and h_missing:
            h = _get_h_index(sid)
            df.at[idx, "h_index"] = h
            log.info(f"    h-index={h}"); time.sleep(0.5)
        _save(df)
    log.info(f"  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 3 — JOURNAL-BASED METRICS
#  (tot_doc, tot_top_100_doc, citations_top_100, fraction_%)
# ══════════════════════════════════════════════════════════════════════════════

_SCOPUS_HEADERS_2 = {"X-ELS-APIKey": SCOPUS_API_KEY, "Accept": "application/json"}
_SEARCH_URL = "https://api.elsevier.com/content/search/scopus"


def _load_top_journal_ids():
    try:
        scimago = pd.read_csv(SCIMAGO_CSV, sep=";")
        return {int(x) for x in scimago["Sourceid"].astype(str) if str(x).isdigit()}
    except Exception as e:
        log.info(f"  WARNING: Could not load SciMago file: {e}")
        return set()


def _fetch_author_docs(au_id, top_ids):
    total_cit = top_cit = tot_doc = top_doc = 0
    start = 0
    while True:
        params = {"query": f"AU-ID({au_id})", "count": 25, "start": start,
                  "field": "citedby-count,source-id"}
        data = _get(_SEARCH_URL, params=params, headers=_SCOPUS_HEADERS_2)
        if not data: break
        entries = data.get("search-results",{}).get("entry",[])
        if not entries: break
        for doc in entries:
            try:
                tot_doc += 1
                cites = int(doc.get("citedby-count","0"))
                total_cit += cites
                sid = int(doc.get("source-id",0))
                if sid in top_ids:
                    top_cit += cites; top_doc += 1
            except: continue
        if len(entries) < 25: break
        start += 25
        time.sleep(0.2)
    return total_cit, top_cit, tot_doc, top_doc


def step3_journals():
    _banner("STEP 3 — Journal-based metrics")
    df = _load()
    for col in ["total_citations","citations_top_100","tot_doc","tot_top_100_doc","fraction_%"]:
        if col not in df.columns: df[col] = None
    top_ids = _load_top_journal_ids()
    log.info(f"  Loaded {len(top_ids):,} top journal IDs from SciMago")
    n = len(df)
    for idx, row in df.iterrows():
        already = pd.notna(row.get("total_citations")) and str(row.get("total_citations")) not in ("","nan")
        if already:
            log.info(f"[{idx+1}/{n}] already populated, skipping")
            continue
        sid = _clean_id(row.get("Scopus_ID"))
        tag = f"[{idx+1}/{n}] {row.get('Surname','')} {row.get('Name','')}"
        if not sid:
            log.info(f"{tag}  no Scopus_ID, skipping")
            continue
        tc, topc, td, topd = _fetch_author_docs(sid, top_ids)
        frac = round(topc/tc*100, 2) if tc > 0 else 0.0
        df.at[idx,"total_citations"]  = tc
        df.at[idx,"citations_top_100"]= topc
        df.at[idx,"tot_doc"]          = td
        df.at[idx,"tot_top_100_doc"]  = topd
        df.at[idx,"fraction_%"]       = frac
        log.info(f"{tag}  total_cit={tc}  top_cit={topc}  tot_doc={td}  frac={frac}%")
        _save(df)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 4 — WIKIPEDIA PAGE-VIEWS
# ══════════════════════════════════════════════════════════════════════════════

_WIKI_HEADERS = {
    "User-Agent": "StanfordBibliometricBot/2.0 (Academic research)",
    "Accept": "application/json",
}

# Person-page signals expanded for all academic fields
_PERSON_SIGNALS = [
    "born","professor","researcher","scientist","academic","economist",
    "physician","doctor","engineer","psychologist","sociologist","historian",
    "philosopher","mathematician","biologist","chemist","physicist","lawyer",
    "politici","author","writer","diplomat","business","entrepren",
    "medicine","medical","cardiolog","oncolog","immunolog","neurolog",
]


def _wiki_opensearch(full_name):
    data = _get("https://en.wikipedia.org/w/api.php",
                params={"action":"opensearch","search":full_name,
                        "limit":5,"format":"json","redirects":"resolve"},
                headers=_WIKI_HEADERS)
    if not data or len(data)<4: return []
    return [{"title":t,"url":u} for t,u in zip(data[1],data[3])]


def _wiki_summary(title):
    return _get(f"https://en.wikipedia.org/api/rest_v1/page/summary/{title.replace(' ','_')}",
                headers=_WIKI_HEADERS)


def _wiki_pageviews(title, days=30):
    end   = datetime.date.today() - datetime.timedelta(days=1)
    start = end - datetime.timedelta(days=days)
    url   = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
             f"/en.wikipedia/all-access/user/{title.replace(' ','_')}/daily"
             f"/{start.strftime('%Y%m%d')}/{end.strftime('%Y%m%d')}")
    data = _get(url, headers=_WIKI_HEADERS)
    if not data or "items" not in data: return None
    views = [i["views"] for i in data["items"]]
    return round(sum(views)/len(views), 1) if views else None


def _query_wikipedia(surname, name, affil=""):
    full = f"{name} {surname}".strip()
    candidates = _wiki_opensearch(full); time.sleep(1.2)
    for cand in candidates:
        if surname.lower() not in cand["title"].lower(): continue
        summary = _wiki_summary(cand["title"]); time.sleep(1.2)
        if not summary: continue
        extract = (summary.get("extract") or "").lower()
        if not any(s in extract for s in _PERSON_SIGNALS): continue
        views = _wiki_pageviews(cand["title"]); time.sleep(1.2)
        return {"wiki_avg_views_30d":views}
    return {"wiki_avg_views_30d":None}


def step4_wikipedia():
    _banner("STEP 4 — Wikipedia page-views")
    df = _load()
    if "wiki_avg_views_30d" not in df.columns: df["wiki_avg_views_30d"] = None
    n = len(df)
    for idx, row in df.iterrows():
        already = pd.notna(row.get("wiki_avg_views_30d")) and str(row.get("wiki_avg_views_30d")) not in ("","nan")
        if already:
            log.info(f"[{idx+1}/{n}] wiki already populated, skipping")
            continue
        surname = str(row.get("Surname","")).strip()
        name    = str(row.get("Name","")).strip()
        if not surname: continue
        result = _query_wikipedia(surname, name, str(row.get("inst_name","")))
        for col, val in result.items():
            df.at[idx, col] = val
        log.info(f"[{idx+1}/{n}] {surname},{name}  wiki_views={result['wiki_avg_views_30d']}")
        _save(df)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 5 — PATENTS (EPO)
# ══════════════════════════════════════════════════════════════════════════════

from datetime import timedelta

class EPOTokenManager:
    _TTL = timedelta(minutes=18)
    def __init__(self, key, secret):
        self._key=key; self._secret=secret; self._token=None; self._issued=None
    def get(self):
        import datetime as _dt
        if (self._token is None or self._issued is None
                or _dt.datetime.now(_dt.timezone.utc) - self._issued >= self._TTL):
            self._refresh()
        return self._token
    def _refresh(self):
        import datetime as _dt
        creds = base64.b64encode(f"{self._key}:{self._secret}".encode()).decode()
        resp  = _SESSION.post(
            "https://ops.epo.org/3.2/auth/accesstoken",
            headers={"Authorization":f"Basic {creds}",
                     "Content-Type":"application/x-www-form-urlencoded"},
            data="grant_type=client_credentials", timeout=20)
        resp.raise_for_status()
        self._token  = resp.json()["access_token"]
        self._issued = _dt.datetime.now(_dt.timezone.utc)
        log.info("    [EPO] token refreshed.")


def _strip_accents(text):
    return "".join(c for c in unicodedata.normalize("NFD",text)
                   if unicodedata.category(c)!="Mn")


def _first_token(name):
    parts = name.strip().split()
    return parts[0] if parts else name


def _epo_query(query, token):
    resp = requests.get("https://ops.epo.org/3.2/rest-services/published-data/search",
                        headers={"Authorization":f"Bearer {token}","Accept":"application/xml"},
                        params={"q":query,"Range":"1-10"}, timeout=20)
    if resp is None or resp.status_code in (404,): return 0
    if resp.status_code != 200: return 0
    try:
        root = ET.fromstring(resp.content)
        for elem in root.iter():
            if "total-result-count" in elem.attrib:
                return int(elem.attrib["total-result-count"])
    except ET.ParseError:
        pass
    return 0


def _get_epo(surname, name, epo_mgr):
    s = _strip_accents(surname); f = _strip_accents(_first_token(name))
    best = 0
    for q in [f'in="{s} {f}"', f'in="{f} {s}"']:
        cnt = _epo_query(q, epo_mgr.get()); time.sleep(1.2)
        if cnt > best: best = cnt
    return best



def step5_patents():
    _banner("STEP 5 — Patents (EPO)")
    df = _load()
    if "patent_count_epo" not in df.columns: df["patent_count_epo"] = None
    epo_mgr = EPOTokenManager(EPO_CONSUMER_KEY, EPO_CONSUMER_SECRET)
    epo_mgr.get()   # eager first fetch
    n = len(df)
    for idx, row in df.iterrows():
        surname = str(row.get("Surname","")).strip()
        name    = str(row.get("Name","")).strip()
        tag     = f"[{idx+1}/{n}] {surname},{name}"
        epo_done = pd.notna(row.get("patent_count_epo")) and str(row.get("patent_count_epo")) not in ("","nan")
        if epo_done:
            log.info(f"{tag}  EPO already populated, skipping"); continue
        if not surname: continue
        cnt = _get_epo(surname, name, epo_mgr)
        df.at[idx,"patent_count_epo"] = cnt
        log.info(f"{tag}  EPO={cnt}")
        _save(df)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 6 — RAO-STIRLING DIVERSITY (RSD)
# ══════════════════════════════════════════════════════════════════════════════

_SCOPUS_RSD = {"X-ELS-APIKey": SCOPUS_API_KEY, "Accept": "application/json"}
_MAX_PUBS_RSD = 50   # 50 × 250 ≈ 12,500 API calls — within weekly quota


def _asjc_distance(c1, c2):
    if c1 == c2: return 0.0
    s1, s2 = str(c1), str(c2)
    if len(s1)>=2 and len(s2)>=2 and s1[:2]==s2[:2]: return 0.33
    if s1[0]==s2[0]: return 0.67
    return 1.0


def _calc_rsd(codes):
    if not codes: return 0.0
    counter = Counter(codes); total = len(codes)
    unique  = list(counter.keys())
    props   = {c: n/total for c,n in counter.items()}
    rsd = 0.0
    for c1,c2 in combinations_with_replacement(unique, 2):
        d = _asjc_distance(c1, c2)
        rsd += (d*props[c1]*props[c2] if c1==c2 else 2*d*props[c1]*props[c2])
    return rsd


def _get_eids(scopus_id, max_results=_MAX_PUBS_RSD):
    eids=[]; start=0; page=min(max_results,200)
    while len(eids)<max_results:
        fetch = min(page, max_results-len(eids))
        data  = _get("https://api.elsevier.com/content/search/scopus",
                     params={"query":f"AU-ID({scopus_id})","count":fetch,
                             "start":start,"sort":"-pubyear"},
                     headers=_SCOPUS_RSD)
        if not data: break
        entries = (data.get("search-results") or {}).get("entry",[])
        if not entries: break
        for e in entries:
            if "eid" in e: eids.append(e["eid"])
        if len(entries)<fetch: break
        start += fetch
    return eids[:max_results]


def _get_asjc(eid):
    data = _get(f"https://api.elsevier.com/content/abstract/eid/{eid}",
                params={"view":"FULL"}, headers=_SCOPUS_RSD)
    if not data: return []
    try:
        areas = (data["abstracts-retrieval-response"]["subject-areas"]
                 .get("subject-area",[]))
        if not isinstance(areas, list): areas=[areas]
        return [int(a["@code"]) for a in areas if "@code" in a]
    except: return []


def step6_rsd():
    _banner("STEP 6 — Rao-Stirling Diversity (RSD)")
    df = _load()
    if "rsd_score" not in df.columns: df["rsd_score"] = None
    n = len(df)
    for idx, row in df.iterrows():
        already = (pd.notna(row.get("rsd_score")) and
                   str(row.get("rsd_score")) not in ("","nan"))
        if already:
            log.info(f"[{idx+1}/{n}] RSD already populated, skipping"); continue
        sid  = _clean_id(row.get("Scopus_ID"))
        tag  = f"[{idx+1}/{n}] {row.get('Surname','')} {row.get('Name','')}"
        if not sid:
            log.info(f"{tag}  no Scopus_ID")
            df.at[idx,"rsd_score"]=np.nan
            _save(df); continue
        eids = _get_eids(int(float(sid)))
        log.info(f"{tag}  {len(eids)} pubs")
        all_asjc=[]
        for i,eid in enumerate(eids):
            codes = _get_asjc(eid); all_asjc.extend(codes)
            if i>0 and i%10==0: print(f"    {i}/{len(eids)} done …")
            time.sleep(0.35)
        rsd = _calc_rsd(all_asjc)
        df.at[idx,"rsd_score"] = round(rsd,6)
        log.info(f"    RSD={rsd:.4f}")
        _save(df)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 7 — GOOGLE NEWS MENTIONS
# ══════════════════════════════════════════════════════════════════════════════

import urllib.parse
try:
    import feedparser as _feedparser
    _HAS_FEEDPARSER = True
except ImportError:
    _HAS_FEEDPARSER = False
    log.info("  WARNING: feedparser not installed. Run: pip install feedparser")


def _get_news_count(name, affil=""):
    if not _HAS_FEEDPARSER: return 0
    if affil and str(affil).strip().lower() not in ("","nan"):
        query = f'"{name}" {affil.strip()}'
    else:
        query = f'"{name}"'
    enc = urllib.parse.quote_plus(query)
    url = f"https://news.google.com/rss/search?q={enc}&num=100&hl=en"
    for attempt in range(4):
        try:
            feed = _feedparser.parse(url)
            if feed.bozo and not feed.entries:
                exc = getattr(feed,"bozo_exception",None)
                if exc: raise Exception(str(exc))
            return len(feed.entries)
        except Exception as e:
            log.info(f"    [News] attempt {attempt+1}: {e}")
            if attempt<3: time.sleep(8)
    return 0


def step7_news():
    _banner("STEP 7 — Google News mentions")
    df = _load()
    if "news_count" not in df.columns: df["news_count"] = None
    n = len(df)
    for idx, row in df.iterrows():
        already = (pd.notna(row.get("news_count")) and
                   str(row.get("news_count")) not in ("","nan"))
        if already:
            log.info(f"[{idx+1}/{n}] news already populated, skipping"); continue
        name  = f"{str(row.get('Name','')).strip()} {str(row.get('Surname','')).strip()}".strip()
        affil = str(row.get("inst_name","")).strip()
        cnt   = _get_news_count(name, affil)
        df.at[idx,"news_count"] = cnt
        log.info(f"[{idx+1}/{n}] {name}  news={cnt}")
        _save(df); time.sleep(1.5)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 8 — OVERTON POLICY CITATIONS
# ══════════════════════════════════════════════════════════════════════════════

def _overton_tokenise(text):
    stop = {"university","institute","center","centre","hospital","college",
            "school","department","faculty","division","research","science",
            "sciences","medical","medicine","economics","business","finance"}
    return {t for t in re.split(r"[\s,\-/]+",text.lower())
            if len(t)>=4 and t not in stop}


def _affil_match(entry_affil, tokens):
    if not entry_affil or not tokens: return False
    return any(t in entry_affil.lower() for t in tokens)


def _query_overton(name, inst_name):
    params = {"query":name,"sort":"citations","format":"json","api_key":OVERTON_API_KEY}
    for attempt in range(3):
        try:
            resp = _SESSION.get("https://app.overton.io/people.php",
                                params=params, timeout=20)
            if resp.status_code==429: time.sleep(8*(attempt+1)); continue
            resp.raise_for_status()
            data = resp.json(); break
        except requests.RequestException as e:
            log.info(f"    [Overton] {e}"); time.sleep(8)
    else:
        return 0
    results = data.get("results",[])
    if not results:
        return 0
    tokens  = _overton_tokenise(inst_name) if inst_name else set()
    matched = [r for r in results if _affil_match(r.get("affiliation",""), tokens)]
    if matched:
        return sum(r.get("linked_policy_document_count",0) or 0 for r in matched)
    else:
        return results[0].get("linked_policy_document_count",0) or 0


def step8_overton():
    _banner("STEP 8 — Overton policy citations")
    df = _load()
    if "overton_policy_docs" not in df.columns: df["overton_policy_docs"] = None
    n = len(df)
    for idx, row in df.iterrows():
        already = (pd.notna(row.get("overton_policy_docs")) and
                   str(row.get("overton_policy_docs")) not in ("","nan"))
        if already:
            log.info(f"[{idx+1}/{n}] Overton already populated, skipping"); continue
        name = f"{str(row.get('Name','')).strip()} {str(row.get('Surname','')).strip()}".strip()
        inst = str(row.get("inst_name","")).strip()
        if not name: continue
        docs = _query_overton(name, inst)
        df.at[idx,"overton_policy_docs"] = docs
        log.info(f"[{idx+1}/{n}] {name}  policy={docs}")
        _save(df); time.sleep(1.1)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 9 — RELATIVE CITATION RATIO (RCR)
# ══════════════════════════════════════════════════════════════════════════════

_PUBMED_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
_ICITE_URL  = "https://icite.od.nih.gov/api/pubs"
_ICITE_FL   = "pmid,relative_citation_ratio,provisional,is_research_article"
_PAUSE_NCBI = 1.1   # 3 req/s without NCBI API key
_MAX_PMIDS  = 100


def _get_pmids(surname, name, inst_name):
    initial = name[0].upper() if name else ""
    author_term = f'"{surname} {initial}"[au]'
    stop_w = {"university","institute","center","centre","hospital","college",
               "medical","health","school","economics","business","finance"}
    affil_tokens = [t for t in re.split(r"[\s,\-/]+",inst_name.lower())
                    if len(t)>=5 and t not in stop_w][:2] if inst_name else []
    query = (f'{author_term} AND "{" ".join(affil_tokens)}"[ad]'
             if affil_tokens else author_term)
    params = {"db":"pubmed","term":query,"retmax":_MAX_PMIDS,
              "retmode":"json","sort":"pub date","usehistory":"n"}
    data = _get(_PUBMED_URL, params=params); time.sleep(_PAUSE_NCBI)
    pmids = (data or {}).get("esearchresult",{}).get("idlist",[])
    if not pmids and affil_tokens:
        params["term"] = author_term
        data2 = _get(_PUBMED_URL, params=params); time.sleep(_PAUSE_NCBI)
        pmids = (data2 or {}).get("esearchresult",{}).get("idlist",[])
    return pmids


def _get_icite(pmids):
    pubs = []
    for i in range(0, len(pmids), 200):
        chunk = pmids[i:i+200]
        data = _get(_ICITE_URL, params={"pmids":",".join(chunk),"fl":_ICITE_FL})
        time.sleep(0.5)
        if data: pubs.extend(data.get("data",[]))
    return pubs


def _aggregate_rcr(pubs):
    vals = []
    for p in pubs:
        if p.get("is_research_article") not in (True,"Yes",1): continue
        if p.get("provisional")         in  (True,"Yes",1): continue
        rcr = p.get("relative_citation_ratio")
        if rcr is not None and rcr > 0: vals.append(float(rcr))
    if not vals:
        return {"rcr_median": np.nan}
    return {"rcr_median": round(float(np.median(vals)), 4)}


def step9_rcr():
    _banner("STEP 9 — Relative Citation Ratio (RCR median)")
    df = _load()
    if "rcr_median" not in df.columns: df["rcr_median"] = None
    n = len(df)
    for idx, row in df.iterrows():
        already = (pd.notna(row.get("rcr_median")) and
                   str(row.get("rcr_median")) not in ("","nan"))
        if already:
            log.info(f"[{idx+1}/{n}] RCR already populated, skipping"); continue
        surname = str(row.get("Surname","")).strip()
        name    = str(row.get("Name","")).strip()
        inst    = str(row.get("inst_name","")).strip()
        tag     = f"[{idx+1}/{n}] {surname},{name}"
        if not surname:
            df.at[idx,"rcr_median"] = np.nan
            _save(df); continue
        pmids = _get_pmids(surname, name, inst)
        log.info(f"{tag}  PubMed={len(pmids)} PMIDs")
        if not pmids:
            df.at[idx,"rcr_median"] = np.nan
            _save(df); continue
        pubs   = _get_icite(pmids)
        result = _aggregate_rcr(pubs)
        df.at[idx,"rcr_median"] = result["rcr_median"]
        log.info(f"    rcr_median={result['rcr_median']}")
        _save(df)
    log.info("  Done.")


# ══════════════════════════════════════════════════════════════════════════════
#  STEP 10 — EXPORT FINAL XLSX
# ══════════════════════════════════════════════════════════════════════════════

def step10_export():
    _banner("STEP 10 — Exporting final xlsx")
    df = _load()

    # Keep only final columns that actually exist in the dataframe
    keep = [c for c in FINAL_COLUMNS if c in df.columns]
    missing = [c for c in FINAL_COLUMNS if c not in df.columns]
    if missing:
        log.info(f"  WARNING: these columns are missing and will be absent: {missing}")

    df_out = df[keep].copy()

    # Rename internal pipeline names to final output names
    df_out = df_out.rename(columns={k: v for k, v in COLUMN_RENAME.items() if k in df_out.columns})

    # Trim whitespace on string columns
    for col in ["Surname","Name","inst_name","cntry"]:
        if col in df_out.columns:
            df_out[col] = df_out[col].astype(str).str.strip()

    # Write xlsx with basic formatting
    with pd.ExcelWriter(FINAL_XLSX, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Data")
        ws = writer.sheets["Data"]

        from openpyxl.styles import Font, PatternFill, Alignment
        header_font = Font(name="Arial", bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="1F3864")
        for cell in ws[1]:
            cell.font      = header_font
            cell.fill      = header_fill
            cell.alignment = Alignment(horizontal="center", vertical="center",
                                       wrap_text=True)
        ws.row_dimensions[1].height = 36
        ws.freeze_panes = "A2"
        for col_cells in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col_cells), default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len*1.1,10),35)

    log.info(f"\n  ✓ Final xlsx saved → {FINAL_XLSX}")
    log.info(f"  Rows: {len(df_out)}  |  Columns: {len(keep)}")

    # Quick descriptive summary
    log.info("\n  ── Data completeness ──")
    for col in keep:
        n_valid = df_out[col].notna().sum()
        n_nz    = (df_out[col] > 0).sum() if df_out[col].dtype in [float,int] else "—"
        log.info(f"    {col:<35} valid={n_valid}/{len(df_out)}  non-zero={n_nz}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{'█'*70}")
    log.info(f"  Stanford Bibliometric Pipeline")
    log.info(f"  Field      : {SM_FIELD}")
    log.info(f"  Output dir : {FIELD_DIR}")
    log.info(f"  Working CSV: {WORKING_CSV}")
    log.info(f"  Final xlsx : {FINAL_XLSX}")
    print(f"{'█'*70}")

    FIELD_DIR.mkdir(parents=True, exist_ok=True)
    _attach_file_log(FIELD_DIR / 'pipeline.log')
    log.info(f'Starting pipeline for field: {SM_FIELD}')
    log.info(f'Output dir: {FIELD_DIR}')
    if RUN_STEP_1_SAMPLE:   step1_sample()
    if RUN_STEP_2_HINDEX:   step2_hindex()
    if RUN_STEP_3_JOURNALS: step3_journals()
    if RUN_STEP_4_WIKI:     step4_wikipedia()
    if RUN_STEP_5_PATENTS:  step5_patents()
    if RUN_STEP_6_RSD:      step6_rsd()
    if RUN_STEP_7_NEWS:     step7_news()
    if RUN_STEP_8_OVERTON:  step8_overton()
    if RUN_STEP_9_RCR:      step9_rcr()

    step10_export()   # always runs — just formats whatever is in the CSV

    print(f"\n{'█'*70}")
    log.info(f"  PIPELINE COMPLETE — {SM_FIELD}")
    log.info(f"  → {FINAL_XLSX}")
    print(f"{'█'*70}\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as _e:
        log.critical(f"PIPELINE CRASHED: {_e}")
        log.critical(_traceback.format_exc())
        sys.exit(1)
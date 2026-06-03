"""
gdelt_fetcher.py
================
Fetches GDELT data daily, cleans it, classifies by title (Political /
Security / Economic / Military / Energy / Other), and saves to Supabase.

Requirements:
    pip install requests pandas supabase

Environment variables (set as GitHub Secrets):
    SUPABASE_URL  — https://xxxx.supabase.co
    SUPABASE_KEY  — your anon/public key
"""

import os, re, io, zipfile, logging, requests, pandas as pd
from datetime import datetime, timezone
from supabase import create_client

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gdelt")

# ── Supabase client ───────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── GDELT column names ────────────────────────────────────────────────────
GDELT_COLS = [
    "GlobalEventID","Day","MonthYear","Year","FractionDate",
    "Actor1Code","Actor1Name","Actor1CountryCode","Actor1KnownGroupCode",
    "Actor1EthnicCode","Actor1Religion1Code","Actor1Religion2Code",
    "Actor1Type1Code","Actor1Type2Code","Actor1Type3Code",
    "Actor2Code","Actor2Name","Actor2CountryCode","Actor2KnownGroupCode",
    "Actor2EthnicCode","Actor2Religion1Code","Actor2Religion2Code",
    "Actor2Type1Code","Actor2Type2Code","Actor2Type3Code",
    "IsRootEvent","EventCode","EventBaseCode","EventRootCode",
    "QuadClass","GoldsteinScale","NumMentions","NumSources",
    "NumArticles","AvgTone","Actor1Geo_Type","Actor1Geo_FullName",
    "Actor1Geo_CountryCode","Actor1Geo_ADM1Code","Actor1Geo_Lat",
    "Actor1Geo_Long","Actor1Geo_FeatureID","Actor2Geo_Type",
    "Actor2Geo_FullName","Actor2Geo_CountryCode","Actor2Geo_ADM1Code",
    "Actor2Geo_Lat","Actor2Geo_Long","Actor2Geo_FeatureID",
    "ActionGeo_Type","ActionGeo_FullName","ActionGeo_CountryCode",
    "ActionGeo_ADM1Code","ActionGeo_Lat","ActionGeo_Long",
    "ActionGeo_FeatureID","DATEADDED","SOURCEURL",
]

# ══════════════════════════════════════════════════════════════════════════
# TITLE-FIRST CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════
RULES = [
    ("Military", "Armed Conflict & Operations", [
        "airstrike","air strike","missile","drone strike","bomb","bombing",
        "troops","soldiers","military operation","ground operation","naval",
        "warship","fighter jet","armed forces","tank","artillery","shelling",
        "mortar","sniper","ambush","insurgent","rebel group","militia",
        "iswap","hamas military","hezbollah","killed in combat","died in battle",
        "war zone","front line","ceasefire","peacekeeping","nato forces",
        "us forces","russian forces","military chief","killed in strike",
        "idf","houthi attack","rocket fire","cross border attack",
    ]),
    ("Military", "Military Affairs & Defense", [
        "military base","defense ministry","pentagon","military drill",
        "military exercise","military pact","defense deal","weapons deal",
        "arms sale","fighter aircraft","submarine","warplane","military budget",
        "defense spending","military alliance","troop deployment","military aid",
        "arms embargo",
    ]),
    ("Security", "Terrorism & Extremism", [
        "terrorist","terrorism","extremist","jihadist","suicide bomb","car bomb",
        "isis","islamic state","al qaeda","boko haram","al shabaab","ttp",
        "hizbul","lashkar","jaish","militant attack","attack killed","bomb blast",
        "explosion kills","gunmen kill","mass shooting","kidnapping","hostage",
        "ransom","abduction","beheaded","execution video",
    ]),
    ("Security", "Crime & Law Enforcement", [
        "arrested","detained","sentenced","prison","jail","charged with",
        "indicted","convicted","murder","homicide","shooting","stabbing",
        "robbery","fraud","drug bust","narcotics","smuggling","trafficking",
        "cartel","gang","police operation","fbi","interpol","warrant",
        "fugitive","extradition","court ruling","verdict","pleaded guilty",
        "sex assault","child abuse",
    ]),
    ("Security", "Threats & Coercion", [
        "threatens","threatened","warning issued","ultimatum","sanctions threat",
        "nuclear threat","cyberattack","cyber attack","hacking","espionage",
        "spy","intelligence agency","covert operation","blackmail",
    ]),
    ("Energy", "Oil & Gas", [
        "oil price","crude oil","brent crude","wti crude","opec","opec+",
        "natural gas","lng","pipeline","oil field","oil production","oil exports",
        "gas exports","refinery","petroleum","barrel","oil minister",
        "oil sanctions","oil embargo","oil supply","gas supply","oil deal",
        "strait of hormuz","oil tanker","gas tanker",
    ]),
    ("Energy", "Power & Renewables", [
        "nuclear plant","nuclear reactor","power plant","electricity grid",
        "solar farm","wind farm","renewable energy","clean energy","green energy",
        "energy transition","power outage","energy crisis","energy minister",
        "energy deal","coal plant","hydropower","energy storage",
    ]),
    ("Economic", "Trade & Finance", [
        "trade deal","trade war","tariff","import duty","export ban","sanctions",
        "stock market","shares fell","shares rose","gdp","inflation",
        "interest rate","central bank","federal reserve","imf","world bank",
        "debt crisis","currency","exchange rate","investment","foreign investment",
        "ipo","merger","acquisition","bankruptcy","recession","economic growth",
        "trade deficit","trade surplus","supply chain",
    ]),
    ("Economic", "Business & Industry", [
        "company","corporation","ceo","earnings","profit","revenue","quarterly",
        "layoffs","job cuts","hiring","factory","manufacturing","production",
        "startup","pharmaceutical","agriculture","livestock","commodity",
        "wheat","corn","soybean","fertilizer","mining","copper","gold",
    ]),
    ("Economic", "Aid & Development", [
        "foreign aid","humanitarian aid","food aid","relief fund","donor",
        "development bank","loan","grant","debt relief","poverty","famine",
        "flood relief","earthquake aid","reconstruction","infrastructure project",
    ]),
    ("Political", "Diplomacy & International Relations", [
        "diplomatic","summit","bilateral","multilateral","treaty",
        "agreement signed","memorandum","mou","foreign minister","state visit",
        "ambassador","united nations","un resolution","security council",
        "g7","g20","negotiations","peace talks","ceasefire talks","mediation",
        "envoy","normalization","diplomatic ties","relations restored",
        "expelled diplomat",
    ]),
    ("Political", "Government & Domestic Politics", [
        "president","prime minister","parliament","congress","senate","cabinet",
        "election","vote","ballot","campaign","party","coalition","opposition",
        "legislation","law passed","bill signed","impeachment","resignation",
        "minister","governor","mayor","policy","reform","budget proposal",
        "government says","administration","white house","kremlin","downing street",
    ]),
    ("Political", "Conflict & Geopolitics", [
        "geopolitical","territorial","sovereignty","occupation","annexation",
        "disputed","tension between","standoff","confrontation","provocation",
        "crackdown","dissidents","political prisoner","opposition leader",
        "coup","regime","authoritarian",
    ]),
    ("Other", "Sports", [
        " nba "," nfl "," nhl "," mlb "," epl ","premier league",
        "champions league","world cup","super bowl","stanley cup",
        "world series","olympic","paralympic","soccer match","football game",
        "basketball game","cricket match","tennis tournament","golf tournament",
        "formula one","f1 race"," scored "," goal "," touchdown ",
        " wicket "," innings "," championship ","transfer fee",
    ]),
    ("Other", "Entertainment & Culture", [
        "box office","oscar ","grammy ","emmy ","golden globe","cannes",
        "netflix","streaming","music video","album release","concert tour",
        "celebrity","film festival","red carpet","movie review","tv show",
        "reality show","pop star","singer","actor ","actress ","rapper",
    ]),
    ("Other", "Lifestyle & Society", [
        "recipe ","restaurant ","food festival","fashion week","horoscope",
        "lottery winner","horse racing","poker","beauty pageant","wedding",
        "viral video","tiktok","instagram","influencer","animal rescue",
        "obituary ","died at age","passed away","funeral",
    ]),
    ("Other", "Science & Technology", [
        "nasa ","space mission","rocket launch","asteroid","planet discovered",
        "artificial intelligence","machine learning","chatgpt","openai","gemini",
        "smartphone","iphone","android","semiconductor","chip shortage",
        "scientific study","research finds","climate change","global warming",
        "species discovered","genome",
    ]),
]

CAMEO_FALLBACK = {
    1:("Political","Statements & Public Appeals"),
    2:("Political","Appeals & Mediation"),
    3:("Political","Cooperation & Agreements"),
    4:("Political","Diplomatic Consultations"),
    5:("Political","Diplomacy & Treaties"),
    6:("Economic","Aid & Material Cooperation"),
    7:("Economic","Trade & Business"),
    8:("Political","Negotiations & Concessions"),
    9:("Political","Investigations & Demands"),
    10:("Political","Domestic Politics"),
    11:("Political","Criticism & Opposition"),
    12:("Political","Rejection & Sanctions"),
    13:("Security","Threats & Coercion"),
    14:("Political","Protests & Demonstrations"),
    15:("Military","Military Affairs & Defense"),
    16:("Political","Breaking Relations"),
    17:("Security","Coercion & Confrontations"),
    18:("Security","Crime & Law Enforcement"),
    19:("Military","Armed Conflict & Operations"),
    20:("Military","Armed Conflict & Operations"),
}


def classify_by_title(title):
    t = " " + title.lower() + " "
    for cat, sub, kws in RULES:
        if any(k in t for k in kws):
            return cat, sub
    return None


def full_classify(title, event_root_code):
    result = classify_by_title(title)
    if result:
        return result
    try:
        code = int(event_root_code)
    except (TypeError, ValueError):
        code = 0
    return CAMEO_FALLBACK.get(code, ("Other", "Uncategorized"))


def extract_title(url):
    if not url or not isinstance(url, str):
        return ""
    path = url.rstrip("/").split("?")[0]
    for seg in reversed(path.split("/")):
        seg = re.sub(r"\.(html?|php|aspx?|shtml|cfm)$", "", seg, flags=re.I)
        if re.match(r"^[\d\-]+$", seg): continue
        if re.match(r"^[a-f0-9\-]{20,}$", seg): continue
        if re.match(r"^article[_\-]", seg, re.I): continue
        if len(seg) < 8: continue
        t = seg.replace("-", " ").replace("_", " ")
        t = re.sub(r"\s+", " ", t).strip()
        return t[0].upper() + t[1:] if t else ""
    return ""


def classify_source(domain):
    if not domain: return "Online News"
    d = domain.lower()
    if any(x in d for x in ["reuters","apnews","bloomberg","xinhua","tass","prnewswire","businesswire","globenewswire"]): return "Wire Service"
    if any(x in d for x in ["aljazeera","foxnews","nbcnews","cbsnews","cnn.","msnbc","skynews","france24","dw.com","bbc.","abc.net"]): return "TV Network"
    if any(x in d for x in ["iheart","npr.org"]) or (d.endswith("fm.com") and len(d)<15): return "Radio"
    if any(x in d for x in [".gov",".mil","un.org","who.int","state.gov","europa.eu"]): return "Government"
    if any(x in d for x in [".edu",".ac.uk","university","research","pubmed","arxiv"]): return "Academic"
    if any(x in d for x in ["wsj.com","ft.com","cnbc.com","marketwatch","moneycontrol"]): return "Financial"
    if any(x in d for x in ["theguardian","nytimes","washingtonpost","independent.co.uk","telegraph","dailymail","thehindu","arabnews","gulfnews","punchng"]): return "Major Newspaper"
    if any(x in d for x in ["tribune","times","post","news","daily","herald","gazette","journal","chronicle","observer","dispatch","courier","star","mirror"]): return "Local / Regional News"
    return "Online News"


def fetch_latest_url():
    master = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    resp = requests.get(master, timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and "export.CSV" in parts[2]:
            return parts[2]
    raise ValueError("export.CSV URL not found in lastupdate.txt")


def download_and_parse(url):
    log.info(f"Downloading: {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        csv_name = [n for n in z.namelist() if n.upper().endswith(".CSV")][0]
        with z.open(csv_name) as f:
            df = pd.read_csv(f, sep="\t", names=GDELT_COLS, on_bad_lines="skip", low_memory=False)
    log.info(f"Raw rows: {len(df):,}")
    return df


def process(df, event_date):
    drop_cols = [c for c in df.columns if df[c].isnull().mean() > 0.90]
    df.drop(columns=drop_cols, inplace=True)
    df.drop_duplicates(subset=["SOURCEURL","EventRootCode"], inplace=True)

    df["domain"]      = df["SOURCEURL"].str.extract(r"https?://(?:www\.)?([^/:]+)")
    df["title"]       = df["SOURCEURL"].apply(extract_title)
    df["source_type"] = df["domain"].apply(classify_source)
    df["sentiment"] = pd.to_numeric(df["AvgTone"], errors="coerce").fillna(0).apply(
    lambda t: "Positive" if t > 2 else ("Negative" if t < -2 else "Neutral")
    df["event_date"] = event_date

    classified = df.apply(
        lambda r: pd.Series(full_classify(str(r.get("title","")), r.get("EventRootCode"))),
        axis=1,
    )
    df["category"]     = classified[0]
    df["sub_category"] = classified[1]

    keep = ["event_date","title","Actor1Name","Actor2Name","category","sub_category",
            "sentiment","GoldsteinScale","AvgTone","ActionGeo_CountryCode",
            "ActionGeo_FullName","NumMentions","domain","source_type","SOURCEURL"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    df.rename(columns={
        "Actor1Name":"actor1","Actor2Name":"actor2",
        "GoldsteinScale":"goldstein","AvgTone":"avg_tone",
        "ActionGeo_CountryCode":"country_code","ActionGeo_FullName":"geo_full_name",
        "NumMentions":"mentions","SOURCEURL":"url",
    }, inplace=True)

    df.fillna("", inplace=True)
    df["goldstein"] = pd.to_numeric(df["goldstein"], errors="coerce").fillna(0.0)
    df["avg_tone"]  = pd.to_numeric(df["avg_tone"],  errors="coerce").fillna(0.0)
    df["mentions"]  = pd.to_numeric(df["mentions"],  errors="coerce").fillna(0).astype(int)
    log.info(f"Clean rows: {len(df):,}")
    return df


def save_to_supabase(df):
    records = df.to_dict("records")
    BATCH = 500
    saved = 0
    for i in range(0, len(records), BATCH):
        supabase.table("gdelt_events").upsert(records[i:i+BATCH]).execute()
        saved += BATCH
        log.info(f"  Saved {min(saved,len(records)):,} / {len(records):,}")
    log.info(f"Done — {len(records):,} rows upserted")


def run():
    log.info("=" * 60)
    log.info(f"Run started: {datetime.now(timezone.utc).isoformat()}")
    try:
        url        = fetch_latest_url()
        raw        = download_and_parse(url)
        event_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        clean      = process(raw, event_date)
        save_to_supabase(clean)
        log.info("Pipeline completed successfully")
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run()

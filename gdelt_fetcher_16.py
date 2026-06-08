"""
gdelt_fetcher.py — v9
=====================
Fetches GDELT v1 daily file, cleans, classifies by title,
picks best source per event, saves to Supabase.
"""

import os, re, io, zipfile, logging, requests, pandas as pd
from datetime import datetime, timezone, timedelta
from supabase import create_client

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("gdelt")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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
# SOURCE PRIORITY — for dedup: keep best source per event
# ══════════════════════════════════════════════════════════════════════════
SOURCE_PRIORITY = {
    "Wire Service": 1,
    "Major Newspaper": 2,
    "TV Network": 3,
    "Financial": 4,
    "News Magazine": 5,
    "Government": 6,
    "Local / Regional News": 7,
    "Online News": 8,
    "Radio": 9,
    "Academic": 10,
    "Unknown": 11,
}

# ══════════════════════════════════════════════════════════════════════════
# TITLE-FIRST CLASSIFIER
# ══════════════════════════════════════════════════════════════════════════
RULES = [
    # ── MILITARY ─────────────────────────────────────────────────────────
    ("Military", "Armed Conflict & Operations", [
        "airstrike","air strike","missile strike","drone strike","bomb","bombing",
        "troops","soldiers","military operation","ground operation","naval battle",
        "warship","fighter jet","armed forces","tank","artillery","shelling",
        "mortar","sniper","ambush","insurgent","rebel group","militia",
        "iswap","hamas military wing","hezbollah","killed in combat","died in battle",
        "war zone","front line","ceasefire","peacekeeping","nato forces",
        "us forces","russian forces","military chief","killed in strike",
        "idf","houthi attack","rocket fire","cross border attack",
        "strikes","military strikes","us strikes","air strikes","strikes on",
        "bombed","destroyed by","targeted by",
        "shot down","intercept drone","drones launched","drone attack","launched toward","shot down drone",
        "occupied territory","siege","blockade","naval blockade",
        "ballistic missile","hypersonic missile","cruise missile",
        "air defense system","iron dome","patriot missile","s-400",
        "combat drone","armed drone","suicide drone","kamikaze drone",
        "special forces","commando","seal team",
        "ground invasion","amphibious assault",
        "war crimes","civilian casualties","collateral damage",
        "ethnic cleansing","genocide","massacre",
        "military coup","coup attempt","junta",
        "arms trafficking","weapons cache","weapons seized",
        "explosive device","ied","roadside bomb","mine explosion","landmine",
        "carrier strike group","aircraft carrier","fighter jet scrambled",
        "no fly zone","military withdrawal","troop pullout",
        "wagner group","private military","mercenary","foreign fighter",
        "irgc","revolutionary guard",
        "hezbollah military","hamas military","houthi military",
        "resistance group","armed faction",
    ]),
    ("Military", "Military Affairs & Defense", [
        "military base","defense ministry","pentagon","military drill",
        "military exercise","military pact","defense deal","weapons deal",
        "arms sale","fighter aircraft","submarine","warplane","military budget",
        "defense spending","military alliance","troop deployment","military aid",
        "arms embargo","force posture","military presence","military buildup",
        "nato exercise","un peacekeepers","peacekeeping mission",
    ]),

    # ── SECURITY ─────────────────────────────────────────────────────────
    ("Security", "Terrorism & Extremism", [
        "terrorist","terrorism","extremist","jihadist","suicide bomb","car bomb",
        "isis","islamic state","al qaeda","boko haram","al shabaab","ttp",
        "hizbul","lashkar","jaish","militant attack","attack killed","bomb blast",
        "explosion kills","gunmen kill","mass shooting","kidnapping","hostage",
        "ransom","abduction","beheaded","execution video",
        "terror plot","terror cell","terror network",
        "lone wolf","radicalized","self-radicalized",
        "deradicalization","counter terrorism",
        "mosque attack","church attack","school shooting",
        "market bombing","hotel attack","airport attack",
        "stabbing attack","knife attack","vehicle attack",
    ]),
    ("Security", "Crime & Law Enforcement", [
        "arrested","detained","sentenced","prison","jail","charged with",
        "indicted","convicted","murder","homicide","shooting","stabbing",
        "robbery","fraud","drug bust","narcotics","smuggling","trafficking",
        "cartel","gang","police operation","fbi","interpol","warrant",
        "fugitive","extradition","court ruling","verdict","pleaded guilty",
        "sex assault","child abuse",
        "money laundering","financial crime","corruption arrest","bribery",
        "assassination","targeted killing","political assassination",
        "kidnap for ransom","mass kidnapping",
        "human trafficking","migrant smuggling",
        "cybercrime","ransomware attack","data breach","identity theft",
        "organized crime","crime syndicate","drug cartel","narco",
        "gang warfare","gang shooting","vigilante","mob violence",
        "riot","civil unrest","looting","prison break",
    ]),
    ("Security", "Threats & Coercion", [
        "threatens","threatened","warning issued","ultimatum","sanctions threat",
        "nuclear threat","cyberattack","cyber attack","hacking","espionage",
        "spy","intelligence agency","covert operation","blackmail",
        "bomb threat","chemical threat","biological threat",
        "dirty bomb","radiological","chemical attack","chemical weapon",
        "biological attack","bioterrorism",
        "intelligence operation","spy network","double agent","defector",
        "proxy war","sectarian violence","sectarian conflict",
        "militia attack","paramilitary","border crossing attack",
    ]),

    # ── ENERGY ───────────────────────────────────────────────────────────
    ("Energy", "Oil & Gas", [
        "oil price","crude oil","brent crude","wti crude","opec","opec+",
        "natural gas","lng","pipeline","oil field","oil production","oil exports",
        "gas exports","refinery","petroleum","barrel","oil minister",
        "oil sanctions","oil embargo","oil supply","gas supply","oil deal",
        "oil tanker","gas tanker","tanker seized","hormuz oil","hormuz shipping",
        "aramco","saudi aramco","adnoc","qatar energy","qatarenergy",
        "sabic","petrochemical","gas field","oil well","oil reserve",
        "brent oil","crude futures","oil rally","oil slump",
        "fuel prices","gasoline prices","diesel prices",
        "liquefied natural gas","lng terminal","lng exports",
        "iraq oil","iran oil","kuwait oil","uae energy",
        "bahrain oil","oman oil","yemen oil","gulf energy","middle east oil",
        "oil tanker attack","refinery attack","oil pipeline attack",
        "opec meeting","opec decision","opec cut","opec output",
        "red sea shipping","suez canal energy",
    ]),
    ("Energy", "Power & Renewables", [
        "nuclear plant","nuclear reactor","power plant","electricity grid",
        "solar farm","wind farm","renewable energy","clean energy","green energy",
        "energy transition","power outage","energy crisis","energy minister",
        "energy deal","coal plant","hydropower","energy storage",
        "energy security","strategic reserve","strategic petroleum",
        "power grid","electricity shortage","electricity prices",
        "vision 2030 energy","neom energy","green hydrogen",
        "carbon neutral","net zero","clean fuel",
        "solar energy project","wind energy project",
        "desalination plant","water energy",
        "electricity grid attack","power station attack",
        "energy sanctions","energy embargo","energy revenue",
        "carbon emissions","greenhouse gas","paris agreement","cop summit",
        "climate change energy","global warming energy",
    ]),

    # ── ECONOMIC ─────────────────────────────────────────────────────────
    ("Economic", "Trade & Finance", [
        "trade deal","trade war","tariff","import duty","export ban","sanctions",
        "stock market","shares fell","shares rose","gdp","inflation",
        "interest rate","central bank","federal reserve","imf","world bank",
        "debt crisis","currency","exchange rate","investment","foreign investment",
        "ipo","merger","acquisition","bankruptcy","recession","economic growth",
        "trade deficit","trade surplus","supply chain",
        "sovereign wealth fund","pif","public investment fund",
        "credit rating","bond issuance","sukuk","foreign reserves",
        "current account","fiscal deficit","budget deficit",
        "austerity","subsidy cut","subsidy reform","privatization",
        "tadawul","stock exchange listing","ipo listing",
        "remittances","tourism revenue","non-oil gdp",
        "economic diversification","free zone","economic zone",
        "digital economy","fintech","startup ecosystem",
        "food security","water security",
    ]),
    ("Economic", "Business & Industry", [
        "earnings","profit","revenue","quarterly results",
        "layoffs","job cuts","hiring freeze","factory closure","manufacturing",
        "pharmaceutical","agriculture","livestock","commodity",
        "wheat","corn","soybean","fertilizer","mining","copper","gold",
        "construction boom","infrastructure spending","giga project","neom",
        "real estate","property market","ports","logistics","shipping route",
        "container shipping","freight rates","labor market","job creation",
        "vision 2030","gulf cooperation","gcc trade","gcc economy",
        "dubai economy","abu dhabi economy","riyadh economy",
    ]),
    ("Economic", "Aid & Development", [
        "foreign aid","humanitarian aid","food aid","relief fund","donor",
        "development bank","loan","grant","debt relief","poverty","famine",
        "flood relief","earthquake aid","reconstruction","infrastructure project",
        "food shortage","starvation","malnutrition",
    ]),

    # ── POLITICAL ────────────────────────────────────────────────────────
    ("Political", "Diplomacy & International Relations", [
        "diplomatic","summit","bilateral","multilateral","treaty",
        "agreement signed","memorandum","mou","foreign minister","state visit",
        "ambassador","united nations","un resolution","security council",
        "g7","g20","negotiations","peace talks","ceasefire talks","mediation",
        "envoy","normalization","diplomatic ties","relations restored",
        "expelled diplomat","iran nuclear","jcpoa","nuclear deal","nuclear talks",
        "abraham accords","normalization talks",
        "two-state solution","west bank","gaza","palestinian authority",
        "arab league","gcc summit","gulf cooperation council",
        "regional security","middle east peace","peace process",
        "hamas ceasefire","hostage deal","prisoner swap",
        "saudi iran","saudi israel","qatar diplomacy","uae diplomacy",
        "egypt mediation","jordan mediation",
        "bin salman","mbs","crown prince",
        "un security council middle east","sanctions lifted","sanctions imposed",
        "diplomatic expulsion","embassy attack","consulate attack",
        "maritime dispute","territorial waters","red sea crisis",
        "bilateral talks","trilateral talks","security pact","defense agreement",
    ]),
    ("Political", "Government & Domestic Politics", [
        "president","prime minister","parliament","congress","senate","cabinet",
        "election","vote","ballot","campaign","party","coalition","opposition",
        "legislation","law passed","bill signed","impeachment","resignation",
        "minister","governor","mayor","policy","reform","budget proposal",
        "government says","administration","white house","kremlin","downing street",
        "political transition","regime change","political asylum",
        "protest movement","popular uprising","election fraud",
        "political prisoner","opposition leader","crackdown","dissidents",
        "netanyahu","biden middle east","trump middle east",
        "sudan crisis","lebanon crisis","syria crisis","iraq politics",
        "yemen peace","yemen talks","houthi negotiations",
    ]),
    ("Political", "Conflict & Geopolitics", [
        "geopolitical","territorial","sovereignty","occupation","annexation",
        "disputed","tension between","standoff","confrontation","provocation",
        "coup","regime","authoritarian",
        "horn of africa","east africa diplomacy",
        "cop summit","paris agreement","climate policy","environmental policy",
    ]),

    # ── OTHER ─────────────────────────────────────────────────────────────
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
    ("Other", "Education & Society", [
        "education reform","school reform","literacy","university ranking",
        "student","curriculum","teacher","classroom","school funding",
        "scholarship","education ministry","education policy",
        "education system","academic freedom",
    ]),
    ("Other", "Lifestyle & Society", [
        "recipe ","restaurant ","food festival","fashion week","horoscope",
        "lottery winner","horse racing","poker","beauty pageant","wedding",
        "viral video","tiktok","instagram","influencer","animal rescue",
        "obituary ","died at age","passed away","funeral",
        "pandemic","epidemic","outbreak","virus","covid","monkeypox",
        "vaccine","vaccination campaign","disease spread","infection rate",
        "who alert","hospital overwhelmed","healthcare crisis","mental health",
        "earthquake","tsunami","volcanic eruption","flood","drought",
        "wildfire","hurricane","species extinction","biodiversity",
    ]),
    ("Other", "Science & Technology", [
        "nasa ","space mission","rocket launch","asteroid","planet discovered",
        "artificial intelligence","machine learning","chatgpt","openai","gemini",
        "smartphone","iphone","android","semiconductor","chip shortage",
        "scientific study","research finds","climate change","global warming",
        "species discovered","genome","pollution","air quality","water pollution",
        "heat wave","extreme weather",
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
        # Remove leading numbers/dates (e.g. "2026 06 05 woman..." → "woman...")
        t = re.sub(r"^[\d\s\W]+", "", t).strip()
        return t[0].upper() + t[1:] if t else ""
    return ""


def classify_source(domain):
    if not domain: return "Online News"
    d = domain.lower()
    if any(x in d for x in ["reuters","apnews","bloomberg","xinhua","tass","prnewswire","businesswire","globenewswire","aninews"]): return "Wire Service"
    if any(x in d for x in ["aljazeera","foxnews","nbcnews","cbsnews","cnn.","msnbc","skynews","france24","dw.com","bbc.","abc.net"]): return "TV Network"
    if any(x in d for x in ["iheart","npr.org"]) or (d.endswith("fm.com") and len(d)<15): return "Radio"
    if any(x in d for x in [".gov",".mil","un.org","who.int","state.gov","europa.eu"]): return "Government"
    if any(x in d for x in [".edu",".ac.uk","university","research","pubmed","arxiv"]): return "Academic"
    if any(x in d for x in ["wsj.com","ft.com","cnbc.com","marketwatch","moneycontrol"]): return "Financial"
    if any(x in d for x in ["theguardian","nytimes","washingtonpost","independent.co.uk","telegraph","dailymail","thehindu","arabnews","gulfnews","punchng"]): return "Major Newspaper"
    if any(x in d for x in ["tribune","times","post","news","daily","herald","gazette","journal","chronicle","observer","dispatch","courier","star","mirror"]): return "Local / Regional News"
    return "Online News"


def fetch_latest_url():
    # Try yesterday first, then 2 days ago as fallback
    for days_back in [1, 2]:
        target = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d")
        url = f"http://data.gdeltproject.org/events/{target}.export.CSV.zip"
        date_str = f"{target[:4]}-{target[4:6]}-{target[6:8]}"
        log.info(f"Checking GDELT v1 daily file for: {date_str}")
        resp = requests.head(url, timeout=15)
        if resp.status_code == 200:
            log.info(f"File found — downloading: {url}")
            return url, date_str
        log.warning(f"File not available for {date_str} — trying previous day")
    log.warning("No file available for yesterday or 2 days ago — skipping run")
    return None, date_str


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
    # Protect important columns from being dropped
    protected = {"DATEADDED", "SOURCEURL", "GlobalEventID", "EventRootCode",
                 "GoldsteinScale", "NumMentions", "AvgTone",
                 "ActionGeo_CountryCode", "ActionGeo_FullName",
                 "Actor1Name", "Actor2Name"}
    drop_cols = [c for c in df.columns if df[c].isnull().mean() > 0.90 and c not in protected]
    df.drop(columns=drop_cols, inplace=True)

    df["domain"]      = df["SOURCEURL"].str.extract(r"https?://(?:www\.)?([^/:]+)")
    df["title"]       = df["SOURCEURL"].apply(extract_title)
    df["source_type"] = df["domain"].apply(classify_source)
    df["src_priority"]= df["source_type"].map(SOURCE_PRIORITY).fillna(11)

    # ── Dedup: keep best source per event ──────────────────────────────
    # Sort by priority (lower = better), keep first
    df = df.sort_values("src_priority")
    df = df.drop_duplicates(subset=["GlobalEventID"], keep="first")
    log.info(f"After dedup (best source): {len(df):,}")

    df["sentiment"] = pd.to_numeric(df["AvgTone"], errors="coerce").fillna(0).apply(
        lambda t: "Positive" if t > 2 else ("Negative" if t < -2 else "Neutral"))
    df["event_date"] = event_date

    classified = df.apply(
        lambda r: pd.Series(full_classify(str(r.get("title","")), r.get("EventRootCode"))),
        axis=1,
    )
    df["category"]     = classified[0]
    df["sub_category"] = classified[1]

    # Parse DATEADDED → readable datetime string
    def parse_dateadded(val):
        try:
            s = str(int(val))
            if len(s) == 14:
                return f"{s[:4]}-{s[4:6]}-{s[6:8]} {s[8:10]}:{s[10:12]}:{s[12:14]}"
        except:
            pass
        return ""
    df["date_added"] = df["DATEADDED"].apply(parse_dateadded)

    keep = ["event_date","date_added","title","Actor1Name","Actor2Name",
            "category","sub_category","sentiment","GoldsteinScale","AvgTone",
            "ActionGeo_CountryCode","ActionGeo_FullName",
            "NumMentions","domain","source_type","SOURCEURL"]
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
    event_date = df["event_date"].iloc[0]
    # Delete existing data for this date first — ensures fresh clean data
    log.info(f"Deleting existing data for {event_date}...")
    supabase.table("gdelt_events").delete().eq("event_date", event_date).execute()
    log.info(f"Deleted — now inserting {len(df):,} fresh rows...")
    records = df.to_dict("records")
    BATCH = 500
    saved = 0
    for i in range(0, len(records), BATCH):
        supabase.table("gdelt_events").insert(records[i:i+BATCH]).execute()
        saved += BATCH
        log.info(f"  Saved {min(saved,len(records)):,} / {len(records):,}")
    log.info(f"Done — {len(records):,} rows inserted")


def run():
    log.info("=" * 60)
    log.info(f"Run started: {datetime.now(timezone.utc).isoformat()}")
    try:
        url, event_date = fetch_latest_url()
        if url is None:
            log.info("No file available — pipeline skipped cleanly")
            return
        raw   = download_and_parse(url)
        clean = process(raw, event_date)
        save_to_supabase(clean)
        log.info("Pipeline completed successfully")
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run()

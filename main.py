from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict, Tuple
from datetime import datetime, timezone, timedelta
import httpx
import json
import re

app = FastAPI(
    title="Polymarket GPT API",
    version="5.6.0",
    description="Read-only API for Polymarket trade analysis using Gamma, CLOB, Data APIs, and ESPN public endpoints"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"
ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

SLUG_PREFIXES: Dict[str, List[str]] = {
    "nba": ["nba-"],
    "nhl": ["nhl-"],
    "mlb": ["mlb-"],
    "nfl": ["nfl-"],
    "cbb": ["cbb-"],
    "ncaab": ["cbb-"],
    "mls": ["mls-"],
    "epl": ["epl-"],
    "ucl": ["ucl-"],
    "ufc": ["ufc-"],
    "soccer": ["epl-", "ucl-", "mls-", "liga-", "seri-", "bund-"],
    "all": ["nba-", "nhl-", "mlb-", "nfl-", "cbb-", "mls-", "epl-", "ucl-", "ufc-"],
}
ALL_SPORT_PREFIXES = ["nba-", "nhl-", "mlb-", "nfl-", "cbb-", "mls-", "epl-", "ucl-", "ufc-"]

GAME_MARKET_TYPES = {
    "moneyline", "winner", "match_winner",
    "spreads", "first_half_spreads", "second_half_spreads",
    "totals", "first_half_totals", "second_half_totals",
    "player_props", "team_props"
}

FUTURES_MARKET_TYPES = {
    "outright_winner", "futures", "season_wins",
    "playoff_odds", "championship"
}

ESPN_SPORT_MAP = {
    "nba":   ("basketball", "nba"),
    "nfl":   ("football", "nfl"),
    "mlb":   ("baseball", "mlb"),
    "nhl":   ("hockey", "nhl"),
    "cbb":   ("basketball", "mens-college-basketball"),
    "ncaab": ("basketball", "mens-college-basketball"),
    "mls":   ("soccer", "usa.1"),
    "epl":   ("soccer", "eng.1"),
    "ucl":   ("soccer", "uefa.champions"),
    "ufc":   ("mma", "ufc"),
    # soccer is an umbrella — map to EPL as default scoreboard, others handled separately
}

# Canonical sport enum — used everywhere for validation
CANONICAL_SPORTS = {"nba", "nhl", "mlb", "nfl", "cbb", "mls", "epl", "ucl", "ufc", "soccer", "all"}

# Sports that have live scoreboard support
SCOREBOARD_SPORTS = {"nba", "nhl", "mlb", "nfl", "cbb", "mls", "epl", "ucl", "ufc"}

# Soccer leagues covered under "soccer" umbrella
SOCCER_LEAGUES = ["epl", "ucl", "mls"]

# Normalize ESPN display names → cleaner search terms.
ESPN_NAME_NORMALIZE: Dict[str, str] = {
    "Charlotte 49ers": "Charlotte",
    "Saint Louis Billikens": "Saint Louis",
    "George Washington Colonials": "George Washington",
    "George Washington Revolutionaries": "George Washington",
    "UAB Blazers": "UAB",
    "UTSA Roadrunners": "UTSA",
    "FIU Panthers": "FIU",
    "FAU Owls": "FAU",
    "UTEP Miners": "UTEP",
    "UConn Huskies": "UConn",
    "VCU Rams": "VCU",
    "SMU Mustangs": "SMU",
    "TCU Horned Frogs": "TCU",
    "BYU Cougars": "BYU",
    "USC Trojans": "USC",
    "UCLA Bruins": "UCLA",
    "LSU Tigers": "LSU",
    "St. Louis Blues": "St Louis",
    "St. John's Red Storm": "St Johns",
    "Saint John's Red Storm": "St Johns",
    "Paris Saint-Germain": "PSG",
}

# Direct slug abbreviations when known
ESPN_TO_POLY: Dict[str, str] = {
    # NBA
    "Atlanta Hawks": "atl", "Boston Celtics": "bos", "Brooklyn Nets": "bkn",
    "Charlotte Hornets": "cha", "Chicago Bulls": "chi", "Cleveland Cavaliers": "cle",
    "Dallas Mavericks": "dal", "Denver Nuggets": "den", "Detroit Pistons": "det",
    "Golden State Warriors": "gsw", "Houston Rockets": "hou", "Indiana Pacers": "ind",
    "LA Clippers": "lac", "Los Angeles Lakers": "lal", "Memphis Grizzlies": "mem",
    "Miami Heat": "mia", "Milwaukee Bucks": "mil", "Minnesota Timberwolves": "min",
    "New Orleans Pelicans": "no", "New York Knicks": "ny", "Oklahoma City Thunder": "okc",
    "Orlando Magic": "orl", "Philadelphia 76ers": "phi", "Phoenix Suns": "pho",
    "Portland Trail Blazers": "por", "Sacramento Kings": "sac", "San Antonio Spurs": "sa",
    "Toronto Raptors": "tor", "Utah Jazz": "uta", "Washington Wizards": "wsh",

    # NHL
    "Anaheim Ducks": "ana", "Boston Bruins": "bos", "Buffalo Sabres": "buf",
    "Calgary Flames": "cgy", "Carolina Hurricanes": "car", "Chicago Blackhawks": "chi",
    "Colorado Avalanche": "col", "Columbus Blue Jackets": "cbj", "Dallas Stars": "dal",
    "Detroit Red Wings": "det", "Edmonton Oilers": "edm", "Florida Panthers": "fla",
    "Los Angeles Kings": "lak", "Minnesota Wild": "min", "Montreal Canadiens": "mtl",
    "Nashville Predators": "nsh", "New Jersey Devils": "njd", "New York Islanders": "nyi",
    "New York Rangers": "nyr", "Ottawa Senators": "ott", "Philadelphia Flyers": "phi",
    "Pittsburgh Penguins": "pit", "San Jose Sharks": "sjs", "Seattle Kraken": "sea",
    "St. Louis Blues": "stl", "Tampa Bay Lightning": "tb", "Toronto Maple Leafs": "tor",
    "Vancouver Canucks": "van", "Vegas Golden Knights": "vgk", "Washington Capitals": "wsh",
    "Winnipeg Jets": "wpg",

    # Some common CBB / CFB / soccer style overrides can be added as needed
    "Ohio State Buckeyes": "ohiost",
    "Michigan Wolverines": "mich",
    "Kentucky Wildcats": "uk",
    "Florida Gators": "fl",
    "George Washington Revolutionaries": "geows",
    "Saint Louis Billikens": "stlou",
    "Missouri State Bears": "msrst",
    "Louisiana Tech Bulldogs": "loutch",
    "Charlotte 49ers": "charlt",
    "UAB Blazers": "uab",
}

STOPWORDS_FOR_SLUG = {
    "university", "college", "state", "st", "saint", "red", "blue", "green",
    "golden", "wild", "fighting", "the", "of", "at", "fc", "cf", "club", "athletic",
    "basketball", "football", "baseball", "hockey", "soccer", "men", "womens", "women"
}


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

async def fetch_json(base: str, path: str, params: Optional[dict] = None) -> Any:
    url = f"{base}{path}"
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Upstream error from {url}: {e.response.text[:500]}"
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Request failed for {url}: {str(e)}")


def parse_possible_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
    except Exception:
        return None


def normalize_espn_name(name: str) -> str:
    return ESPN_NAME_NORMALIZE.get(name, name)


def normalize_text(s: str) -> str:
    s = (s or "").lower()
    s = s.replace("&", " and ")
    s = s.replace(".", "")
    s = s.replace("'", "")
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def slug_word_parts(name: str) -> List[str]:
    name = normalize_espn_name(name)
    parts = normalize_text(name).split()
    cleaned = [p for p in parts if p not in STOPWORDS_FOR_SLUG]
    return cleaned or parts or [normalize_text(name)]


def fallback_poly_abbr(name: str) -> str:
    parts = slug_word_parts(name)
    if not parts:
        return normalize_text(name).replace(" ", "")[:6]

    # Prefer a meaningful last token for schools/clubs, otherwise join a couple of tokens
    if len(parts) == 1:
        return parts[0][:6]

    # Handle common school naming patterns better than "last word only"
    joined = "".join(parts[-2:])[:8]
    last = parts[-1][:6]

    # If the last token is too generic, use the joined form
    if last in {"team", "club", "city"}:
        return joined

    # For schools with short known forms like "north texas" -> "ntx"
    if len(parts[-2]) <= 5 and len(parts[-1]) <= 5:
        compact = f"{parts[-2][:3]}{parts[-1][:3]}"[:8]
        return compact

    return joined if len(joined) >= 4 else last


def build_poly_slug(sport: str, away_team: str, home_team: str, date_str: str) -> str:
    away_team = normalize_espn_name(away_team)
    home_team = normalize_espn_name(home_team)
    away_abbr = ESPN_TO_POLY.get(away_team) or fallback_poly_abbr(away_team)
    home_abbr = ESPN_TO_POLY.get(home_team) or fallback_poly_abbr(home_team)
    prefix = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])[0].rstrip("-")
    return f"{prefix}-{away_abbr}-{home_abbr}-{date_str}"


def extract_token_ids(m: Dict[str, Any]) -> List[str]:
    candidates = [
        m.get("clobTokenIds"),
        m.get("tokenIds"),
        m.get("tokens"),
    ]
    out: List[str] = []

    for c in candidates:
        parsed = parse_possible_json(c)
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict):
                    token_id = item.get("token_id") or item.get("id")
                    if token_id:
                        out.append(str(token_id))
                elif item is not None:
                    out.append(str(item))

    return list(dict.fromkeys([x for x in out if x]))


def text_blob(m: Dict[str, Any]) -> str:
    fields = [
        m.get("question"),
        m.get("title"),
        m.get("slug"),
        m.get("eventSlug"),
        m.get("eventTitle"),
        m.get("description"),
        m.get("category"),
        " ".join([str(x) for x in parse_possible_json(m.get("outcomes")) or []]),
    ]
    return normalize_text(" ".join([str(x or "") for x in fields]))


def to_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except Exception:
        return default


def liquidity_key(m: Dict[str, Any]) -> float:
    return to_float(m.get("liquidityNum"), to_float(m.get("liquidity"), 0.0))


def volume_key(m: Dict[str, Any]) -> float:
    return to_float(m.get("volumeNum"), to_float(m.get("volume"), 0.0))


def yes_price_from_market(m: Dict[str, Any]) -> Optional[float]:
    prices = parse_possible_json(m.get("outcomePrices"))
    if isinstance(prices, list) and prices:
        try:
            return float(prices[0])
        except Exception:
            return None
    return None


def compute_implied_prob_gap(prices: List[Any]) -> Optional[float]:
    try:
        total = sum(float(p) for p in prices if p is not None)
        return round(total - 1.0, 4)
    except Exception:
        return None


def is_tradeable(m: Dict[str, Any]) -> bool:
    if not isinstance(m, dict):
        return False
    if m.get("closed") is True:
        return False
    if m.get("active") is False:
        return False

    outcomes = parse_possible_json(m.get("outcomes"))
    prices = parse_possible_json(m.get("outcomePrices"))

    if outcomes is not None and not isinstance(outcomes, list):
        return False
    if prices is not None and not isinstance(prices, list):
        return False

    return True


def is_futures_market(m: Dict[str, Any]) -> bool:
    market_type = str(m.get("sportsMarketType") or m.get("marketType") or "").lower()
    if market_type in FUTURES_MARKET_TYPES:
        return True

    blob = text_blob(m)
    futures_keywords = [
        "champion", "title", "season wins", "playoffs", "division winner",
        "conference winner", "mvp", "rookie of the year", "finals winner"
    ]
    return any(k in blob for k in futures_keywords) and not is_game_market(m)


def is_game_market(m: Dict[str, Any]) -> bool:
    market_type = str(m.get("sportsMarketType") or m.get("marketType") or "").lower()
    if market_type in GAME_MARKET_TYPES:
        return True
    blob = text_blob(m)
    return " vs " in blob or "@" in blob or " at " in blob


def is_moneyline_market(m: Dict[str, Any]) -> bool:
    market_type = str(m.get("sportsMarketType") or m.get("marketType") or "").lower()
    if market_type in {"moneyline", "winner", "match_winner"}:
        return True
    # If exactly two named team outcomes and not obviously spread/total, treat as ML-ish
    outcomes = parse_possible_json(m.get("outcomes")) or []
    blob = text_blob(m)
    if isinstance(outcomes, list) and len(outcomes) == 2 and "o/u" not in blob and "spread" not in blob:
        return True
    return False


def has_sport_slug_prefix(m: Dict[str, Any], prefixes: List[str]) -> bool:
    slug = (m.get("slug") or m.get("eventSlug") or "").lower()
    return any(slug.startswith(p) for p in prefixes)


def matches_sport(m: Dict[str, Any], sport: Optional[str]) -> bool:
    if not sport or sport.lower() == "all":
        return True
    prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
    return has_sport_slug_prefix(m, prefixes)


def extreme_price_penalty(m: Dict[str, Any]) -> float:
    p = yes_price_from_market(m)
    if p is None:
        return 0.0
    if p <= 0.02 or p >= 0.98:
        return -20.0
    if p <= 0.05 or p >= 0.95:
        return -10.0
    return 0.0


def market_quality_score(m: Dict[str, Any]) -> str:
    """
    Tags market with quality tier based on liquidity.
    high   = $100K+   — major NBA/NHL game, fully tradeable
    medium = $10K-$100K — solid market, tradeable with care
    low    = $1K-$10K  — thin, treat with caution
    thin   = under $1K  — avoid unless specific reason
    """
    liq = liquidity_key(m)
    if liq >= 100_000:
        return "high"
    if liq >= 10_000:
        return "medium"
    if liq >= 1_000:
        return "low"
    return "thin"


def normalize_market(m: Dict[str, Any]) -> Dict[str, Any]:
    nm = dict(m)

    outcomes = parse_possible_json(nm.get("outcomes"))
    if isinstance(outcomes, list):
        nm["outcomes"] = outcomes

    prices = parse_possible_json(nm.get("outcomePrices"))
    if isinstance(prices, list):
        nm["outcomePrices"] = prices
        if len(prices) >= 2:
            nm["impliedProbGap"] = compute_implied_prob_gap(prices)

    token_ids = extract_token_ids(nm)
    if token_ids:
        nm["tokenIds"] = token_ids

    nm["isGameMarket"] = is_game_market(nm)
    nm["isFuturesMarket"] = is_futures_market(nm)
    nm["isMoneyline"] = is_moneyline_market(nm)
    nm["liquidityNum"] = liquidity_key(nm)
    nm["volumeNum"] = volume_key(nm)
    nm["marketQuality"] = market_quality_score(nm)
    # These get filled in by external verification — default false
    nm["verificationComplete"] = False
    nm["externalFairPrice"] = None

    return nm


def game_is_live_by_dates(obj: Dict[str, Any]) -> bool:
    now = datetime.now(timezone.utc)
    start = parse_dt(obj.get("startDate") or obj.get("startDateIso") or obj.get("gameStartTime"))
    end = parse_dt(obj.get("endDate") or obj.get("endDateIso"))
    if start and end:
        return start <= now <= end
    if start:
        # Conservative: within 4 hours after start counts as possibly live if no end date
        return start <= now <= (start + timedelta(hours=4))
    return False


def game_is_today(obj: Dict[str, Any]) -> bool:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    start = obj.get("startDate") or obj.get("startDateIso") or obj.get("gameStartTime") or ""
    return str(start)[:10] == today_str


def annotate_market_time_flags(m: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(m)
    if "isLive" not in out or out.get("isLive") is None:
        out["isLive"] = game_is_live_by_dates(out)
    if "isToday" not in out or out.get("isToday") is None:
        out["isToday"] = game_is_today(out)
    return out


def event_to_markets(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    markets = []
    event_slug = event.get("slug", "")
    event_title = event.get("title", "")
    event_cat = event.get("category", "")
    is_live = game_is_live_by_dates(event)
    is_today = game_is_today(event)

    for m in event.get("markets", []):
        if not is_tradeable(m):
            continue
        mm = dict(m)
        if not mm.get("eventSlug"):
            mm["eventSlug"] = event_slug
        if not mm.get("eventTitle"):
            mm["eventTitle"] = event_title
        if not mm.get("category"):
            mm["category"] = event_cat
        if not mm.get("liquidity") and not mm.get("liquidityNum"):
            mm["liquidity"] = event.get("liquidity")
        if not mm.get("volume") and not mm.get("volumeNum"):
            mm["volume"] = event.get("volume")
        if not mm.get("startDate"):
            mm["startDate"] = event.get("startDate") or event.get("startDateIso")
        mm["isLive"] = is_live
        mm["isToday"] = is_today
        prices = parse_possible_json(mm.get("outcomePrices"))
        if isinstance(prices, list) and len(prices) >= 2:
            mm["impliedProbGap"] = compute_implied_prob_gap(prices)
        markets.append(mm)

    return markets


def aliases_for_team(team: str) -> List[str]:
    team = normalize_espn_name(team)
    base = normalize_text(team)
    parts = base.split()
    aliases = {base}

    if len(parts) >= 2:
        aliases.add(" ".join(parts[-2:]))
        aliases.add(parts[-1])
        aliases.add(parts[0])

    replacements = {
        "saint": "st",
        "st": "saint",
        "and": "&",
        "&": "and",
    }

    expanded = set()
    for a in aliases:
        expanded.add(a)
        for old, new in replacements.items():
            if old in a:
                expanded.add(a.replace(old, new))
    aliases |= expanded

    return [a for a in aliases if a]


def score_game_candidate(m: Dict[str, Any], team1: str, team2: str) -> float:
    blob = text_blob(m)
    score = 0.0

    team1_hits = sum(1 for t in aliases_for_team(team1) if t in blob)
    team2_hits = sum(1 for t in aliases_for_team(team2) if t in blob)

    if team1_hits == 0 and normalize_text(team1)[:4] in blob:
        team1_hits = 0.5
    if team2_hits == 0 and normalize_text(team2)[:4] in blob:
        team2_hits = 0.5

    score += team1_hits * 15.0
    score += team2_hits * 15.0

    if team1_hits > 0 and team2_hits > 0:
        score += 35.0
    if is_game_market(m):
        score += 20.0
    if is_moneyline_market(m):
        score += 25.0
    if is_futures_market(m):
        score -= 35.0

    score += extreme_price_penalty(m)
    score += liquidity_key(m) / 100000.0
    score += volume_key(m) / 100000.0

    return score


def normalize_espn_competition(event: Dict[str, Any]) -> Dict[str, Any]:
    status = event.get("status", {})
    competitions = event.get("competitions", [{}])
    comp = competitions[0] if competitions else {}
    competitors = comp.get("competitors", [])

    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") == "away"), {})

    home_team = home.get("team", {}).get("displayName", "")
    away_team = away.get("team", {}).get("displayName", "")
    home_score = home.get("score", "")
    away_score = away.get("score", "")

    return {
        "id": event.get("id"),
        "name": event.get("name"),
        "status": status.get("type", {}).get("description", ""),
        "state": status.get("type", {}).get("state", ""),
        "clock": status.get("displayClock", ""),
        "period": status.get("period", ""),
        "isLive": status.get("type", {}).get("state", "") == "in",
        "isCompleted": status.get("type", {}).get("completed", False),
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "score_display": f"{away_team} {away_score} - {home_score} {home_team}",
        "date": event.get("date"),
    }


async def fetch_espn_scoreboard_events_for_discovery(sport: str) -> List[Dict[str, Any]]:
    """
    Discovery source for live games.
    CBB: uses explicit dates + groups=50 + high limit to cover full D-I slate.
    UFC: uses MMA scoreboard.
    soccer: fans out to EPL + UCL + MLS scoreboards and merges.
    """
    sport_key = sport.lower()

    # Soccer umbrella — fan out to all soccer leagues
    if sport_key == "soccer":
        all_events: List[Dict[str, Any]] = []
        for league in SOCCER_LEAGUES:
            all_events.extend(await fetch_espn_scoreboard_events_for_discovery(league))
        return all_events

    if sport_key not in ESPN_SPORT_MAP:
        return []

    sport_path, league = ESPN_SPORT_MAP[sport_key]
    params: Dict[str, Any] = {}

    if sport_key in {"cbb", "ncaab"}:
        today_yyyymmdd = datetime.now(timezone.utc).strftime("%Y%m%d")
        params = {"dates": today_yyyymmdd, "groups": 50, "limit": 500}

    try:
        data = await fetch_json(ESPN_BASE, f"/{sport_path}/{league}/scoreboard", params=params)
        events = data.get("events") or []
        return events if isinstance(events, list) else []
    except Exception:
        return []


async def fetch_all_active_events(max_pages: int = 5) -> List[Dict[str, Any]]:
    all_events: List[Dict[str, Any]] = []
    limit = 100
    offset = 0

    for _ in range(max_pages):
        try:
            page = await fetch_json(
                GAMMA_BASE,
                "/events",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                    "order": "startDate",
                    "ascending": "false",
                },
            )
            if not isinstance(page, list) or len(page) == 0:
                break
            all_events.extend(page)
            if len(page) < limit:
                break
            offset += len(page)
        except Exception:
            break

    return all_events


async def fetch_active_markets_by_prefix(prefixes: List[str], max_pages: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch active markets directly from /markets endpoint, filtered by slug prefix.
    Pagination fix: advance by raw upstream page length, not by filtered kept count.
    """
    all_markets: List[Dict[str, Any]] = []
    seen_ids: set = set()
    limit = 100
    offset = 0

    for _ in range(max_pages):
        try:
            page = await fetch_json(
                GAMMA_BASE,
                "/markets",
                params={
                    "active": "true",
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                    "order": "volume24hr",
                    "ascending": "false",
                },
            )
            if not isinstance(page, list) or len(page) == 0:
                break

            for m in page:
                slug = (m.get("slug") or "").lower()
                if not any(slug.startswith(p) for p in prefixes):
                    continue
                if not is_tradeable(m):
                    continue
                mid = str(m.get("id") or slug)
                if mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)

            if len(page) < limit:
                break
            offset += len(page)
        except Exception:
            break

    return all_markets


async def fetch_clob_pricing_for_token(token_id: str) -> Dict[str, Any]:
    errors = []
    price_buy = None
    price_sell = None
    midpoint = None
    spread = None
    book_summary = None

    try:
        price_buy = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "BUY"})
    except Exception as e:
        errors.append(f"BUY price failed: {str(e)}")
    try:
        price_sell = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "SELL"})
    except Exception as e:
        errors.append(f"SELL price failed: {str(e)}")
    try:
        midpoint = await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})
    except Exception as e:
        errors.append(f"Midpoint failed: {str(e)}")
    try:
        spread = await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})
    except Exception as e:
        errors.append(f"Spread failed: {str(e)}")
    try:
        book_summary = await fetch_json(CLOB_BASE, "/book", params={"token_id": token_id})
    except Exception as e:
        errors.append(f"Book failed: {str(e)}")

    result = {
        "token_id": token_id,
        "price_buy": price_buy,
        "price_sell": price_sell,
        "midpoint": midpoint,
        "spread": spread,
    }
    if book_summary is not None:
        result["book_summary"] = book_summary
    if errors:
        result["errors"] = errors
    return result


def extract_markets_from_search(search_res: Dict[str, Any], exclude_futures: bool = False) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if not isinstance(search_res, dict):
        return out

    # Be permissive with possible response shapes.
    candidates = []
    for key in ("markets", "items", "results", "data"):
        value = search_res.get(key)
        if isinstance(value, list):
            candidates.extend(value)

    # Some public-search responses nest market-like objects deeper.
    if not candidates:
        for value in search_res.values():
            if isinstance(value, list):
                candidates.extend([x for x in value if isinstance(x, dict)])

    seen_ids = set()
    for item in candidates:
        if not isinstance(item, dict):
            continue

        m = item.get("market") if isinstance(item.get("market"), dict) else item
        if not isinstance(m, dict):
            continue
        if not is_tradeable(m):
            continue
        if exclude_futures and is_futures_market(m):
            continue

        mid = str(m.get("id") or m.get("slug") or "")
        if mid and mid not in seen_ids:
            seen_ids.add(mid)
            out.append(m)

    return out


# ─────────────────────────────────────────
# CORE ENDPOINTS
# ─────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "Polymarket GPT API is live",
        "status": "ok",
        "version": "5.5.0",
        "apis": {"gamma": GAMMA_BASE, "clob": CLOB_BASE, "data": DATA_BASE},
        "supported_sports": list(SLUG_PREFIXES.keys()),
        "pipeline": "ESPN discovery (CBB uses dates+groups=50+limit) → direct slug → find_slug fallback → CLOB price",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/categories")
def categories():
    return {
        "categories": ["all", "sports", "politics", "crypto", "news", "current-events"],
        "sports": list(SLUG_PREFIXES.keys()),
        "slug_prefixes": SLUG_PREFIXES,
    }


@app.get("/public-search")
async def public_search(
    q: str = Query(...),
    limit_per_type: int = Query(default=25, ge=1, le=100),
    page: int = Query(default=1, ge=1),
    events_status: str = Query(default="active"),
):
    return await fetch_json(
        GAMMA_BASE,
        "/public-search",
        params={
            "q": q,
            "limit_per_type": limit_per_type,
            "page": page,
            "events_status": events_status,
            "keep_closed_markets": 0,
            "search_profiles": False,
            "optimized": True,
        },
    )


@app.get("/markets")
async def markets(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
):
    raw = await fetch_json(
        GAMMA_BASE,
        "/markets",
        params={"limit": 300, "active": "true", "closed": "false"},
    )
    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response from Gamma API")

    items = [annotate_market_time_flags(normalize_market(m)) for m in raw if is_tradeable(m)]

    if search:
        s = normalize_text(search)
        items = [m for m in items if s in text_blob(m)]

    if category.lower() == "sports" or sport:
        items = [m for m in items if any(has_sport_slug_prefix(m, prefixes) for prefixes in SLUG_PREFIXES.values())]

    if sport:
        items = [m for m in items if matches_sport(m, sport)]

    if category.lower() not in ("all", "sports"):
        cat = normalize_text(category)
        items = [m for m in items if cat in text_blob(m)]

    items = sorted(items, key=lambda m: (liquidity_key(m), volume_key(m)), reverse=True)[:limit]

    return {
        "category": category,
        "sport": sport,
        "search": search,
        "limit": limit,
        "count": len(items),
        "markets": items,
    }


@app.get("/find-market")
async def find_market(
    query: str = Query(...),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50),
):
    queries = [query]
    if sport:
        prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
        queries += prefixes

    seen_ids: set = set()
    all_markets: List[Dict[str, Any]] = []

    for q in queries:
        try:
            search_res = await public_search(q=q, limit_per_type=50, page=1, events_status="active")
            for m in extract_markets_from_search(search_res):
                mid = str(m.get("id") or m.get("slug") or "")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(annotate_market_time_flags(normalize_market(m)))
        except Exception:
            continue

    if sport:
        prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
        all_markets = [m for m in all_markets if has_sport_slug_prefix(m, prefixes)]

    ranked = sorted(
        all_markets,
        key=lambda m: extreme_price_penalty(m) + liquidity_key(m) / 100000.0 + volume_key(m) / 100000.0,
        reverse=True,
    )[:limit]

    return {
        "query": query,
        "sport": sport,
        "count": len(ranked),
        "markets": ranked,
    }


@app.get("/find-game")
async def find_game(
    team1: str = Query(...),
    team2: str = Query(...),
    sport: str = Query(default="nba"),
    limit: int = Query(default=10, ge=1, le=30),
):
    def short_name(team: str) -> str:
        team = normalize_espn_name(team)
        parts = team.strip().split()
        return parts[-1] if len(parts) > 1 else team

    t1 = normalize_espn_name(team1)
    t2 = normalize_espn_name(team2)
    t1_short = short_name(t1)
    t2_short = short_name(t2)

    queries = [
        f"{t1} vs {t2}",
        f"{t2} vs {t1}",
        f"{t1_short} vs {t2_short}",
        f"{t2_short} vs {t1_short}",
        f"{t1} {t2}",
        f"{t1_short} {t2_short}",
        t1,
        t2,
        t1_short,
        t2_short,
    ]

    seen_q: set = set()
    queries = [q for q in queries if q and not (q in seen_q or seen_q.add(q))]

    seen_ids: set = set()
    all_markets: List[Dict[str, Any]] = []

    for q in queries:
        try:
            search_res = await public_search(q=q, limit_per_type=50, page=1, events_status="active")
            for m in extract_markets_from_search(search_res, exclude_futures=True):
                mid = str(m.get("id") or m.get("slug") or "")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(annotate_market_time_flags(normalize_market(m)))
        except Exception:
            continue

    # Critical fix: filter by sport before ranking
    prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
    all_markets = [m for m in all_markets if has_sport_slug_prefix(m, prefixes)]

    ranked_pairs = [(score_game_candidate(m, t1, t2), m) for m in all_markets]
    ranked_pairs.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, market in ranked_pairs[:limit]:
        nm = dict(market)
        nm["matchScore"] = round(score, 2)
        results.append(nm)

    return {
        "team1": team1,
        "team2": team2,
        "sport": sport,
        "count": len(results),
        "markets": results,
    }


@app.get("/find-slug")
async def find_slug(
    team1: str = Query(...),
    team2: str = Query(...),
    sport: str = Query(default="nba"),
):
    t1 = normalize_espn_name(team1)
    t2 = normalize_espn_name(team2)

    found = await find_game(team1=t1, team2=t2, sport=sport, limit=5)
    moneyline_markets = [m for m in found["markets"] if m.get("isMoneyline")]
    slugs = [m["slug"] for m in found["markets"] if m.get("slug")]

    return {
        "team1": team1,
        "team2": team2,
        "team1_normalized": t1,
        "team2_normalized": t2,
        "sport": sport,
        "slugs": slugs,
        "recommended_slug": moneyline_markets[0]["slug"] if moneyline_markets else (slugs[0] if slugs else None),
        "markets": found["markets"],
    }


@app.get("/market-details")
async def market_details(
    id: Optional[str] = Query(default=None),
    slug: Optional[str] = Query(default=None),
):
    if not id and not slug:
        raise HTTPException(status_code=400, detail="Provide either id or slug")

    if id:
        raw = await fetch_json(GAMMA_BASE, f"/markets/{id}")
        return {"lookup": "id", "market": annotate_market_time_flags(normalize_market(raw))}

    try:
        raw = await fetch_json(GAMMA_BASE, "/markets", params={"slug": slug})
        if isinstance(raw, list) and raw:
            return {"lookup": "slug", "market": annotate_market_time_flags(normalize_market(raw[0]))}
    except Exception:
        pass

    # Fallback: search by exact slug text
    raw = await fetch_json(GAMMA_BASE, "/markets", params={"limit": 100, "active": "true", "closed": "false"})
    if isinstance(raw, list):
        for m in raw:
            if (m.get("slug") or "") == slug:
                return {"lookup": "slug_scan", "market": annotate_market_time_flags(normalize_market(m))}

    raise HTTPException(status_code=404, detail="Market not found")


@app.get("/scan-market")
async def scan_market(
    slug: Optional[str] = Query(default=None),
    id: Optional[str] = Query(default=None),
):
    details = await market_details(id=id, slug=slug)
    chosen_market = details["market"]

    token_ids = extract_token_ids(chosen_market)
    all_pricing = []

    outcomes = parse_possible_json(chosen_market.get("outcomes")) or []
    for idx, token_id in enumerate(token_ids):
        result = await fetch_clob_pricing_for_token(token_id)
        if idx < len(outcomes):
            result["outcome"] = outcomes[idx]
        all_pricing.append(result)

    quality = chosen_market.get("marketQuality") or market_quality_score(chosen_market)

    return {
        "market": chosen_market,
        "clob_pricing": all_pricing,
        "marketQuality": quality,
        "verificationComplete": False,
        "verificationRequired": [
            "injury_status_both_teams" if chosen_market.get("isGameMarket") else "event_context",
            "vegas_line" if chosen_market.get("isGameMarket") else "external_consensus",
            "live_score_and_clock" if chosen_market.get("isGameMarket") else "macro_context",
        ],
        "warning": "thin_market_low_confidence" if quality == "thin" else (
            "low_liquidity_trade_with_caution" if quality == "low" else None
        ),
    }


@app.get("/best-opportunities")
async def best_opportunities(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
    min_price: float = Query(default=0.10, ge=0.0, le=0.5),
    max_price: float = Query(default=0.90, ge=0.5, le=1.0),
    min_liquidity: float = Query(default=5000.0, ge=0.0, description="Minimum liquidity threshold. Default 5000 filters out thin markets."),
    include_thin: bool = Query(default=False, description="Include thin markets under $1K liquidity. Default false."),
):
    data = await markets(category=category, sport=sport, search=None, limit=200)
    items = data["markets"]

    filtered = []
    for m in items:
        p = yes_price_from_market(m)
        if p is not None and (p > max_price or p < min_price):
            continue
        liq = liquidity_key(m)
        if not include_thin and liq < 1000:
            continue
        if liq < min_liquidity:
            continue
        filtered.append(m)

    ranked = sorted(filtered, key=lambda m: (liquidity_key(m), volume_key(m)), reverse=True)[:limit]

    # Tag each with quality
    for m in ranked:
        if "marketQuality" not in m:
            m["marketQuality"] = market_quality_score(m)

    return {
        "category": category,
        "sport": sport,
        "limit": limit,
        "min_price": min_price,
        "max_price": max_price,
        "min_liquidity": min_liquidity,
        "count": len(ranked),
        "message": "Top active markets ranked by liquidity and volume. Thin markets excluded by default.",
        "opportunities": ranked,
    }


# ─────────────────────────────────────────
# MOMENTUM / CLOB ENDPOINTS
# ─────────────────────────────────────────

@app.get("/momentum")
async def momentum(
    token_id: str = Query(...),
    interval: str = Query(default="6h", description="1m, 5m, 1h, 6h, 1d"),
    fidelity: int = Query(default=20),
):
    history = await fetch_json(
        CLOB_BASE,
        "/prices-history",
        params={"market": token_id, "interval": interval, "fidelity": fidelity},
    )

    prices = []
    try:
        history_data = history.get("history") or history
        if isinstance(history_data, list):
            prices = [float(p.get("p") or p.get("price") or 0) for p in history_data if p]
    except Exception:
        pass

    if len(prices) < 2:
        return {
            "token_id": token_id,
            "interval": interval,
            "signal": "unknown",
            "magnitude": None,
            "first_price": None,
            "last_price": None,
            "raw_count": len(prices),
        }

    first = prices[0]
    last = prices[-1]
    mid = prices[len(prices) // 2]
    change = last - first
    magnitude = round(abs(change), 4)

    if abs(change) < 0.02:
        signal = "stable"
    elif change > 0:
        signal = "rising_fast" if (last - mid) > (mid - first) else "rising"
    else:
        signal = "falling_fast" if (mid - last) > (first - mid) else "falling"

    return {
        "token_id": token_id,
        "interval": interval,
        "signal": signal,
        "magnitude": magnitude,
        "change": round(change, 4),
        "first_price": round(first, 4),
        "last_price": round(last, 4),
        "price_history": [round(p, 4) for p in prices],
    }


@app.get("/clob/price")
async def clob_price(
    token_id: str = Query(...),
    side: str = Query(default="BUY"),
):
    return await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": side})


@app.get("/clob/prices")
async def clob_prices(
    token_ids: str = Query(..., description="Comma-separated token ids"),
    side: str = Query(default="BUY"),
):
    ids = [x.strip() for x in token_ids.split(",") if x.strip()]
    results = []
    for token_id in ids:
        try:
            price = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": side})
            results.append({"token_id": token_id, "price": price})
        except Exception as e:
            results.append({"token_id": token_id, "error": str(e)})
    return {"side": side, "count": len(results), "results": results}


@app.get("/clob/book")
async def clob_book(token_id: str = Query(...)):
    return await fetch_json(CLOB_BASE, "/book", params={"token_id": token_id})


@app.get("/clob/midpoint")
async def clob_midpoint(token_id: str = Query(...)):
    return await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})


@app.get("/clob/spread")
async def clob_spread(token_id: str = Query(...)):
    return await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})


@app.get("/clob/history")
async def clob_history(
    token_id: str = Query(...),
    interval: str = Query(default="6h", description="1m, 5m, 1h, 6h, 1d"),
    fidelity: int = Query(default=10),
):
    return await fetch_json(
        CLOB_BASE,
        "/prices-history",
        params={"market": token_id, "interval": interval, "fidelity": fidelity},
    )


@app.get("/price-check")
async def price_check(token_id: str = Query(...)):
    price_buy = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "BUY"})
    price_sell = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "SELL"})
    midpoint = await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})
    spread = await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})
    return {
        "token_id": token_id,
        "price_buy": price_buy,
        "price_sell": price_sell,
        "midpoint": midpoint,
        "spread": spread,
    }


# ─────────────────────────────────────────
# DATA API ENDPOINTS
# ─────────────────────────────────────────

@app.get("/data/open-interest")
async def data_open_interest(market: Optional[str] = Query(default=None)):
    params = {}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/oi", params=params)


@app.get("/data/holders")
async def data_holders(
    market: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    minBalance: int = Query(default=1, ge=1),
):
    params = {"limit": limit, "minBalance": minBalance}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/holders", params=params)


@app.get("/data/trades")
async def data_trades(
    market: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    params = {"limit": limit}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/trades", params=params)


@app.get("/data/live-volume")
async def data_live_volume(event: Optional[str] = Query(default=None)):
    params = {}
    if event:
        params["event"] = event
    return await fetch_json(DATA_BASE, "/live-volume", params=params)


# ─────────────────────────────────────────
# ESPN GAME STATE / DISCOVERY ENDPOINTS
# ─────────────────────────────────────────

@app.get("/game-state")
async def game_state(
    sport: str = Query(..., description="nba, nfl, mlb, nhl, cbb, mls, epl, ucl, ufc, soccer"),
    team: Optional[str] = Query(default=None, description="Filter by team or fighter name"),
):
    sport_key = sport.lower()
    if sport_key not in CANONICAL_SPORTS:
        raise HTTPException(
            status_code=400,
            detail={"error": "unsupported_sport", "sport": sport, "supported": sorted(SCOREBOARD_SPORTS)}
        )
    if sport_key not in SCOREBOARD_SPORTS and sport_key != "soccer":
        raise HTTPException(
            status_code=400,
            detail={"error": "no_scoreboard_for_sport", "sport": sport, "supported": sorted(SCOREBOARD_SPORTS)}
        )

    try:
        events = await fetch_espn_scoreboard_events_for_discovery(sport_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ESPN API error: {str(e)}")

    games = []
    for event in events:
        game_info = normalize_espn_competition(event)
        if team:
            t = team.lower()
            if t not in game_info["home_team"].lower() and t not in game_info["away_team"].lower():
                continue
        games.append(game_info)

    live_games_count = sum(1 for g in games if g["isLive"])

    return {
        "sport": sport_key,
        "total_games": len(games),
        "live_games": live_games_count,
        "games": games,
    }


@app.get("/live-now")
async def live_now(
    sport: Optional[str] = Query(default=None, description="nba, nhl, mlb, nfl, cbb, mls, epl, ucl, ufc, soccer, all. Leave empty for all."),
):
    """
    Most reliable live game endpoint.
    Uses ESPN for all sports. CBB uses dates+groups=50 for full D-I coverage.
    soccer fans out to EPL+UCL+MLS. UFC uses MMA scoreboard.
    Returns live score, clock, period, and Polymarket market for each game.
    """
    sport_input = (sport or "all").lower()

    if sport_input not in CANONICAL_SPORTS:
        raise HTTPException(
            status_code=400,
            detail={"error": "unsupported_sport", "sport": sport, "supported": sorted(CANONICAL_SPORTS)}
        )

    # Expand to individual sports
    if sport_input in ("all", "soccer"):
        if sport_input == "all":
            sports_to_check = list(SCOREBOARD_SPORTS - {"ncaab"})
        else:
            sports_to_check = SOCCER_LEAGUES
    else:
        sports_to_check = [sport_input]

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_live = []

    for sp in sports_to_check:
        if sp not in SCOREBOARD_SPORTS:
            continue

        try:
            events = await fetch_espn_scoreboard_events_for_discovery(sp)
        except Exception:
            continue

        for event in events:
            game = normalize_espn_competition(event)
            if not game["isLive"]:
                continue

            home_name  = game["home_team"]
            away_name  = game["away_team"]
            expected_slug = build_poly_slug(sp, away_name, home_name, today_str)

            reconciliation: Dict[str, Any] = {
                "matchup": f"{away_name} vs {home_name}",
                "expected_slug": expected_slug,
                "poly_market_found": False,
                "poly_slug": None,
                "lookup_method": "pending",
            }

            poly_market = None

            # Direct slug lookup first
            try:
                details = await market_details(slug=expected_slug)
                poly_market = details.get("market")
                reconciliation["poly_market_found"] = True
                reconciliation["poly_slug"] = expected_slug
                reconciliation["lookup_method"] = "direct_slug"
            except Exception:
                pass

            # Fallback: search by full team names
            if not poly_market:
                try:
                    result = await find_slug(team1=away_name, team2=home_name, sport=sp)
                    if result.get("recommended_slug"):
                        details = await market_details(slug=result["recommended_slug"])
                        poly_market = details.get("market")
                        reconciliation["poly_market_found"] = True
                        reconciliation["poly_slug"] = result["recommended_slug"]
                        reconciliation["lookup_method"] = "find_slug_fallback"
                except Exception:
                    reconciliation["lookup_method"] = "not_found"

            market_summary = None
            if poly_market:
                try:
                    scanned = await scan_market(slug=poly_market.get("slug"))
                    market_summary = scanned
                except Exception:
                    market_summary = {"market": poly_market, "clob_pricing": []}

            all_live.append({
                "sport": sp,
                "home_team": home_name,
                "away_team": away_name,
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "clock": game["clock"],
                "period": game["period"],
                "score_display": game["score_display"],
                "expected_slug": expected_slug,
                "polymarket": market_summary["market"] if market_summary and market_summary.get("market") else poly_market,
                "pricing": market_summary.get("clob_pricing") if market_summary else [],
                "reconciliation": reconciliation,
            })

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sport": sport_input,
        "live_game_count": len(all_live),
        "games": all_live,
    }


@app.get("/live-games")
async def live_games(
    sport: Optional[str] = Query(default=None, description="Filter by sport: nba, nhl, mlb, nfl, cbb, mls, epl, ucl, ufc, soccer. Leave empty for all."),
    moneyline_only: bool = Query(default=False, description="Return only moneyline/winner markets"),
    live_only: bool = Query(default=False, description="Return only games currently in progress"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """
    Returns active game markets for today.
    Important fix: active market existence no longer forces isLive=True.
    """
    if sport and sport.lower() != "all":
        prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
    else:
        prefixes = ALL_SPORT_PREFIXES

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seen_ids: set = set()
    all_markets: List[Dict[str, Any]] = []

    # SOURCE 1: direct /markets
    direct_markets = await fetch_active_markets_by_prefix(prefixes, max_pages=10)
    for m in direct_markets:
        if is_futures_market(m):
            continue
        mm = annotate_market_time_flags(normalize_market(m))
        mid = str(mm.get("id") or mm.get("slug") or "")
        if not mid or mid in seen_ids:
            continue
        if live_only and not mm.get("isLive"):
            continue
        if not mm.get("isToday") and str(mm.get("gameStartTime") or mm.get("startDate") or "")[:10] > today_str:
            continue
        seen_ids.add(mid)
        all_markets.append(mm)

    # SOURCE 2: /events for the full slate
    all_events = await fetch_all_active_events(max_pages=5)
    for event in all_events:
        event_slug = (event.get("slug") or "").lower()
        event_start = str(event.get("startDate") or event.get("startDateIso") or "")[:10]

        if event_start and event_start > today_str:
            continue

        end = parse_dt(event.get("endDate") or event.get("endDateIso"))
        if end:
            hours_since_end = (datetime.now(timezone.utc) - end).total_seconds() / 3600
            if hours_since_end > 2:
                continue

        if not any(event_slug.startswith(p) for p in prefixes):
            continue
        if is_futures_market(event):
            continue

        for m in event_to_markets(event):
            mm = annotate_market_time_flags(normalize_market(m))
            if is_futures_market(mm):
                continue
            if live_only and not mm.get("isLive"):
                continue
            mid = str(mm.get("id") or mm.get("slug") or "")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                all_markets.append(mm)

    if moneyline_only:
        all_markets = [m for m in all_markets if is_moneyline_market(m)]

    all_markets.sort(key=liquidity_key, reverse=True)

    return {
        "date": today_str,
        "sport": sport or "all",
        "prefixes_searched": prefixes,
        "live_only": live_only,
        "moneyline_only": moneyline_only,
        "count": len(all_markets[:limit]),
        "markets": all_markets[:limit],
    }

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict
from datetime import datetime, timezone
import httpx
import json

app = FastAPI(
    title="Polymarket GPT API",
    version="5.1.0",
    description="Read-only API for Polymarket trade analysis using Gamma, CLOB, and Data APIs"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"
DATA_BASE  = "https://data-api.polymarket.com"

# Confirmed Polymarket slug prefixes by sport
# These are derived from real Polymarket URLs and are the most reliable search terms
SLUG_PREFIXES: Dict[str, List[str]] = {
    "nba":    ["nba-"],
    "nhl":    ["nhl-"],
    "mlb":    ["mlb-"],
    "nfl":    ["nfl-"],
    "cbb":    ["cbb-"],
    "mls":    ["mls-"],
    "epl":    ["epl-"],
    "ucl":    ["ucl-"],
    "ufc":    ["ufc-"],
    "soccer": ["epl-", "ucl-", "mls-", "liga-", "seri-", "bund-"],
    "all":    ["nba-", "nhl-", "mlb-", "nfl-", "cbb-", "mls-", "epl-", "ucl-", "ufc-"],
}

# All slug prefixes to search when no sport filter is given
ALL_SPORT_PREFIXES = ["nba-", "nhl-", "mlb-", "nfl-", "cbb-", "mls-", "epl-", "ucl-", "ufc-"]

# Sports market types that are game markets
GAME_MARKET_TYPES = {
    "moneyline", "winner", "match_winner",
    "spreads", "first_half_spreads", "second_half_spreads",
    "totals", "first_half_totals", "second_half_totals",
    "player_props", "team_props"
}

# Sports market types that are futures
FUTURES_MARKET_TYPES = {
    "outright_winner", "futures", "season_wins",
    "playoff_odds", "championship"
}


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

async def fetch_json(base: str, path: str, params: Optional[dict] = None) -> Any:
    url = f"{base}{path}"
    async with httpx.AsyncClient(timeout=25.0) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code,
                                detail=f"Upstream error from {url}: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Request failed for {url}: {str(e)}")


def parse_possible_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def extract_token_ids(m: Dict[str, Any]) -> List[str]:
    candidates = [
        m.get("clobTokenIds"),
        m.get("tokenIds"),
        m.get("outcomeTokenIds"),
        m.get("tokens"),
    ]
    token_ids: List[str] = []
    for candidate in candidates:
        parsed = parse_possible_json(candidate)
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, str):
                    token_ids.append(item)
                elif isinstance(item, dict):
                    for key in ["tokenId", "token_id", "id", "asset_id"]:
                        if item.get(key):
                            token_ids.append(str(item[key]))
    seen = set()
    result = []
    for tid in token_ids:
        if tid not in seen:
            seen.add(tid)
            result.append(tid)
    return result


def is_recent_market(m: Dict[str, Any]) -> bool:
    """Return False if the market end date is in the past."""
    end_date = m.get("endDate") or m.get("endDateIso")
    if not end_date:
        return True
    try:
        if isinstance(end_date, str):
            end_date = end_date.replace("Z", "+00:00")
            dt = datetime.fromisoformat(end_date)
            return dt > datetime.now(timezone.utc)
    except Exception:
        return True
    return True


def is_tradeable(m: Dict[str, Any]) -> bool:
    """Return True only if market is active, not closed, and not expired."""
    if m.get("closed"):
        return False
    if not m.get("active"):
        return False
    if not is_recent_market(m):
        return False
    return True


def is_futures_market(m: Dict[str, Any]) -> bool:
    """Detect futures markets using sportsMarketType first, then keyword fallback."""
    smt = m.get("sportsMarketType")
    if smt and smt in FUTURES_MARKET_TYPES:
        return True
    if smt and smt in GAME_MARKET_TYPES:
        return False
    blob = text_blob(m)
    futures_terms = [
        "finals", "championship", "champion", "stanley cup", "super bowl",
        "world series", "conference finals", "title", "mvp",
        "to win the 2026", "to win the nba finals", "season wins",
        "to win the", "most wins", "win the nba", "win the nfl",
        "win the mlb", "win the nhl", "reach the finals", "make the playoffs",
        "to win the 2025", "ncaa tournament", "march madness champion",
        "to win the 2027",
    ]
    return any(term in blob for term in futures_terms)


def is_game_market(m: Dict[str, Any]) -> bool:
    smt = m.get("sportsMarketType")
    if smt and smt in GAME_MARKET_TYPES:
        return True
    if smt and smt in FUTURES_MARKET_TYPES:
        return False
    blob = text_blob(m)
    game_terms = [
        "vs", "v.", "tonight", "today", "game", "match", "winner",
        "moneyline", "spread", "over/under", "will win", "beat", "defeat",
        "1h", "2h", "first half", "second half", "total", "o/u"
    ]
    return any(term in blob for term in game_terms) and not is_futures_market(m)


def is_moneyline_market(m: Dict[str, Any]) -> bool:
    smt = m.get("sportsMarketType")
    if smt:
        return smt in {"moneyline", "winner", "match_winner"}
    q = (m.get("question") or "").lower()
    return "moneyline" in q or (
        "vs" in q and
        "spread" not in q and
        "o/u" not in q and
        "total" not in q and
        "1h" not in q and
        "2h" not in q
    )


def has_sport_slug_prefix(m: Dict[str, Any], prefixes: List[str]) -> bool:
    """Check if a market slug starts with any of the given sport prefixes."""
    slug = (m.get("slug") or "").lower()
    event_slug = (m.get("eventSlug") or "").lower()
    return any(slug.startswith(p) or event_slug.startswith(p) for p in prefixes)


def normalize_market(m: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": m.get("id"),
        "question": m.get("question"),
        "slug": m.get("slug"),
        "category": m.get("category"),
        "endDate": m.get("endDate"),
        "active": m.get("active"),
        "closed": m.get("closed"),
        "liquidity": m.get("liquidity") or m.get("liquidityNum") or m.get("liquidityClob"),
        "volume": m.get("volume") or m.get("volumeNum"),
        "description": m.get("description"),
        "image": m.get("image"),
        "outcomes": parse_possible_json(m.get("outcomes")),
        "outcomePrices": parse_possible_json(m.get("outcomePrices")),
        "tokenIds": extract_token_ids(m),
        "rawConditionId": m.get("conditionId"),
        "sportsMarketType": m.get("sportsMarketType"),
        "bestBid": m.get("bestBid"),
        "bestAsk": m.get("bestAsk"),
        "lastTradePrice": m.get("lastTradePrice"),
        "spread": m.get("spread"),
        "gameStartTime": m.get("gameStartTime"),
        "isGameMarket": is_game_market(m),
        "isFuturesMarket": is_futures_market(m),
        "isMoneyline": is_moneyline_market(m),
    }


def text_blob(m: Dict[str, Any]) -> str:
    return " ".join([
        str(m.get("question", "")),
        str(m.get("slug", "")),
        str(m.get("category", "")),
        str(m.get("description", "")),
        str(m.get("eventTitle", "")),
    ]).lower()


def liquidity_key(m: Dict[str, Any]) -> float:
    for key in ("liquidity", "liquidityNum", "liquidityClob"):
        try:
            v = m.get(key)
            if v is not None:
                return float(v)
        except Exception:
            continue
    return 0.0


def volume_key(m: Dict[str, Any]) -> float:
    for key in ("volume", "volumeNum", "volume24hr"):
        try:
            v = m.get(key)
            if v is not None:
                return float(v)
        except Exception:
            continue
    return 0.0


def yes_price_from_market(m: Dict[str, Any]) -> Optional[float]:
    for key in ("lastTradePrice", "bestAsk"):
        try:
            v = m.get(key)
            if v is not None:
                return float(v)
        except Exception:
            continue
    prices = parse_possible_json(m.get("outcomePrices"))
    if isinstance(prices, list) and prices:
        try:
            return float(prices[0])
        except Exception:
            return None
    return None


def extreme_price_penalty(m: Dict[str, Any]) -> float:
    p = yes_price_from_market(m)
    if p is None:
        return 0.0
    if p > 0.90 or p < 0.10:
        return -30.0
    if p > 0.85 or p < 0.15:
        return -12.0
    return 0.0


def extract_markets_from_search(
    search_res: Dict[str, Any],
    game_markets_only: bool = False,
    exclude_futures: bool = False,
    slug_prefixes: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    Flattens events and top-level markets from public-search response.
    Filters closed/expired markets. Optionally filters by game type, futures, or slug prefix.
    """
    seen_ids: set = set()
    found: List[Dict[str, Any]] = []

    def should_include(m: Dict[str, Any]) -> bool:
        if not is_tradeable(m):
            return False
        if game_markets_only and not is_game_market(m):
            return False
        if exclude_futures and is_futures_market(m):
            return False
        if slug_prefixes and not has_sport_slug_prefix(m, slug_prefixes):
            return False
        return True

    for m in search_res.get("markets", []):
        if not should_include(m):
            continue
        mid = str(m.get("id") or m.get("slug") or "")
        if mid and mid not in seen_ids:
            seen_ids.add(mid)
            found.append(m)

    for event in search_res.get("events", []):
        event_category = event.get("category", "")
        event_title    = event.get("title", "")
        event_slug     = event.get("slug", "")
        for m in event.get("markets", []):
            # Inject event slug so has_sport_slug_prefix can check it
            if not m.get("eventSlug"):
                m["eventSlug"] = event_slug
            if not should_include(m):
                continue
            mid = str(m.get("id") or m.get("slug") or "")
            if mid and mid not in seen_ids:
                seen_ids.add(mid)
                if not m.get("category"):
                    m["category"] = event_category
                if not m.get("eventTitle"):
                    m["eventTitle"] = event_title
                if not m.get("liquidity") and not m.get("liquidityNum"):
                    m["liquidity"] = event.get("liquidity")
                if not m.get("volume") and not m.get("volumeNum"):
                    m["volume"] = event.get("volume")
                found.append(m)

    return found


TEAM_ALIASES: Dict[str, List[str]] = {
    # NBA
    "lakers":        ["lakers", "los angeles lakers", "la lakers"],
    "timberwolves":  ["timberwolves", "wolves", "minnesota timberwolves", "min"],
    "celtics":       ["celtics", "boston celtics"],
    "knicks":        ["knicks", "new york knicks"],
    "warriors":      ["warriors", "golden state warriors"],
    "nuggets":       ["nuggets", "denver nuggets"],
    "thunder":       ["thunder", "oklahoma city thunder", "okc thunder"],
    "mavericks":     ["mavericks", "dallas mavericks", "mavs"],
    "bucks":         ["bucks", "milwaukee bucks"],
    "heat":          ["heat", "miami heat"],
    "sixers":        ["76ers", "sixers", "philadelphia 76ers"],
    "spurs":         ["spurs", "san antonio spurs"],
    "clippers":      ["clippers", "la clippers", "los angeles clippers"],
    "suns":          ["suns", "phoenix suns"],
    "bulls":         ["bulls", "chicago bulls"],
    "pistons":       ["pistons", "detroit pistons"],
    "grizzlies":     ["grizzlies", "memphis grizzlies"],
    "pelicans":      ["pelicans", "new orleans pelicans"],
    "pacers":        ["pacers", "indiana pacers"],
    "hawks":         ["hawks", "atlanta hawks"],
    "nets":          ["nets", "brooklyn nets"],
    "raptors":       ["raptors", "toronto raptors"],
    "magic":         ["magic", "orlando magic"],
    "cavaliers":     ["cavaliers", "cavs", "cleveland cavaliers"],
    "wizards":       ["wizards", "washington wizards"],
    "kings":         ["kings", "sacramento kings"],
    "jazz":          ["jazz", "utah jazz"],
    "rockets":       ["rockets", "houston rockets"],
    "trail blazers": ["trail blazers", "blazers", "portland trail blazers"],
    "hornets":       ["hornets", "charlotte hornets"],
    # NFL
    "chiefs":        ["chiefs", "kansas city chiefs"],
    "eagles":        ["eagles", "philadelphia eagles"],
    "patriots":      ["patriots", "new england patriots"],
    "cowboys":       ["cowboys", "dallas cowboys"],
    "49ers":         ["49ers", "san francisco 49ers", "niners"],
    "ravens":        ["ravens", "baltimore ravens"],
    "bills":         ["bills", "buffalo bills"],
    "bengals":       ["bengals", "cincinnati bengals"],
    "steelers":      ["steelers", "pittsburgh steelers"],
    "packers":       ["packers", "green bay packers"],
    "bears":         ["bears", "chicago bears"],
    "giants":        ["giants", "new york giants"],
    "jets":          ["jets", "new york jets"],
    "dolphins":      ["dolphins", "miami dolphins"],
    "broncos":       ["broncos", "denver broncos"],
    "raiders":       ["raiders", "las vegas raiders"],
    "chargers":      ["chargers", "los angeles chargers"],
    "seahawks":      ["seahawks", "seattle seahawks"],
    "rams":          ["rams", "los angeles rams"],
    "cardinals":     ["cardinals", "arizona cardinals"],
    "saints":        ["saints", "new orleans saints"],
    "buccaneers":    ["buccaneers", "bucs", "tampa bay buccaneers"],
    "falcons":       ["falcons", "atlanta falcons"],
    "panthers":      ["panthers", "carolina panthers"],
    "vikings":       ["vikings", "minnesota vikings"],
    "lions":         ["lions", "detroit lions"],
    "colts":         ["colts", "indianapolis colts"],
    "jaguars":       ["jaguars", "jacksonville jaguars"],
    "titans":        ["titans", "tennessee titans"],
    "texans":        ["texans", "houston texans"],
    "browns":        ["browns", "cleveland browns"],
    # MLB
    "yankees":       ["yankees", "new york yankees"],
    "dodgers":       ["dodgers", "los angeles dodgers"],
    "red sox":       ["red sox", "boston red sox"],
    "cubs":          ["cubs", "chicago cubs"],
    "mets":          ["mets", "new york mets"],
    "braves":        ["braves", "atlanta braves"],
    "astros":        ["astros", "houston astros"],
    "giants sf":     ["giants", "san francisco giants"],
    "padres":        ["padres", "san diego padres"],
    "phillies":      ["phillies", "philadelphia phillies"],
    # NHL
    "rangers":        ["rangers", "new york rangers"],
    "bruins":         ["bruins", "boston bruins"],
    "maple leafs":    ["maple leafs", "toronto maple leafs", "leafs"],
    "blackhawks":     ["blackhawks", "chicago blackhawks"],
    "penguins":       ["penguins", "pittsburgh penguins"],
    "capitals":       ["capitals", "washington capitals", "caps"],
    "lightning":      ["lightning", "tampa bay lightning"],
    "avalanche":      ["avalanche", "colorado avalanche", "avs"],
    "golden knights": ["golden knights", "vegas golden knights"],
    "oilers":         ["oilers", "edmonton oilers"],
    "flames":         ["flames", "calgary flames"],
    "canucks":        ["canucks", "vancouver canucks"],
    "hurricanes":     ["hurricanes", "carolina hurricanes", "canes"],
    "islanders":      ["islanders", "new york islanders"],
    "blues":          ["blues", "st. louis blues"],
    "predators":      ["predators", "nashville predators", "preds"],
    "winnipeg jets":  ["jets", "winnipeg jets"],
    "wild":           ["wild", "minnesota wild"],
    "red wings":      ["red wings", "detroit red wings"],
    "sabres":         ["sabres", "buffalo sabres"],
    "senators":       ["senators", "ottawa senators"],
    "canadiens":      ["canadiens", "montreal canadiens", "habs"],
    "sharks":         ["sharks", "san jose sharks"],
    "ducks":          ["ducks", "anaheim ducks"],
    "kings nhl":      ["kings", "los angeles kings"],
    "panthers nhl":   ["panthers", "florida panthers"],
    "kraken":         ["kraken", "seattle kraken"],
    "blue jackets":   ["blue jackets", "columbus blue jackets"],
    # Soccer
    "manchester city":   ["manchester city", "man city"],
    "arsenal":           ["arsenal"],
    "liverpool":         ["liverpool"],
    "chelsea":           ["chelsea"],
    "real madrid":       ["real madrid"],
    "barcelona":         ["barcelona", "barca"],
    "manchester united": ["manchester united", "man united", "man utd"],
    "tottenham":         ["tottenham", "spurs", "tottenham hotspur"],
    "bayern munich":     ["bayern munich", "bayern"],
    "psg":               ["psg", "paris saint-germain", "paris saint germain"],
    "juventus":          ["juventus", "juve"],
    "inter milan":       ["inter milan", "inter"],
    "ac milan":          ["ac milan", "milan"],
    "atletico madrid":   ["atletico madrid", "atletico"],
    "dortmund":          ["dortmund", "borussia dortmund", "bvb"],
}


def aliases_for_team(name: str) -> List[str]:
    key = name.lower().strip()
    return TEAM_ALIASES.get(key, [key])


def matches_sport(m: Dict[str, Any], sport: Optional[str]) -> bool:
    if not sport or sport.lower() == "all":
        return True
    prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
    return has_sport_slug_prefix(m, prefixes)


def score_game_candidate(m: Dict[str, Any], team1: str, team2: str) -> float:
    blob  = text_blob(m)
    score = 0.0

    team1_hits = sum(1 for t in aliases_for_team(team1) if t in blob)
    team2_hits = sum(1 for t in aliases_for_team(team2) if t in blob)

    if team1_hits == 0 and team1.lower()[:4] in blob:
        team1_hits = 0.5
    if team2_hits == 0 and team2.lower()[:4] in blob:
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


# ─────────────────────────────────────────
# CORE ENDPOINTS
# ─────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "Polymarket GPT API is live",
        "status": "ok",
        "version": "5.1.0",
        "apis": {"gamma": GAMMA_BASE, "clob": CLOB_BASE, "data": DATA_BASE},
        "supported_sports": list(SLUG_PREFIXES.keys())
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/categories")
def categories():
    return {
        "categories": ["all", "sports", "politics", "crypto", "news", "current-events"],
        "sports": list(SLUG_PREFIXES.keys()),
        "slug_prefixes": SLUG_PREFIXES
    }


@app.get("/public-search")
async def public_search(
    q: str = Query(...),
    limit_per_type: int = Query(default=25, ge=1, le=100),
    page: int = Query(default=1, ge=1),
    events_status: str = Query(default="active")
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
        }
    )


@app.get("/live-games")
async def live_games(
    sport: Optional[str] = Query(default=None, description="Filter by sport: nba, nhl, mlb, nfl, cbb, mls, epl, ucl, ufc, soccer. Leave empty for all."),
    moneyline_only: bool = Query(default=False, description="Return only moneyline/winner markets"),
    limit: int = Query(default=50, ge=1, le=200)
):
    """
    Returns every active game market on Polymarket right now.
    Searches by confirmed slug prefixes (nba-, nhl-, cbb-, epl-, ucl-, mls-, ufc-, etc.)
    No team names needed. Use this as the first call in any board scan.
    """
    # Determine which prefixes to search
    if sport and sport.lower() != "all":
        prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
    else:
        prefixes = ALL_SPORT_PREFIXES

    seen_ids: set = set()
    all_markets: List[Dict[str, Any]] = []

    for prefix in prefixes:
        try:
            search_res = await public_search(
                q=prefix, limit_per_type=50, page=1, events_status="active"
            )
            for m in extract_markets_from_search(
                search_res,
                game_markets_only=True,
                exclude_futures=True,
                slug_prefixes=[prefix]
            ):
                mid = str(m.get("id") or m.get("slug") or "")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)
        except Exception:
            continue

    if moneyline_only:
        all_markets = [m for m in all_markets if is_moneyline_market(m)]

    all_markets.sort(key=liquidity_key, reverse=True)

    return {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "sport": sport or "all",
        "prefixes_searched": prefixes,
        "moneyline_only": moneyline_only,
        "count": len(all_markets[:limit]),
        "markets": [normalize_market(m) for m in all_markets[:limit]]
    }


@app.get("/markets")
async def markets(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):
    raw = await fetch_json(
        GAMMA_BASE, "/markets",
        params={"limit": 200, "active": "true", "closed": "false"}
    )
    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response from Gamma API")

    items = [m for m in raw if is_tradeable(m)]

    if search:
        s = search.lower()
        items = [m for m in items if s in text_blob(m)]

    if category.lower() == "sports" or sport:
        items = [m for m in items if any(
            has_sport_slug_prefix(m, prefixes)
            for prefixes in SLUG_PREFIXES.values()
        )]

    if sport:
        items = [m for m in items if matches_sport(m, sport)]

    if category.lower() not in ("all", "sports"):
        items = [m for m in items if category.lower() in text_blob(m)]

    items = sorted(items, key=lambda m: (liquidity_key(m), volume_key(m)), reverse=True)[:limit]

    return {
        "category": category,
        "sport": sport,
        "search": search,
        "limit": limit,
        "count": len(items),
        "markets": [normalize_market(m) for m in items]
    }


@app.get("/find-market")
async def find_market(
    query: str = Query(...),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50)
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
                    all_markets.append(m)
        except Exception:
            continue

    if sport:
        prefixes = SLUG_PREFIXES.get(sport.lower(), [sport.lower() + "-"])
        all_markets = [m for m in all_markets if has_sport_slug_prefix(m, prefixes)]

    ranked = sorted(
        all_markets,
        key=lambda m: extreme_price_penalty(m) + liquidity_key(m) / 100000.0 + volume_key(m) / 100000.0,
        reverse=True
    )[:limit]

    return {
        "query": query,
        "sport": sport,
        "count": len(ranked),
        "markets": [normalize_market(m) for m in ranked]
    }


@app.get("/find-game")
async def find_game(
    team1: str = Query(...),
    team2: str = Query(...),
    sport: str = Query(default="nba"),
    limit: int = Query(default=10, ge=1, le=30)
):
    def short_name(team: str) -> str:
        parts = team.strip().split()
        return parts[-1] if len(parts) > 1 else team

    t1_short = short_name(team1)
    t2_short = short_name(team2)

    queries = [
        f"{t1_short} vs {t2_short}",
        f"{t2_short} vs {t1_short}",
        f"{t1_short} {t2_short}",
        t1_short,
        t2_short,
    ]

    seen_q: set = set()
    queries = [q for q in queries if not (q in seen_q or seen_q.add(q))]

    seen_ids: set = set()
    all_markets: List[Dict[str, Any]] = []

    for q in queries:
        try:
            search_res = await public_search(q=q, limit_per_type=50, page=1, events_status="active")
            for m in extract_markets_from_search(search_res, exclude_futures=True):
                mid = str(m.get("id") or m.get("slug") or "")
                if mid and mid not in seen_ids:
                    seen_ids.add(mid)
                    all_markets.append(m)
        except Exception:
            continue

    ranked_pairs = [(score_game_candidate(m, team1, team2), m) for m in all_markets]
    ranked_pairs.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, market in ranked_pairs[:limit]:
        nm = normalize_market(market)
        nm["matchScore"] = round(score, 2)
        results.append(nm)

    return {
        "team1": team1,
        "team2": team2,
        "sport": sport,
        "count": len(results),
        "markets": results
    }


@app.get("/find-slug")
async def find_slug(
    team1: str = Query(...),
    team2: str = Query(...),
    sport: str = Query(default="nba")
):
    """Find the actual Polymarket slug for a game. Always call before scanMarket."""
    found = await find_game(team1=team1, team2=team2, sport=sport, limit=5)
    moneyline_markets = [m for m in found["markets"] if m.get("isMoneyline")]
    slugs = [m["slug"] for m in found["markets"] if m.get("slug")]
    return {
        "team1": team1,
        "team2": team2,
        "sport": sport,
        "slugs": slugs,
        "recommended_slug": moneyline_markets[0]["slug"] if moneyline_markets else (slugs[0] if slugs else None),
        "markets": found["markets"]
    }


@app.get("/market-details")
async def market_details(
    id: Optional[str] = Query(default=None),
    slug: Optional[str] = Query(default=None)
):
    if not id and not slug:
        raise HTTPException(status_code=400, detail="Provide either id or slug")

    if id:
        raw = await fetch_json(GAMMA_BASE, f"/markets/{id}")
        return {"lookup": "id", "market": normalize_market(raw)}

    try:
        raw = await fetch_json(GAMMA_BASE, "/markets", params={"slug": slug})
        if isinstance(raw, list) and raw:
            return {"lookup": "slug", "market": normalize_market(raw[0])}
    except Exception:
        pass

    raw = await fetch_json(GAMMA_BASE, "/markets",
                           params={"limit": 300, "active": "true", "closed": "false"})
    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response from Gamma API")

    match = next((m for m in raw if str(m.get("slug", "")) == slug), None)
    if not match:
        raise HTTPException(status_code=404, detail="Market not found for slug")

    return {"lookup": "slug", "market": normalize_market(match)}


@app.get("/market-summary")
async def market_summary(
    query: Optional[str] = Query(default=None),
    slug: Optional[str] = Query(default=None),
    team1: Optional[str] = Query(default=None),
    team2: Optional[str] = Query(default=None),
    sport: Optional[str] = Query(default=None)
):
    chosen_market = None

    if slug:
        chosen_market = (await market_details(slug=slug))["market"]
    elif team1 and team2:
        found = await find_game(team1=team1, team2=team2, sport=sport or "nba", limit=5)
        if found["count"] == 0:
            raise HTTPException(status_code=404, detail="No game market found")
        moneyline = next((m for m in found["markets"] if m.get("isMoneyline")), None)
        chosen_market = moneyline or found["markets"][0]
    elif query:
        found = await find_market(query=query, limit=1)
        if found["count"] == 0:
            raise HTTPException(status_code=404, detail="No market found")
        chosen_market = found["markets"][0]
    else:
        raise HTTPException(status_code=400, detail="Provide slug, query, or team1+team2")

    token_ids = chosen_market.get("tokenIds") or []
    pricing = {}

    if token_ids:
        first_token = token_ids[0]
        try:
            pricing = {
                "price_buy":  await fetch_json(CLOB_BASE, "/price", params={"token_id": first_token, "side": "BUY"}),
                "price_sell": await fetch_json(CLOB_BASE, "/price", params={"token_id": first_token, "side": "SELL"}),
                "midpoint":   await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": first_token}),
                "spread":     await fetch_json(CLOB_BASE, "/spread",   params={"token_id": first_token}),
            }
        except Exception as e:
            pricing = {"error": str(e)}

    return {"market": chosen_market, "pricing": pricing}


@app.get("/scan-market")
async def scan_market(
    slug: Optional[str] = Query(default=None),
    id: Optional[str] = Query(default=None)
):
    if not slug and not id:
        raise HTTPException(status_code=400, detail="Provide either slug or id")

    details = await market_details(id=id, slug=slug)
    chosen_market = details["market"]
    token_ids = chosen_market.get("tokenIds") or []
    outcomes = chosen_market.get("outcomes") or []

    all_pricing = []

    for i, token_id in enumerate(token_ids):
        label = outcomes[i] if i < len(outcomes) else f"Token {i}"
        result: Dict[str, Any] = {"outcome": label, "token_id": token_id}
        errors = []

        try:
            result["price_buy"] = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "BUY"})
        except Exception as e:
            errors.append(f"price_buy: {str(e)}")

        try:
            result["price_sell"] = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": "SELL"})
        except Exception as e:
            errors.append(f"price_sell: {str(e)}")

        try:
            result["midpoint"] = await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})
        except Exception as e:
            errors.append(f"midpoint: {str(e)}")

        try:
            result["spread"] = await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})
        except Exception as e:
            errors.append(f"spread: {str(e)}")

        try:
            book = await fetch_json(CLOB_BASE, "/book", params={"token_id": token_id})
            result["book_summary"] = {
                "best_bid":  book.get("bids", [{}])[0] if book.get("bids") else None,
                "best_ask":  book.get("asks", [{}])[0] if book.get("asks") else None,
                "bid_depth": len(book.get("bids", [])),
                "ask_depth": len(book.get("asks", [])),
            }
        except Exception as e:
            errors.append(f"book: {str(e)}")

        if errors:
            result["errors"] = errors

        all_pricing.append(result)

    return {
        "market": chosen_market,
        "clob_pricing": all_pricing
    }


@app.get("/best-opportunities")
async def best_opportunities(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20),
    min_price: float = Query(default=0.10, ge=0.0, le=0.5),
    max_price: float = Query(default=0.90, ge=0.5, le=1.0)
):
    data = await markets(category=category, sport=sport, search=None, limit=100)
    items = data["markets"]

    filtered = []
    for m in items:
        p = yes_price_from_market(m)
        if p is not None and (p > max_price or p < min_price):
            continue
        filtered.append(m)

    ranked = sorted(filtered, key=lambda m: (liquidity_key(m), volume_key(m)), reverse=True)[:limit]

    return {
        "category": category,
        "sport": sport,
        "limit": limit,
        "min_price": min_price,
        "max_price": max_price,
        "count": len(ranked),
        "message": "Top active markets ranked by liquidity and volume",
        "opportunities": ranked
    }


# ─────────────────────────────────────────
# CLOB ENDPOINTS
# ─────────────────────────────────────────

@app.get("/clob/price")
async def clob_price(
    token_id: str = Query(...),
    side: str = Query(default="BUY", description="BUY or SELL")
):
    return await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id, "side": side})


@app.get("/clob/prices")
async def clob_prices(
    token_ids: str = Query(..., description="Comma-separated token ids"),
    side: str = Query(default="BUY", description="BUY or SELL")
):
    ids = [t.strip() for t in token_ids.split(",") if t.strip()]
    results = {}
    for tid in ids:
        try:
            results[tid] = await fetch_json(CLOB_BASE, "/price", params={"token_id": tid, "side": side})
        except Exception as e:
            results[tid] = {"error": str(e)}
    return {"token_ids": ids, "side": side, "prices": results}


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
    fidelity: int = Query(default=10, description="Number of data points")
):
    return await fetch_json(CLOB_BASE, "/prices-history",
                            params={"market": token_id, "interval": interval, "fidelity": fidelity})


@app.get("/price-check")
async def price_check(token_id: str = Query(...)):
    price_buy  = await fetch_json(CLOB_BASE, "/price",    params={"token_id": token_id, "side": "BUY"})
    price_sell = await fetch_json(CLOB_BASE, "/price",    params={"token_id": token_id, "side": "SELL"})
    midpoint   = await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})
    spread     = await fetch_json(CLOB_BASE, "/spread",   params={"token_id": token_id})
    return {
        "token_id":   token_id,
        "price_buy":  price_buy,
        "price_sell": price_sell,
        "midpoint":   midpoint,
        "spread":     spread
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
    minBalance: int = Query(default=1, ge=1)
):
    params = {"limit": limit, "minBalance": minBalance}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/holders", params=params)


@app.get("/data/trades")
async def data_trades(
    market: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200)
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

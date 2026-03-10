from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict, Tuple
import httpx
import json

app = FastAPI(
    title="Polymarket GPT API",
    version="4.0.0",
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
CLOB_BASE = "https://clob.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"


async def fetch_json(base: str, path: str, params: Optional[dict] = None) -> Any:
    url = f"{base}{path}"
    async with httpx.AsyncClient(timeout=25.0) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Upstream API error from {url}: {e.response.text}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Request failed for {url}: {str(e)}"
            )


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


def normalize_market(m: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": m.get("id"),
        "question": m.get("question"),
        "slug": m.get("slug"),
        "category": m.get("category"),
        "endDate": m.get("endDate"),
        "active": m.get("active"),
        "closed": m.get("closed"),
        "liquidity": m.get("liquidity"),
        "volume": m.get("volume"),
        "description": m.get("description"),
        "image": m.get("image"),
        "outcomes": parse_possible_json(m.get("outcomes")),
        "outcomePrices": parse_possible_json(m.get("outcomePrices")),
        "tokenIds": extract_token_ids(m),
        "rawConditionId": m.get("conditionId"),
    }


def text_blob(m: Dict[str, Any]) -> str:
    return " ".join([
        str(m.get("question", "")),
        str(m.get("slug", "")),
        str(m.get("category", "")),
        str(m.get("description", "")),
    ]).lower()


def liquidity_key(m: Dict[str, Any]) -> float:
    try:
        return float(m.get("liquidity") or 0)
    except Exception:
        return 0.0


def volume_key(m: Dict[str, Any]) -> float:
    try:
        return float(m.get("volume") or 0)
    except Exception:
        return 0.0


def sport_terms_for(sport: str) -> List[str]:
    sport = sport.lower()
    aliases = {
        "nba": ["nba", "basketball", "western conference", "eastern conference"],
        "mls": ["mls", "soccer", "major league soccer"],
        "nfl": ["nfl", "football", "super bowl"],
        "mlb": ["mlb", "baseball", "world series"],
        "nhl": ["nhl", "hockey", "stanley cup"],
        "soccer": ["soccer", "football", "fifa", "uefa", "champions league", "world cup"],
    }
    return aliases.get(sport, [sport])


TEAM_ALIASES = {
    "lakers": ["lakers", "los angeles lakers", "la lakers"],
    "timberwolves": ["timberwolves", "wolves", "minnesota timberwolves", "minnesota wolves"],
    "celtics": ["celtics", "boston celtics"],
    "knicks": ["knicks", "new york knicks"],
    "warriors": ["warriors", "golden state warriors"],
    "nuggets": ["nuggets", "denver nuggets"],
    "thunder": ["thunder", "oklahoma city thunder", "okc thunder"],
    "mavericks": ["mavericks", "dallas mavericks", "mavs"],
    "bucks": ["bucks", "milwaukee bucks"],
    "heat": ["heat", "miami heat"],
    "sixers": ["76ers", "sixers", "philadelphia 76ers"],
    "spurs": ["spurs", "san antonio spurs"],
    "clippers": ["clippers", "la clippers", "los angeles clippers"],
    "suns": ["suns", "phoenix suns"],
    "bulls": ["bulls", "chicago bulls"],
    "pistons": ["pistons", "detroit pistons"],
    "grizzlies": ["grizzlies", "memphis grizzlies"],
    "pelicans": ["pelicans", "new orleans pelicans"],
    "panthers": ["panthers", "florida panthers"],
    "bruins": ["bruins", "boston bruins"],
    "rangers": ["rangers", "new york rangers"],
    "oilers": ["oilers", "edmonton oilers"],
}


FUTURES_TERMS = [
    "finals",
    "championship",
    "champion",
    "stanley cup",
    "super bowl",
    "world series",
    "conference finals",
    "to win the 2026",
    "to win the 2025",
    "to win the nba finals",
    "to win the stanley cup",
    "season wins",
    "title",
    "mvp",
]


GAME_TERMS = [
    "vs",
    "v.",
    "tonight",
    "today",
    "game",
    "match",
    "winner",
    "moneyline",
    "spread",
    "over/under",
]


def aliases_for_team(name: str) -> List[str]:
    key = name.lower().strip()
    return TEAM_ALIASES.get(key, [key])


def is_probable_futures_market(m: Dict[str, Any]) -> bool:
    blob = text_blob(m)
    return any(term in blob for term in FUTURES_TERMS)


def is_probable_game_market(m: Dict[str, Any]) -> bool:
    blob = text_blob(m)
    return any(term in blob for term in GAME_TERMS) and not is_probable_futures_market(m)


def score_general_market(m: Dict[str, Any], search: Optional[str], sport: Optional[str], category: str) -> float:
    blob = text_blob(m)
    score = 0.0

    score += liquidity_key(m) / 100000.0
    score += volume_key(m) / 100000.0

    if search:
        for term in search.lower().split():
            if term in blob:
                score += 4.0

    if sport:
        for term in sport_terms_for(sport):
            if term in blob:
                score += 3.0

    if category.lower() == "sports":
        if any(term in blob for term in ["nba", "nfl", "mlb", "nhl", "mls", "soccer", "basketball", "football", "baseball", "hockey"]):
            score += 4.0

    if is_probable_game_market(m):
        score += 8.0

    if is_probable_futures_market(m):
        score -= 6.0

    return score


def score_game_market(m: Dict[str, Any], team1: str, team2: str, sport: Optional[str]) -> float:
    blob = text_blob(m)
    score = 0.0

    team1_terms = aliases_for_team(team1)
    team2_terms = aliases_for_team(team2)

    team1_hits = sum(1 for t in team1_terms if t in blob)
    team2_hits = sum(1 for t in team2_terms if t in blob)

    score += team1_hits * 12.0
    score += team2_hits * 12.0

    if team1_hits > 0 and team2_hits > 0:
        score += 30.0

    if any(term in blob for term in ["vs", "v.", "winner", "moneyline", "spread", "game", "match", "tonight", "today"]):
        score += 12.0

    if is_probable_game_market(m):
        score += 10.0

    if is_probable_futures_market(m):
        score -= 25.0

    if sport:
        for term in sport_terms_for(sport):
            if term in blob:
                score += 2.0

    score += liquidity_key(m) / 100000.0
    score += volume_key(m) / 100000.0

    return score


@app.get("/")
def root():
    return {
        "message": "Polymarket GPT API is live",
        "status": "ok",
        "apis": {
            "gamma": GAMMA_BASE,
            "clob": CLOB_BASE,
            "data": DATA_BASE,
        }
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/categories")
def categories():
    return {
        "categories": [
            "all",
            "sports",
            "politics",
            "crypto",
            "news",
            "current-events"
        ],
        "sports": [
            "nba",
            "mls",
            "nfl",
            "mlb",
            "nhl",
            "soccer",
            "other"
        ]
    }


# -------------------------
# GAMMA: discovery
# -------------------------

@app.get("/markets")
async def markets(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):
    raw = await fetch_json(
        GAMMA_BASE,
        "/markets",
        params={"limit": 300, "active": "true", "closed": "false"}
    )

    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    markets_list = raw

    if search:
        s = search.lower()
        markets_list = [m for m in markets_list if s in text_blob(m)]

    if sport:
        terms = sport_terms_for(sport)
        markets_list = [
            m for m in markets_list
            if any(term in text_blob(m) for term in terms)
        ]

    if category and category.lower() != "all":
        c = category.lower()
        if c == "sports":
            sports_terms = [
                "nba", "nfl", "mlb", "nhl", "mls", "soccer",
                "basketball", "football", "baseball", "hockey",
                "champion", "match", "game", "playoff", "stanley cup",
                "world cup", "finals", "super bowl"
            ]
            markets_list = [
                m for m in markets_list
                if any(term in text_blob(m) for term in sports_terms)
            ]
        else:
            markets_list = [m for m in markets_list if c in text_blob(m)]

    ranked = sorted(
        markets_list,
        key=lambda m: score_general_market(m, search, sport, category),
        reverse=True
    )

    cleaned = [normalize_market(m) for m in ranked[:limit]]

    return {
        "category": category,
        "sport": sport,
        "search": search,
        "limit": limit,
        "count": len(cleaned),
        "markets": cleaned
    }


@app.get("/find-market")
async def find_market(
    query: str = Query(..., description="Natural-language search like 'lakers', 'stanley cup', 'bitcoin'"),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50)
):
    return await markets(category="all", sport=sport, search=query, limit=limit)


@app.get("/find-game")
async def find_game(
    team1: str = Query(..., description="First team, e.g. lakers"),
    team2: str = Query(..., description="Second team, e.g. timberwolves"),
    sport: str = Query(default="nba"),
    limit: int = Query(default=10, ge=1, le=30)
):
    raw = await fetch_json(
        GAMMA_BASE,
        "/markets",
        params={"limit": 300, "active": "true", "closed": "false"}
    )

    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    filtered = []
    for m in raw:
        score = score_game_market(m, team1, team2, sport)
        if score > 0:
            filtered.append((score, m))

    filtered.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, market in filtered[:limit]:
        item = normalize_market(market)
        item["matchScore"] = round(score, 2)
        item["isProbableGameMarket"] = is_probable_game_market(market)
        item["isProbableFuturesMarket"] = is_probable_futures_market(market)
        results.append(item)

    return {
        "team1": team1,
        "team2": team2,
        "sport": sport,
        "count": len(results),
        "markets": results
    }


@app.get("/market-details")
async def market_details(
    id: Optional[str] = Query(default=None, description="Polymarket market id"),
    slug: Optional[str] = Query(default=None, description="Unique market slug")
):
    if not id and not slug:
        raise HTTPException(status_code=400, detail="Provide either id or slug")

    if id:
        raw = await fetch_json(GAMMA_BASE, f"/markets/{id}")
        return {
            "lookup": "id",
            "market": normalize_market(raw)
        }

    raw = await fetch_json(
        GAMMA_BASE,
        "/markets",
        params={"limit": 300, "active": "true", "closed": "false"}
    )
    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    match = next((m for m in raw if str(m.get("slug", "")) == slug), None)
    if not match:
        raise HTTPException(status_code=404, detail="Market not found for given slug")

    return {
        "lookup": "slug",
        "market": normalize_market(match)
    }


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
        details = await market_details(slug=slug)
        chosen_market = details["market"]

    elif team1 and team2:
        found = await find_game(team1=team1, team2=team2, sport=sport or "nba", limit=1)
        if found["count"] == 0:
            raise HTTPException(status_code=404, detail="No likely game market found")
        chosen_market = found["markets"][0]

    elif query:
        found = await find_market(query=query, sport=sport, limit=1)
        if found["count"] == 0:
            raise HTTPException(status_code=404, detail="No market found")
        chosen_market = found["markets"][0]

    else:
        raise HTTPException(status_code=400, detail="Provide slug, query, or team1/team2")

    token_ids = chosen_market.get("tokenIds") or []
    pricing = {}

    if token_ids:
        first_token = token_ids[0]
        try:
            pricing = {
                "price": await fetch_json(CLOB_BASE, "/price", params={"token_id": first_token}),
                "midpoint": await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": first_token}),
                "spread": await fetch_json(CLOB_BASE, "/spread", params={"token_id": first_token}),
            }
        except Exception as e:
            pricing = {"error": str(e)}

    return {
        "market": chosen_market,
        "pricing": pricing
    }


# -------------------------
# CLOB: pricing
# -------------------------

@app.get("/clob/price")
async def clob_price(token_id: str = Query(...)):
    return await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id})


@app.get("/clob/prices")
async def clob_prices(token_ids: str = Query(..., description="Comma-separated token ids")):
    ids = [x.strip() for x in token_ids.split(",") if x.strip()]
    return await fetch_json(CLOB_BASE, "/prices", params={"token_ids": ids})


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
    interval: str = Query(default="1d"),
    fidelity: int = Query(default=60)
):
    return await fetch_json(
        CLOB_BASE,
        "/prices-history",
        params={
            "market": token_id,
            "interval": interval,
            "fidelity": fidelity,
        }
    )


@app.get("/price-check")
async def price_check(token_id: str = Query(...)):
    price = await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id})
    midpoint = await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})
    spread = await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})

    return {
        "token_id": token_id,
        "price": price,
        "midpoint": midpoint,
        "spread": spread,
    }


# -------------------------
# DATA API
# -------------------------

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


# -------------------------
# Scanner
# -------------------------

@app.get("/best-opportunities")
async def best_opportunities(
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20)
):
    data = await markets(category=category, sport=sport, search=None, limit=100)
    markets_list = data["markets"]

    ranked = sorted(
        markets_list,
        key=lambda m: (liquidity_key(m), volume_key(m)),
        reverse=True
    )[:limit]

    return {
        "category": category,
        "sport": sport,
        "limit": limit,
        "count": len(ranked),
        "message": "Top active markets ranked by liquidity and volume for GPT-side analysis",
        "opportunities": ranked
    }


@app.get("/scan-market")
async def scan_market(
    slug: Optional[str] = Query(default=None),
    id: Optional[str] = Query(default=None)
):
    details = await market_details(id=id, slug=slug)
    market = details["market"]
    token_ids = market.get("tokenIds") or []

    clob = {}
    if token_ids:
        first_token = token_ids[0]
        try:
            clob = {
                "price": await fetch_json(CLOB_BASE, "/price", params={"token_id": first_token}),
                "midpoint": await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": first_token}),
                "spread": await fetch_json(CLOB_BASE, "/spread", params={"token_id": first_token}),
            }
        except Exception as e:
            clob = {"error": str(e)}

    return {
        "market": market,
        "clob": clob
    }


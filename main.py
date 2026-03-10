from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict
import httpx
import json

app = FastAPI(
    title="Polymarket GPT API",
    version="3.0.0",
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

    # de-dupe while preserving order
    seen = set()
    result = []
    for tid in token_ids:
        if tid not in seen:
            seen.add(tid)
            result.append(tid)
    return result


def text_blob(m: Dict[str, Any]) -> str:
    return " ".join([
        str(m.get("question", "")),
        str(m.get("slug", "")),
        str(m.get("category", "")),
        str(m.get("description", "")),
    ]).lower()


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


def sport_terms_for(sport: str) -> List[str]:
    sport = sport.lower()
    aliases = {
        "nba": ["nba", "basketball", "warriors", "lakers", "celtics", "knicks", "nuggets", "thunder"],
        "mls": ["mls", "soccer", "inter miami", "lafc", "messi"],
        "nfl": ["nfl", "football", "super bowl", "chiefs", "eagles", "cowboys", "bills"],
        "mlb": ["mlb", "baseball", "world series", "yankees", "dodgers", "mets", "braves"],
        "nhl": ["nhl", "hockey", "stanley cup", "panthers", "rangers", "bruins", "oilers"],
        "soccer": ["soccer", "football", "fifa", "uefa", "champions league", "world cup"],
    }
    return aliases.get(sport, [sport])


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


# -------------------------
# GAMMA: market discovery
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
        params={"limit": 200, "active": "true", "closed": "false"}
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

    cleaned = [normalize_market(m) for m in markets_list[:limit]]

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
    query: str = Query(..., description="Natural-language query like 'lakers', 'stanley cup', 'bitcoin'"),
    limit: int = Query(default=10, ge=1, le=50)
):
    data = await markets(category="all", sport=None, search=query, limit=limit)
    return data


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
        params={"limit": 200, "active": "true", "closed": "false"}
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
# CLOB: pricing & orderbook
# -------------------------

@app.get("/clob/price")
async def clob_price(token_id: str = Query(..., description="CLOB token id")):
    return await fetch_json(CLOB_BASE, "/price", params={"token_id": token_id})


@app.get("/clob/prices")
async def clob_prices(token_ids: str = Query(..., description="Comma-separated token ids")):
    ids = [x.strip() for x in token_ids.split(",") if x.strip()]
    return await fetch_json(CLOB_BASE, "/prices", params={"token_ids": ids})


@app.get("/clob/book")
async def clob_book(token_id: str = Query(..., description="CLOB token id")):
    return await fetch_json(CLOB_BASE, "/book", params={"token_id": token_id})


@app.get("/clob/midpoint")
async def clob_midpoint(token_id: str = Query(..., description="CLOB token id")):
    return await fetch_json(CLOB_BASE, "/midpoint", params={"token_id": token_id})


@app.get("/clob/spread")
async def clob_spread(token_id: str = Query(..., description="CLOB token id")):
    return await fetch_json(CLOB_BASE, "/spread", params={"token_id": token_id})


@app.get("/clob/history")
async def clob_history(
    token_id: str = Query(..., description="CLOB token id"),
    interval: str = Query(default="1d", description="History interval, e.g. max, 1w, 1d"),
    fidelity: int = Query(default=60, description="Granularity / fidelity")
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
async def price_check(token_id: str = Query(..., description="CLOB token id")):
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
# DATA API: context / analytics
# -------------------------

@app.get("/data/open-interest")
async def data_open_interest(
    market: Optional[str] = Query(default=None, description="0x-prefixed market identifier")
):
    params = {}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/oi", params=params)


@app.get("/data/holders")
async def data_holders(
    market: Optional[str] = Query(default=None, description="0x-prefixed market identifier"),
    limit: int = Query(default=20, ge=1, le=100),
    minBalance: int = Query(default=1, ge=1)
):
    params = {"limit": limit, "minBalance": minBalance}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/holders", params=params)


@app.get("/data/trades")
async def data_trades(
    market: Optional[str] = Query(default=None, description="0x-prefixed market identifier"),
    limit: int = Query(default=50, ge=1, le=200)
):
    params = {"limit": limit}
    if market:
        params["market"] = market
    return await fetch_json(DATA_BASE, "/trades", params=params)


@app.get("/data/live-volume")
async def data_live_volume(
    event: Optional[str] = Query(default=None, description="Optional event identifier")
):
    params = {}
    if event:
        params["event"] = event
    return await fetch_json(DATA_BASE, "/live-volume", params=params)


# -------------------------
# Combined scanner
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

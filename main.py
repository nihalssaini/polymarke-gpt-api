from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict
import httpx

app = FastAPI(
    title="Polymarket GPT API",
    version="2.0.0",
    description="Read-only API for Polymarket trade analysis across sports, politics, crypto, and current events"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

GAMMA_BASE = "https://gamma-api.polymarket.com"


async def fetch_gamma(path: str, params: Optional[dict] = None) -> Any:
    url = f"{GAMMA_BASE}{path}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Gamma API error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Request failed: {str(e)}")


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
        "outcomes": m.get("outcomes"),
        "outcomePrices": m.get("outcomePrices"),
        "description": m.get("description"),
        "image": m.get("image"),
    }


@app.get("/")
def root():
    return {
        "message": "Polymarket GPT API is live",
        "status": "ok"
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/markets")
async def markets(
    category: str = Query(default="all", description="Category such as sports, politics, crypto, news, all"),
    sport: Optional[str] = Query(default=None, description="Sport such as nba, mls, nfl, mlb"),
    search: Optional[str] = Query(default=None, description="Search term for market title or event"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum number of markets to return")
):
    params = {
        "limit": limit,
        "active": "true",
        "closed": "false"
    }

    raw = await fetch_gamma("/markets", params=params)

    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    markets_list: List[Dict[str, Any]] = raw

    # Filter locally because Gamma category/tag usage can vary by market structure.
    if category and category.lower() != "all":
        markets_list = [
            m for m in markets_list
            if str(m.get("category", "")).lower() == category.lower()
               or category.lower() in str(m.get("question", "")).lower()
        ]

    if sport:
        sport_lower = sport.lower()
        markets_list = [
            m for m in markets_list
            if sport_lower in str(m.get("question", "")).lower()
               or sport_lower in str(m.get("category", "")).lower()
               or sport_lower in str(m.get("slug", "")).lower()
        ]

    if search:
        s = search.lower()
        markets_list = [
            m for m in markets_list
            if s in str(m.get("question", "")).lower()
               or s in str(m.get("slug", "")).lower()
               or s in str(m.get("category", "")).lower()
        ]

    cleaned = [normalize_market(m) for m in markets_list[:limit]]

    return {
        "category": category,
        "sport": sport,
        "search": search,
        "limit": limit,
        "count": len(cleaned),
        "markets": cleaned
    }


@app.get("/market-details")
async def market_details(
    id: Optional[str] = Query(default=None, description="Polymarket market id"),
    slug: Optional[str] = Query(default=None, description="Unique market slug")
):
    if not id and not slug:
        raise HTTPException(status_code=400, detail="Provide either id or slug")

    if id:
        raw = await fetch_gamma(f"/markets/{id}")
        return {
            "lookup": "id",
            "market": normalize_market(raw)
        }

    # slug lookup via list endpoint then filter
    raw = await fetch_gamma("/markets", params={"limit": 100, "active": "true", "closed": "false"})
    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    match = next((m for m in raw if str(m.get("slug", "")) == slug), None)
    if not match:
        raise HTTPException(status_code=404, detail="Market not found for given slug")

    return {
        "lookup": "slug",
        "market": normalize_market(match)
    }


@app.get("/best-opportunities")
async def best_opportunities(
    category: str = Query(default="all", description="Category such as sports, politics, crypto, news, all"),
    sport: Optional[str] = Query(default=None, description="Sport such as nba, mls, nfl, mlb"),
    limit: int = Query(default=5, ge=1, le=20, description="Maximum number of opportunities to return")
):
    # For now this is a scanner, not a true edge model.
    # It pulls live markets and returns the most liquid active ones for GPT-side analysis.
    data = await markets(category=category, sport=sport, search=None, limit=min(limit * 5, 100))
    markets_list = data["markets"]

    def liquidity_key(m: Dict[str, Any]) -> float:
        try:
            return float(m.get("liquidity") or 0)
        except (TypeError, ValueError):
            return 0.0

    ranked = sorted(markets_list, key=liquidity_key, reverse=True)[:limit]

    return {
        "category": category,
        "sport": sport,
        "limit": limit,
        "message": "Top liquid active markets for GPT-side opportunity analysis",
        "opportunities": ranked
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

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any, List, Dict
import httpx

app = FastAPI(
    title="Polymarket GPT API",
    version="2.1.0",
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
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Gamma API error: {e.response.text}"
            )
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Request failed: {str(e)}"
            )


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


def text_blob(m: Dict[str, Any]) -> str:
    return " ".join([
        str(m.get("question", "")),
        str(m.get("slug", "")),
        str(m.get("category", "")),
        str(m.get("description", "")),
    ]).lower()


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
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):
    params = {
        "limit": 100,
        "active": "true",
        "closed": "false"
    }

    raw = await fetch_gamma("/markets", params=params)

    if not isinstance(raw, list):
        raise HTTPException(status_code=502, detail="Unexpected response format from Gamma API")

    markets_list = raw

    if search:
        s = search.lower()
        markets_list = [m for m in markets_list if s in text_blob(m)]

    if sport:
        s = sport.lower()
        sport_aliases = {
            "nba": ["nba", "basketball", "warriors", "lakers", "celtics", "knicks", "nuggets"],
            "mls": ["mls", "soccer", "inter miami"],
            "nfl": ["nfl", "football", "super bowl"],
            "mlb": ["mlb", "baseball", "world series"],
            "nhl": ["nhl", "hockey", "stanley cup"]
        }
        terms = sport_aliases.get(s, [s])
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
                "champion", "match", "game", "playoff"
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
    category: str = Query(default="all"),
    sport: Optional[str] = Query(default=None),
    limit: int = Query(default=5, ge=1, le=20)
):
    data = await markets(category=category, sport=sport, search=None, limit=100)
    markets_list = data["markets"]

    def liquidity_key(m):
        try:
            return float(m.get("liquidity") or 0)
        except Exception:
            return 0.0

    def volume_key(m):
        try:
            return float(m.get("volume") or 0)
        except Exception:
            return 0.0

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

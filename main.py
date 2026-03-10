from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

app = FastAPI(
    title="Polymarket GPT API",
    version="1.0.0",
    description="Read-only API for Polymarket trade analysis across sports, politics, crypto, and current events"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
def markets(
    category: str = Query(default="all", description="Category such as sports, politics, crypto, news, all"),
    sport: Optional[str] = Query(default=None, description="Sport such as nba, mls, nfl, mlb"),
    search: Optional[str] = Query(default=None, description="Search term for market title or event"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum number of markets to return")
):
    return {
        "category": category,
        "sport": sport,
        "search": search,
        "limit": limit,
        "message": "Placeholder markets endpoint",
        "markets": []
    }


@app.get("/best-opportunities")
def best_opportunities(
    category: str = Query(default="all", description="Category such as sports, politics, crypto, news, all"),
    sport: Optional[str] = Query(default=None, description="Sport such as nba, mls, nfl, mlb"),
    limit: int = Query(default=5, ge=1, le=20, description="Maximum number of opportunities to return")
):
    return {
        "category": category,
        "sport": sport,
        "limit": limit,
        "message": "Placeholder best opportunities endpoint",
        "opportunities": []
    }


@app.get("/market-details")
def market_details(
    slug: str = Query(..., description="Unique market slug")
):
    return {
        "slug": slug,
        "message": "Placeholder market details endpoint",
        "market": {}
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

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from market_aggregator import orchestrate_market_scan

app = FastAPI(
    title="Denarii District Market Aggregator API",
    description="Microservice for aggregating world coin pricing via live web scraping.",
    version="1.0.0"
)

# Enable CORS so the Vercel frontend can make cross-origin background fetch requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to e.g. ["https://denarii-district.vercel.app"]
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    """
    Supabase/UptimeRobot 10-minute ping receiver to prevent Render sleep cycles.
    """
    return {"status": "awake", "message": "The Denarii District backend is warm and ready."}

@app.get("/api/scan")
def scan_market(country: str, km: str, nominal: str, year: str):
    """
    Main aggregator endpoint. Evaluates target coin across NGC, Numista, eBay, Okazii and MA-Shops.
    Params:
      country: e.g. "Romania"
      km: e.g. "17.1"
      nominal: e.g. "5 Lei"
      year: e.g. "1881"
    """
    if not all([country, km, nominal, year]):
        raise HTTPException(status_code=400, detail="Missing required parameters: country, km, nominal, year")
        
    try:
        # Run our polymorphic architecture scraper!
        payload = orchestrate_market_scan(country, km, year, nominal)
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregator crash: {str(e)}")

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
import json
import traceback
from datetime import datetime, timezone
from market_aggregator import orchestrate_market_scan

# ==========================================
# SUPABASE SDK INITIALIZATION
# ==========================================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

def get_supabase_client():
    """Lazy-load Supabase client to avoid import errors during local dev without the SDK."""
    try:
        from supabase import create_client
        if SUPABASE_URL and SUPABASE_KEY:
            return create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        print("⚠️  [!] supabase SDK not installed. Running in local-only mode.")
    return None

# ==========================================
# FASTAPI APPLICATION
# ==========================================
app = FastAPI(
    title="Denarii District Market Aggregator API",
    description="Microservice for aggregating world coin pricing via live web scraping.",
    version="2.0.0"
)

# Enable CORS so the Vercel frontend can make cross-origin background fetch requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to ["https://denarii-district.vercel.app"]
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ==========================================
# HEALTH CHECK (Cron Ping Receiver)
# ==========================================
@app.get("/health")
def health_check():
    """
    Supabase/UptimeRobot 10-minute ping receiver to prevent Render sleep cycles.
    """
    return {"status": "awake", "message": "The Denarii District backend is warm and ready."}

# ==========================================
# BACKGROUND SCRAPE WORKER
# ==========================================
def run_and_store_scrape(country: str, km: str, year: str, nominal: str, coin_id: str):
    """
    Heavy background worker. Runs the full 15-30 second WAF-proxied scrape,
    then pushes the JSON payload to Supabase d_price_analysis table.
    """
    try:
        print(f"\n🔧 [BACKGROUND] Starting scrape for coin_id={coin_id}...")
        
        # Run the heavy orchestrator (15-30 seconds)
        payload = orchestrate_market_scan(
            country=country,
            km_num=km,
            target_year=year,
            nominal=nominal
        )

        raw_payload = {
            "active_listings": payload.pop("raw_active_listings", []),
            "sold_listings": payload.pop("raw_sold_listings", [])
        }
        # Push to Supabase
        supabase = get_supabase_client()
        if supabase:
            row = {
                "coin_id": coin_id,
                "payload": payload,
                "raw_payload": raw_payload,
                "scraped_at": datetime.now(timezone.utc).isoformat()
            }
            supabase.table("d_price_analysis").upsert(row).execute()
            print(f"✅ [BACKGROUND] Successfully cached data for {coin_id} in Supabase.")
        else:
            # Fallback: dump to local JSON file if Supabase is not configured
            fallback_file = f"cache_{coin_id}.json"
            with open(fallback_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            print(f"⚠️  [BACKGROUND] Supabase not configured. Saved locally to {fallback_file}")

    except Exception as e:
        print(f"❌ [BACKGROUND] Scrape failed for {coin_id}: {str(e)}")
        traceback.print_exc()

# ==========================================
# ASYNC FIRE-AND-FORGET ENDPOINT (v2)
# ==========================================
@app.get("/api/scan")
def trigger_market_scan(
    country: str,
    km: str,
    nominal: str,
    year: str,
    coin_id: str,
    background_tasks: BackgroundTasks
):
    """
    Asynchronous fire-and-forget endpoint for Vercel integration.
    
    Immediately returns HTTP 202 Accepted and starts the scrape in the background.
    The frontend should poll Supabase d_price_analysis table for the coin_id 
    to detect when data is ready.
    
    Params:
      country: e.g. "Romania"
      km: e.g. "17.1"
      nominal: e.g. "5 Lei"
      year: e.g. "1881"
      coin_id: The unique identifier from f_coins table (required for Supabase FK)
    """
    if not all([country, km, nominal, year, coin_id]):
        raise HTTPException(
            status_code=400,
            detail="Missing required parameters: country, km, nominal, year, coin_id"
        )

    # Queue the heavy scrape to the FastAPI background thread
    background_tasks.add_task(
        run_and_store_scrape,
        country=country,
        km=km,
        year=year,
        nominal=nominal,
        coin_id=coin_id
    )

    # Return IMMEDIATELY (0.1s) to beat Vercel's 10s timeout
    return JSONResponse(
        status_code=202,
        content={
            "status": "scraping_started",
            "coin_id": coin_id,
            "message": f"Market scan for '{nominal} {year}' initiated. Poll Supabase for results."
        }
    )

# ==========================================
# SYNCHRONOUS ENDPOINT (Direct mode, for local testing)
# ==========================================
@app.get("/api/scan/sync")
def scan_market_sync(country: str, km: str, nominal: str, year: str):
    """
    Synchronous endpoint that waits for the full scrape and returns inline.
    Use this for local testing only -- will timeout on Vercel!
    """
    if not all([country, km, nominal, year]):
        raise HTTPException(status_code=400, detail="Missing required parameters: country, km, nominal, year")

    try:
        payload = orchestrate_market_scan(country, km, year, nominal)
        return payload
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregator crash: {str(e)}")

# ==========================================
# UVICORN ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
import asyncio
import re
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Set
from collections import defaultdict

app = FastAPI(title="RW Tips - Esoccer Result Scraper v2.0")

# ====================== CONFIG ======================
MONGO_URI = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client["estrelabet_esoccer"]
matches = db["finished_matches"]

LIVE_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66&catIds=2085,1571,1728,1594,2086,1729,2130"
EVENT_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventDetails?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}&showNonBoosts=false"
TRACKER_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventTrackerInfo?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}"

previous_event_ids = set()

# Cache expandido
live_cache = defaultdict(lambda: {
    "home_score": 0,
    "away_score": 0,
    "home_raw": "",
    "away_raw": "",
    "league": "",
    "last_seen": datetime.now(timezone.utc)
})

# ====================== AUXILIARES ======================
def extract_pure_nick_canonical(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    m = re.search(r'\(([^)]+)\)', raw)
    if m:
        nick = m.group(1).strip().upper()
        if 2 <= len(nick) <= 16 and (nick.isupper() or '_' in nick or any(c.isdigit() for c in nick)):
            return nick
    words = re.findall(r'\b[A-Z0-9_]{2,15}\b', raw.upper())
    return words[-1] if words else raw.strip().upper()[:15]

def map_league_name(name: str) -> str:
    # Cole aqui sua função completa de mapeamento
    if "H2H" in name.upper():
        return "H2H 8 MIN"
    if "BATTLE" in name.upper():
        return "BATTLE 8 MIN"
    if "GT" in name.upper():
        return "GT LEAGUE 12 MIN"
    return name or "UNKNOWN"

# ====================== FETCH ======================
async def fetch_event_details(event_id: str) -> Dict:
    url = EVENT_API.format(event_id)
    async with httpx.AsyncClient(timeout=10) as session:
        r = await session.get(url)
        r.raise_for_status()
        data = r.json()
        
        score = data.get('score', [0, 0])
        ft_home = int(score[0]) if len(score) > 0 else 0
        ft_away = int(score[1]) if len(score) > 1 else 0
        
        ht_home = ht_away = 0
        
        competitors = data.get('competitors', [])
        home_raw = competitors[0].get('name', '') if competitors else ''
        away_raw = competitors[1].get('name', '') if len(competitors) > 1 else ''
        
        return {
            "home_raw": home_raw,
            "away_raw": away_raw,
            "ht_home": ht_home,
            "ht_away": ht_away,
            "ft_home": ft_home,
            "ft_away": ft_away,
            "league": data.get('championshipName', data.get('leagueName', ''))
        }

async def fetch_event_tracker_info(event_id: str) -> Dict | None:
    url = TRACKER_API.format(event_id)
    try:
        async with httpx.AsyncClient(timeout=10) as session:
            r = await session.get(url)
            r.raise_for_status()
            data = r.json()
            score = data.get("score", [0, 0])
            home = int(score[0]) if len(score) > 0 else 0
            away = int(score[1]) if len(score) > 1 else 0
            return {"ft_home": home, "ft_away": away, "ht_home": 0, "ht_away": 0}
    except Exception as e:
        print(f"[WARN] Tracker falhou {event_id}: {e}")
        return None

# ====================== SCRAPER LOOP ======================
async def scraper_loop():
    global previous_event_ids
    print("🚀 Scraper iniciado - cache + detecção por desaparecimento")
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=20) as session:
                r = await session.get(LIVE_API)
                data = r.json()
                
                current_events = data.get('events', [])
                current_event_ids = set()
                
                for event in current_events:
                    if event.get('sportId') != 66: continue
                    event_id = str(event['id'])
                    current_event_ids.add(event_id)
                    
                    # Atualiza cache
                    score_raw = event.get('score', [0, 0])
                    home = int(score_raw[0]) if len(score_raw) > 0 else 0
                    away = int(score_raw[1]) if len(score_raw) > 1 else 0
                    home_raw = event.get('home_team_name', '') or event.get('home_player_name', '')
                    away_raw = event.get('away_team_name', '') or event.get('away_player_name', '')
                    league = event.get('championshipName', '') or event.get('leagueName', '')
                    
                    live_cache[event_id] = {
                        "home_score": home,
                        "away_score": away,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "league": league,
                        "last_seen": datetime.now(timezone.utc)
                    }
                    print(f"[DEBUG LIVE] {event_id}: {home_raw} {home}-{away} {away_raw}")
                
                print(f"[DEBUG] Eventos live atuais: {len(current_event_ids)}")
                
                finished_ids = previous_event_ids - current_event_ids
                
                for event_id in finished_ids:
                    print(f"[INFO] Finalizado detectado: {event_id}")
                    cached = live_cache[event_id]
                    home_raw = cached["home_raw"]
                    away_raw = cached["away_raw"]
                    home_nick = extract_pure_nick_canonical(home_raw)
                    away_nick = extract_pure_nick_canonical(away_raw)
                    league_mapped = map_league_name(cached["league"])
                    
                    placar_final = {
                        "ft_home": cached["home_score"],
                        "ft_away": cached["away_score"],
                        "ht_home": 0,
                        "ht_away": 0
                    }
                    
                    # Tenta details
                    try:
                        details = await fetch_event_details(event_id)
                        placar_final["ft_home"] = details.get("ft_home", placar_final["ft_home"])
                        placar_final["ft_away"] = details.get("ft_away", placar_final["ft_away"])
                        placar_final["ht_home"] = details.get("ht_home", placar_final["ht_home"])
                        placar_final["ht_away"] = details.get("ht_away", placar_final["ht_away"])
                    except:
                        print(f"[DEBUG] Details falhou, tentando tracker")
                    
                    tracker = await fetch_event_tracker_info(event_id)
                    if tracker:
                        placar_final.update(tracker)
                    
                    doc = {
                        "event_id": event_id,
                        "league_mapped": league_mapped,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "home_nick": home_nick,
                        "away_nick": away_nick,
                        "home_score_ht": placar_final["ht_home"],
                        "away_score_ht": placar_final["ht_away"],
                        "home_score_ft": placar_final["ft_home"],
                        "away_score_ft": placar_final["ft_away"],
                        "finished_at": datetime.now(timezone.utc),
                        "source": "desaparecimento_cache_tracker"
                    }
                    
                    await matches.update_one({"event_id": event_id}, {"$set": doc}, upsert=True)
                    print(f"✅ SALVO: {home_nick} {placar_final['ft_home']}-{placar_final['ft_away']} {away_nick} (HT: {placar_final['ht_home']}-{placar_final['ht_away']})")
                
                previous_event_ids = current_event_ids
                
        except Exception as e:
            print(f"Scraper error: {e}")
        
        await asyncio.sleep(8)

# ====================== ENDPOINTS ======================
@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

@app.get("/api/history")
async def get_history(page: int = 1, limit: int = 30):
    five_days_ago = datetime.now(timezone.utc) - timedelta(days=5)
    skip = (page - 1) * limit
    cursor = matches.find({"finished_at": {"$gte": five_days_ago}}).sort("finished_at", -1).skip(skip).limit(limit)
    results = await cursor.to_list(length=limit)
    for r in results:
        r.pop("_id", None)
    total = await matches.count_documents({"finished_at": {"$gte": five_days_ago}})
    return {"results": results, "page": page, "total": total}

@app.get("/api/finished/{event_id}")
async def get_by_event_id(event_id: str):
    doc = await matches.find_one({"event_id": event_id})
    if doc:
        doc.pop("_id", None)
        return doc
    return {"error": "not found"}

@app.on_event("startup")
async def startup():
    asyncio.create_task(scraper_loop())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
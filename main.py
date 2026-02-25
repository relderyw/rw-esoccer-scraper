from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
import asyncio
import re
import os
from datetime import datetime, timezone
from typing import Dict

app = FastAPI(title="RW Tips - Esoccer Result Scraper v2.0")

# ====================== CONFIG ======================
MONGO_URI = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client["estrelabet_esoccer"]
matches = db["finished_matches"]

LIVE_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66&catIds=2085,1571,1728,1594,2086,1729,2130"
EVENT_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventDetails?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}&showNonBoosts=false"

processed = set()  # evita reprocessar no mesmo ciclo

# ====================== CANONICAL NICK (a mágica) ======================
def extract_pure_nick_canonical(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""
    # Prioridade máxima: o que está entre parênteses (padrão Altenar)
    m = re.search(r'\(([^)]+)\)', raw)
    if m:
        nick = m.group(1).strip().upper()
        if 2 <= len(nick) <= 16 and (nick.isupper() or '_' in nick or any(c.isdigit() for c in nick)):
            return nick
    # Fallback seguro
    words = re.findall(r'\b[A-Z0-9_]{2,15}\b', raw.upper())
    return words[-1] if words else raw.strip().upper()[:15]

# ====================== SCRAPER ======================
async def fetch_event_details(event_id: str) -> Dict:
    async with httpx.AsyncClient(timeout=12) as session:
        r = await session.get(EVENT_API.format(event_id))
        data = r.json()
        
        # Score do EventDetails (mais confiável)
        score = data.get('score', [0, 0])
        if isinstance(score, dict):
            ft_home = int(score.get('home', 0))
            ft_away = int(score.get('away', 0))
        else:
            ft_home = int(score[0] if len(score)>0 else 0)
            ft_away = int(score[1] if len(score)>1 else 0)
        
        # HT costuma vir em markets ou em "periodScores"
        ht_home = ht_away = 0
        for market in data.get('markets', []):
            if market.get('name', '').lower() in ['1º tempo - total de gols', '1st half - total goals']:
                # simplisticamente pegamos o score HT se disponível
                pass  # Altenar geralmente não separa HT no details, mas live tem
        
        competitors = data.get('competitors', [])
        home_raw = competitors[0].get('name', '') if competitors else ''
        away_raw = competitors[1].get('name', '') if len(competitors)>1 else ''
        
        return {
            "home_raw": home_raw,
            "away_raw": away_raw,
            "ht_home": ht_home,
            "ht_away": ht_away,
            "ft_home": ft_home,
            "ft_away": ft_away,
            "league": data.get('championshipName', data.get('leagueName', ''))
        }

async def scraper_loop():
    print("🚀 Scraper iniciado - salvando resultados consistentes")
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as session:
                r = await session.get(LIVE_API)
                data = r.json()
                
                for event in data.get("events", []):
                    if event.get("sportId") != 66: continue
                    
                    event_id = str(event["id"])
                    if event_id in processed: continue
                    
                    live_time = event.get("liveTime", "").upper()
                    if any(x in live_time for x in ["FINAL", "ENDED", "FIM", "TERMINADO", "9:"]):
                        # Jogo acabou!
                        details = await fetch_event_details(event_id)
                        
                        home_nick = extract_pure_nick_canonical(details["home_raw"])
                        away_nick = extract_pure_nick_canonical(details["away_raw"])
                        
                        doc = {
                            "_id": event_id,
                            "event_id": event_id,
                            "league_raw": event.get("championshipName") or event.get("leagueName", ""),
                            "league_mapped": map_league_name(event.get("championshipName") or ""),  # reuse sua função
                            "home_raw": details["home_raw"],
                            "away_raw": details["away_raw"],
                            "home_nick": home_nick,
                            "away_nick": away_nick,
                            "home_score_ht": details["ht_home"],
                            "away_score_ht": details["ht_away"],
                            "home_score_ft": details["ft_home"],
                            "away_score_ft": details["ft_away"],
                            "finished_at": datetime.now(timezone.utc),
                            "source": "altenar_scraped"
                        }
                        
                        await matches.update_one({"_id": event_id}, {"$set": doc}, upsert=True)
                        processed.add(event_id)
                        print(f"✅ SALVO: {home_nick} {details['ft_home']}-{details['ft_away']} {away_nick}")
        except Exception as e:
            print(f"Scraper error: {e}")
        
        await asyncio.sleep(8)  # poll a cada 8 segundos

# ====================== ENDPOINTS (compatível com seu bot) ======================
@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}

@app.get("/api/history")
async def get_history(page: int = 1, limit: int = 30):
    skip = (page - 1) * limit
    cursor = matches.find().sort("finished_at", -1).skip(skip).limit(limit)
    results = await cursor.to_list(length=limit)
    # remove _id do mongo
    for r in results:
        r.pop("_id", None)
    return {"results": results, "page": page, "total": await matches.count_documents({})}

@app.get("/api/finished/{event_id}")
async def get_by_event_id(event_id: str):
    doc = await matches.find_one({"event_id": event_id})
    if doc:
        doc.pop("_id", None)
        return doc
    return {"error": "not found"}

# ====================== STARTUP ======================
@app.on_event("startup")
async def startup():
    asyncio.create_task(scraper_loop())

# Coloque aqui sua função map_league_name (copie do seu bot)
def map_league_name(name: str):
    # ... cole toda sua LIVE_LEAGUE_MAPPING + HISTORY_LEAGUE_MAPPING aqui
    # (ou importe se preferir)
    pass  # substitua pela sua função completa
# No topo (adicione se não tiver)
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import httpx

# Cache de placar em tempo real
score_cache = defaultdict(lambda: {"home": 0, "away": 0, "ht_home": 0, "ht_away": 0})

previous_event_ids = set()

async def fetch_event_tracker_info(event_id: str):
    url = f"https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventTrackerInfo?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={event_id}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            # Parse placar (ajuste conforme estrutura real do JSON)
            score = data.get("score", [0, 0])
            home = int(score[0]) if len(score) > 0 else 0
            away = int(score[1]) if len(score) > 1 else 0
            # HT geralmente não vem, mas se tiver
            ht_home = data.get("ht_home", 0)
            ht_away = data.get("ht_away", 0)
            return {"ft_home": home, "ft_away": away, "ht_home": ht_home, "ht_away": ht_away}
    except Exception as e:
        print(f"[WARN] Falha GetEventTrackerInfo {event_id}: {e}")
        return None

async def scraper_loop():
    global previous_event_ids
    print("🚀 Scraper iniciado - detecção por desaparecimento + cache + tracker fallback")
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as session:
                r = await session.get(LIVE_API)
                data = r.json()
                
                current_events = data.get('events', [])
                current_event_ids = {str(e['id']) for e in current_events if e.get('sportId') == 66}
                
                print(f"[DEBUG] Eventos live atuais: {len(current_event_ids)}")
                
                # Atualiza cache de placar para jogos ainda live
                for event in current_events:
                    if event.get('sportId') != 66: continue
                    event_id = str(event['id'])
                    score_raw = event.get('score', [0, 0])
                    home = int(score_raw[0]) if len(score_raw) > 0 else 0
                    away = int(score_raw[1]) if len(score_raw) > 1 else 0
                    score_cache[event_id]["home"] = home
                    score_cache[event_id]["away"] = away
                
                # Jogos que sumiram (finalizados)
                finished_ids = previous_event_ids - current_event_ids
                
                for event_id in finished_ids:
                    print(f"[INFO] Jogo finalizado detectado (sumiu): {event_id}")
                    
                    last_score = score_cache[event_id]
                    
                    placar_final = {"ft_home": last_score["home"], "ft_away": last_score["away"],
                                    "ht_home": last_score["ht_home"], "ht_away": last_score["ht_away"]}
                    
                    # Tenta GetEventDetails primeiro
                    try:
                        details = await fetch_event_details(event_id)
                        placar_final["ft_home"] = details.get("ft_home", placar_final["ft_home"])
                        placar_final["ft_away"] = details.get("ft_away", placar_final["ft_away"])
                        placar_final["ht_home"] = details.get("ht_home", placar_final["ht_home"])
                        placar_final["ht_away"] = details.get("ht_away", placar_final["ht_away"])
                    except:
                        print(f"[DEBUG] GetEventDetails falhou para {event_id}, tentando tracker")
                        # Fallback para GetEventTrackerInfo
                        tracker = await fetch_event_tracker_info(event_id)
                        if tracker:
                            placar_final.update(tracker)
                    
                    home_nick = "UNKNOWN"  # Ajuste se tiver como pegar do cache ou live anterior
                    away_nick = "UNKNOWN"
                    
                    doc = {
                        "event_id": event_id,
                        "league_mapped": "UNKNOWN",  # Ajuste se tiver cache de liga
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
                    print(f"✅ SALVO: {home_nick} {placar_final['ft_home']}-{placar_final['ft_away']} {away_nick}  (HT: {placar_final['ht_home']}-{placar_final['ht_away']})")
                
                previous_event_ids = current_event_ids
                
        except Exception as e:
            print(f"Scraper error: {e}")
        
        await asyncio.sleep(8)
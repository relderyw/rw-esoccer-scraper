# No topo do arquivo, adicione isso (junto com os outros imports)
from collections import defaultdict
import json

# Cache de placar em tempo real (event_id -> último placar conhecido)
score_cache = defaultdict(lambda: {"home": 0, "away": 0, "ht_home": 0, "ht_away": 0})

async def scraper_loop():
    global previous_event_ids
    print("🚀 Scraper iniciado - salvando resultados consistentes (com cache de placar)")
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as session:
                r = await session.get(LIVE_API)
                data = r.json()
                
                current_events = data.get('events', [])
                current_event_ids = {str(e['id']) for e in current_events if e.get('sportId') == 66}
                
                print(f"[DEBUG] Eventos live atuais: {len(current_event_ids)}")
                
                # ==================== ATUALIZA CACHE DE PLACAR ====================
                for event in current_events:
                    if event.get('sportId') != 66: continue
                    event_id = str(event['id'])
                    
                    # Parseia placar atual
                    score_raw = event.get('score', [0, 0])
                    if isinstance(score_raw, list):
                        home = int(score_raw[0]) if len(score_raw) > 0 else 0
                        away = int(score_raw[1]) if len(score_raw) > 1 else 0
                    else:
                        home = away = 0
                    
                    score_cache[event_id] = {
                        "home": home,
                        "away": away,
                        "ht_home": score_cache[event_id]["ht_home"],  # mantém HT se já tinha
                        "ht_away": score_cache[event_id]["ht_away"]
                    }
                
                # ==================== DETECTA JOGOS QUE TERMINARAM ====================
                finished_ids = previous_event_ids - current_event_ids
                
                for event_id in finished_ids:
                    print(f"[INFO] Jogo finalizado detectado por desaparecimento: {event_id}")
                    
                    last_score = score_cache[event_id]
                    
                    try:
                        details = await fetch_event_details(event_id)
                        home_nick = extract_pure_nick_canonical(details.get("home_raw", ""))
                        away_nick = extract_pure_nick_canonical(details.get("away_raw", ""))
                        
                        # Prioriza placar do details se disponível, senão usa cache
                        ft_home = details.get("ft_home", last_score["home"])
                        ft_away = details.get("ft_away", last_score["away"])
                        ht_home = details.get("ht_home", last_score["ht_home"])
                        ht_away = details.get("ht_away", last_score["ht_away"])
                        
                        doc = {
                            "event_id": event_id,
                            "league_mapped": map_league_name(details.get("league", "")),
                            "home_nick": home_nick,
                            "away_nick": away_nick,
                            "home_score_ht": ht_home,
                            "away_score_ht": ht_away,
                            "home_score_ft": ft_home,
                            "away_score_ft": ft_away,
                            "finished_at": datetime.now(timezone.utc),
                            "source": "desaparecimento_com_cache"
                        }
                        
                        await matches.update_one({"event_id": event_id}, {"$set": doc}, upsert=True)
                        
                        print(f"✅ SALVO: {home_nick} {ft_home}-{ft_away} {away_nick}  (HT: {ht_home}-{ht_away})")
                        
                    except Exception as e:
                        print(f"[WARN] Erro ao salvar {event_id}: {e}")
                        # Salva pelo menos com o cache
                        doc = {
                            "event_id": event_id,
                            "home_nick": "UNKNOWN",
                            "away_nick": "UNKNOWN",
                            "home_score_ft": last_score["home"],
                            "away_score_ft": last_score["away"],
                            "finished_at": datetime.now(timezone.utc),
                            "source": "desaparecimento_cache_fallback"
                        }
                        await matches.update_one({"event_id": event_id}, {"$set": doc}, upsert=True)
                
                previous_event_ids = current_event_ids
                
        except Exception as e:
            print(f"Scraper error: {e}")
        
        await asyncio.sleep(8)
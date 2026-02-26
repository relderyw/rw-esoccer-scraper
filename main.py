# No topo (adicione se não tiver)
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# Cache expandido: event_id -> {placar, nicks raw, liga}
live_cache = defaultdict(lambda: {
    "home_score": 0,
    "away_score": 0,
    "home_raw": "",
    "away_raw": "",
    "league": "",
    "last_seen": datetime.now(timezone.utc)
})

previous_event_ids: Set[str] = set()

# Função para atualizar cache enquanto jogo está live
def update_live_cache(event):
    event_id = str(event['id'])
    score_raw = event.get('score', [0, 0])
    home = int(score_raw[0]) if len(score_raw) > 0 else 0
    away = int(score_raw[1]) if len(score_raw) > 1 else 0
    
    competitors = event.get('competitorIds', [])
    # Pegue nomes raw (ajuste conforme estrutura real da API)
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

async def scraper_loop():
    global previous_event_ids
    print("🚀 Scraper iniciado - cache expandido + detecção por desaparecimento")
    
    while True:
        try:
            async with httpx.AsyncClient(timeout=20) as session:  # timeout maior
                r = await session.get(LIVE_API)
                data = r.json()
                
                current_events = data.get('events', [])
                current_event_ids = set()
                
                for event in current_events:
                    if event.get('sportId') != 66: continue
                    event_id = str(event['id'])
                    current_event_ids.add(event_id)
                    
                    # Atualiza cache enquanto vivo
                    update_live_cache(event)
                    print(f"[DEBUG LIVE] Atualizado cache para {event_id}: {live_cache[event_id]['home_raw']} {live_cache[event_id]['home_score']}-{live_cache[event_id]['away_score']} {live_cache[event_id]['away_raw']}")
                
                print(f"[DEBUG] Eventos live atuais: {len(current_event_ids)}")
                
                # Jogos que sumiram
                finished_ids = previous_event_ids - current_event_ids
                
                for event_id in finished_ids:
                    print(f"[INFO] Jogo finalizado detectado por desaparecimento: {event_id}")
                    
                    cached = live_cache[event_id]
                    home_raw = cached["home_raw"]
                    away_raw = cached["away_raw"]
                    home_nick = extract_pure_nick_canonical(home_raw)
                    away_nick = extract_pure_nick_canonical(away_raw)
                    league_mapped = map_league_name(cached["league"])
                    
                    placar_final = {
                        "ft_home": cached["home_score"],
                        "ft_away": cached["away_score"],
                        "ht_home": 0,  # fallback, tente atualizar se tiver HT
                        "ht_away": 0
                    }
                    
                    # Tenta GetEventDetails
                    details = None
                    try:
                        details = await fetch_event_details(event_id)
                        if details:
                            placar_final["ft_home"] = details.get("ft_home", placar_final["ft_home"])
                            placar_final["ft_away"] = details.get("ft_away", placar_final["ft_away"])
                            placar_final["ht_home"] = details.get("ht_home", placar_final["ht_home"])
                            placar_final["ht_away"] = details.get("ht_away", placar_final["ht_away"])
                    except:
                        print(f"[DEBUG] Details falhou, tentando tracker")
                    
                    # Fallback tracker
                    if not details:
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
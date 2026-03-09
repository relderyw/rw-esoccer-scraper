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

# Fuso horário do usuário (UTC-4)
USER_TZ = timezone(timedelta(hours=-4))


# ====================== CONFIG ======================
MONGO_URI = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client["estrelabet_esoccer"]
matches = db["finished_matches"]

LIVE_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetLiveEvents?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventCount=0&sportId=66&catIds=2085,1571,1728,1594,2086,1729,2130"
EVENT_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventDetails?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}&showNonBoosts=false"
TRACKER_API = "https://sb2frontend-altenar2.biahosted.com/api/widget/GetEventTrackerInfo?culture=pt-BR&timezoneOffset=-180&integration=estrelabet&deviceType=1&numFormat=en-GB&countryCode=BR&eventId={}"

SUPERBET_STRUCT_API = "https://production-superbet-offer-br.freetls.fastly.net/v2/pt-BR/struct?currentStatus=active"
SUPERBET_HISTORY_API = "https://production-superbet-offer-br.freetls.fastly.net/v2/pt-BR/events/by-date?compression=true&sportId=75&currentStatus=finished&startDate={}&endDate={}"

previous_event_ids = set()
superbet_seen_match_ids = set()

# Cache de torneios Superbet
superbet_tournaments = {}


# Cache expandido
live_cache = defaultdict(lambda: {
    "home_score": 0,
    "away_score": 0,
    "ht_home": 0,
    "ht_away": 0,
    "home_raw": "",
    "away_raw": "",
    "league": "",
    "started_at": None,
    "last_seen": datetime.now(USER_TZ)
})


# ====================== AUXILIARES ======================


def extract_pure_nick_canonical(raw: str) -> str:
    if not raw or not isinstance(raw, str):
        return ""

    # Common team names and abbreviations
    common_teams = [
        'Spain', 'France', 'Germany', 'Italy', 'Brazil', 'Argentina', 'Portugal', 'Netherlands', 'England', 'Belgium',
        'Real Madrid', 'Barcelona', 'FC Bayern', 'Man City', 'Man Utd', 'Liverpool', 'PSG', 'Juventus', 'Arsenal', 'Chelsea',
        'Borussia Dortmund', 'Bayer Leverkusen', 'Napoli', 'AC Milan', 'Inter', 'Inter de Milão', 'Atletico Madrid', 'Sevilla',
        'Piemonte Calcio', 'Latium', 'Genoa', 'Roma', 'RB Leipzig', 'Real Sociedad', 'Athletic Club', 'Aston Villa', 'Spurs'
    ]
    known_acronyms = ['PSG', 'RMA', 'FCB', 'MCI', 'MUN', 'LIV', 'CHE', 'ARS',
                      'TOT', 'JUV', 'MIL', 'INT', 'NAP', 'BVB', 'ATM', 'FC', 'CF', 'SC']

    # 1. Parentheses logic
    paren_match = re.search(r'(.*?)\((.*?)\)', raw)
    if paren_match:
        part1, part2 = paren_match.group(
            1).strip(), paren_match.group(2).strip()

        is_p1_caps = bool(
            re.match(r'^[A-Z0-9\s_]+$', part1)) and len(part1) > 1
        is_p2_caps = bool(
            re.match(r'^[A-Z0-9\s_]+$', part2)) and len(part2) > 1

        if is_p2_caps and not is_p1_caps:
            return part2
        if is_p1_caps and not is_p2_caps:
            return part1

        if any(team in part1 for team in common_teams):
            return part2
        if any(team in part2 for team in common_teams):
            return part1

        return part2

    # 2. No parentheses logic
    clean_str = raw.strip()

    if " " not in clean_str and re.match(r'^[A-Z0-9_]+$', clean_str) and clean_str not in known_acronyms:
        return clean_str

    # Strip known teams out completely
    team_words_to_remove = sorted(
        common_teams + known_acronyms, key=len, reverse=True)
    for team in team_words_to_remove:
        clean_str = re.sub(rf'\b{re.escape(team)}\b',
                           '', clean_str, flags=re.IGNORECASE).strip()

    clean_str = re.sub(r'^[-·]+|[-·]+$', '', clean_str).strip()
    clean_str = re.sub(r'\s+', ' ', clean_str)

    if clean_str:
        return clean_str

    # Ultimate fallback
    parts = raw.split()
    if len(parts) > 1:
        return parts[-1]

    return raw.strip()


def map_league_name(name: str, duration: str = "8 min") -> str:
    original = name.upper()

    if "H2H" in original:
        return "H2H 8 MIN"

    if "BATTLE" in original:
        if "6" in duration or "3 MIN" in duration.upper() or "VOLTA" in original:
            return "VOLTA - 6 MIN"
        return "BATTLE 8 MIN"

    if "GT " in original or "GT LEAGUES" in original:
        return "GT LEAGUES"

    if "EAL" in original or "ADRIATIC" in original:
        return "ADRIATIC"

    if "CHAMPIONS LEAGUE" in original and "BATTLE" not in original:
        if "12" in duration or "6 MIN" in duration.upper():
            return "CHAMPIONS LEAGUE - 12 MIN"
        return "CHAMPIONS LEAGUE"

    return name.strip() or "UNKNOWN"

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
        away_raw = competitors[1].get(
            'name', '') if len(competitors) > 1 else ''

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

# ====================== SUPERBET SCRAPERS ======================
SUPERBET_STRUCT_API = "https://production-superbet-offer-br.freetls.fastly.net/v2/pt-BR/struct"

# ... (omitting lines between for brevity, wait, I can just replace the whole function and the constant if I select the right lines. Actually I will just replace `superbet_struct_cacher_loop` here, and use the hardcoded url inside it to be safe, or just replace the function body.)


async def superbet_struct_cacher_loop():
    print("🚀 Inciando cacher de torneios da Superbet (1h)")
    # URL mudou para pegar todos os torneios, mesmo os recém-fechados
    url = "https://production-superbet-offer-br.freetls.fastly.net/v2/pt-BR/struct"
    while True:
        try:
            async with httpx.AsyncClient(timeout=20) as session:
                r = await session.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                data = r.json()
                tournaments = data.get('data', {}).get('tournaments', [])
                count = 0

                # Se for lista (como é no endpoint sem currentStatus)
                if isinstance(tournaments, list):
                    for t_data in tournaments:
                        t_id = str(t_data.get('id', ''))
                        name = t_data.get('localNames', {}).get(
                            'pt-BR', t_data.get('name', ''))
                        footer = str(t_data.get('footer', ''))
                        duration_match = re.search(
                            r'(\d+x\d+)', footer, re.IGNORECASE)
                        duration = f"{duration_match.group(1)} min" if duration_match else "12 min"
                        if t_id:
                            superbet_tournaments[t_id] = {
                                "name": name, "duration": duration}
                            count += 1

                # Se por algum motivo voltar a ser dict
                elif isinstance(tournaments, dict):
                    for t_id, t_data in tournaments.items():
                        name = t_data.get('localNames', {}).get(
                            'pt-BR', t_data.get('name', ''))
                        footer = str(t_data.get('footer', ''))
                        duration_match = re.search(
                            r'(\d+x\d+)', footer, re.IGNORECASE)
                        duration = f"{duration_match.group(1)} min" if duration_match else "12 min"
                        superbet_tournaments[str(t_id)] = {
                            "name": name, "duration": duration}
                        count += 1

                print(
                    f"✅ [SUPERBET STRUCT] Cache atualizado com {count} torneios.")
        except Exception as e:
            print(f"❌ [SUPERBET STRUCT] Erro ao atualizar: {e}")

        await asyncio.sleep(3600)


async def superbet_scraper_loop():
    print("⏳ Aguardando cache de torneios da Superbet carregar...")
    while not superbet_tournaments:
        await asyncio.sleep(1)

    print("🚀 Superbet History Scraper Iniciado (30s)")
    while True:
        try:
            # Query dates always in UTC for Superbet API
            now_utc = datetime.now(timezone.utc)
            past_utc = now_utc - timedelta(hours=3)

            start_date = past_utc.strftime('%Y-%m-%d+%H:%M:%S')
            end_date = now_utc.strftime('%Y-%m-%d+%H:%M:%S')


            url = SUPERBET_HISTORY_API.format(start_date, end_date)

            async with httpx.AsyncClient(timeout=30) as session:
                r = await session.get(url, headers={'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
                if r.status_code == 200:
                    data = r.json()
                    events = data.get('data', [])

                    saved_count = 0
                    for event in events:
                        event_id = str(event.get('eventId'))

                        if event_id in superbet_seen_match_ids:
                            continue

                        match_name = event.get('matchName', '')
                        parts = match_name.split('·')
                        home_raw = parts[0].strip() if len(parts) > 0 else ''
                        away_raw = parts[1].strip() if len(parts) > 1 else ''

                        home_nick = extract_pure_nick_canonical(home_raw)
                        away_nick = extract_pure_nick_canonical(away_raw)

                        meta = event.get('metadata', {})
                        ft_home = int(meta.get('homeTeamScore', 0))
                        ft_away = int(meta.get('awayTeamScore', 0))

                        ht_home = 0
                        ht_away = 0
                        periods = meta.get('periods', [])
                        for p in periods:
                            if p.get('num') == 1:
                                ht_home = int(p.get('homeTeamScore', 0))
                                ht_away = int(p.get('awayTeamScore', 0))
                                break

                        t_id = str(event.get('tournamentId'))
                        cached_tournament = superbet_tournaments.get(t_id, {})
                        league_raw_name = cached_tournament.get(
                            'name', f"Superbet League {t_id}")
                        duration = cached_tournament.get('duration', '12 min')
                        league_mapped = map_league_name(
                            league_raw_name, duration)

                        utc_date = event.get('utcDate')
                        finished_at = datetime.fromisoformat(utc_date.replace(
                            'Z', '+00:00')).astimezone(USER_TZ) if utc_date else datetime.now(USER_TZ)


                        doc = {
                            "event_id": f"sb-{event_id}",
                            "league_mapped": league_mapped,
                            "duration": duration,
                            "home_raw": home_raw,
                            "away_raw": away_raw,
                            "home_nick": home_nick,
                            "away_nick": away_nick,
                            "home_score_ht": ht_home,
                            "away_score_ht": ht_away,
                            "home_score_ft": ft_home,
                            "away_score_ft": ft_away,
                            "started_at": finished_at - timedelta(minutes=15),
                            "finished_at": finished_at,
                            "source": "superbet_api"
                        }

                        await matches.update_one({"event_id": doc["event_id"]}, {"$set": doc}, upsert=True)
                        superbet_seen_match_ids.add(event_id)
                        saved_count += 1
                        print(
                            f"✅ SUPERBET: {home_nick} {ft_home}-{ft_away} {away_nick} ({league_mapped})")

                    if saved_count > 0:
                        total_matches = await matches.count_documents({})
                        if total_matches > 2000:
                            excess = total_matches - 2000
                            oldest_matches = await matches.find({}, {"_id": 1}).sort("finished_at", 1).limit(excess).to_list(length=excess)
                            old_ids = [m["_id"] for m in oldest_matches]
                            if old_ids:
                                await matches.delete_many({"_id": {"$in": old_ids}})
                                print(
                                    f"🧹 [CLEANUP] Superbet {len(old_ids)} jogos excluídos (limite 2000).")

        except Exception as e:
            print(f"Superbet Scraper error: {e}")

        await asyncio.sleep(30)

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
                competitors_dict = {c['id']: c['name']
                                    for c in data.get('competitors', [])}
                champs_dict = {c['id']: c['name']
                               for c in data.get('champs', [])}

                for event in current_events:
                    if event.get('sportId') != 66:
                        continue
                    event_id = str(event['id'])
                    current_event_ids.add(event_id)

                    # Atualiza cache
                    score_raw = event.get('score', [0, 0])
                    home = int(score_raw[0]) if len(score_raw) > 0 else 0
                    away = int(score_raw[1]) if len(score_raw) > 1 else 0

                    live_time = str(
                        event.get('liveTime', event.get('ls', ''))).lower()
                    cached_ht_home = live_cache[event_id].get("ht_home", 0)
                    cached_ht_away = live_cache[event_id].get("ht_away", 0)

                    if "1" in live_time or "int" in live_time:
                        ht_home = home
                        ht_away = away
                    else:
                        ht_home = cached_ht_home
                        ht_away = cached_ht_away

                    competitor_ids = event.get('competitorIds', [])
                    if len(competitor_ids) >= 2:
                        home_raw = competitors_dict.get(competitor_ids[0], '')
                        away_raw = competitors_dict.get(competitor_ids[1], '')
                    else:
                        home_raw, away_raw = '', ''

                    if not home_raw or not away_raw:
                        parts = str(event.get('name', '')).split(' vs. ')
                        if len(parts) == 2:
                            home_raw, away_raw = parts[0].strip(
                            ), parts[1].strip()

                    league = champs_dict.get(event.get('champId'), '')

                    # Captura startDate da API
                    start_date_str = event.get('startDate', '')
                    started_at = None
                    if start_date_str:
                        try:
                            # Converte do UTC da API para o timezone do usuário
                            started_at = datetime.fromisoformat(
                                start_date_str.replace('Z', '+00:00')).astimezone(USER_TZ)
                        except:
                            started_at = None


                    live_cache[event_id] = {
                        "home_score": home,
                        "away_score": away,
                        "ht_home": ht_home,
                        "ht_away": ht_away,
                        "home_raw": home_raw,
                        "away_raw": away_raw,
                        "league": league,
                        "started_at": started_at,
                        "last_seen": datetime.now(USER_TZ)
                    }

                    print(
                        f"[DEBUG LIVE] {event_id}: {home_raw} {home}-{away} {away_raw}")

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
                        "ht_home": cached.get("ht_home", 0),
                        "ht_away": cached.get("ht_away", 0)
                    }

                    # Tenta details
                    try:
                        details = await fetch_event_details(event_id)
                        dh, da = details.get(
                            "ft_home", 0), details.get("ft_away", 0)
                        if dh >= placar_final["ft_home"] and da >= placar_final["ft_away"] and (dh > 0 or da > 0):
                            placar_final["ft_home"] = dh
                            placar_final["ft_away"] = da
                        hth = details.get("ht_home", 0)
                        hta = details.get("ht_away", 0)
                        if hth >= placar_final["ht_home"] and hta >= placar_final["ht_away"] and (hth > 0 or hta > 0):
                            placar_final["ht_home"] = hth
                            placar_final["ht_away"] = hta
                    except:
                        pass

                    tracker = await fetch_event_tracker_info(event_id)
                    if tracker:
                        th, ta = tracker.get(
                            "ft_home", 0), tracker.get("ft_away", 0)
                        if th >= placar_final["ft_home"] and ta >= placar_final["ft_away"] and (th > 0 or ta > 0):
                            placar_final["ft_home"] = th
                            placar_final["ft_away"] = ta

                    # ALtenar DB Sync - APENAS LIGAS VALHALLA E VALKYRIE
                    if "VALHALLA" in league_mapped.upper() or "VALKYRIE" in league_mapped.upper():
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
                            "started_at": cached.get("started_at"),
                            "finished_at": datetime.now(USER_TZ),
                            "source": "desaparecimento_cache_tracker"

                        }

                        await matches.update_one({"event_id": event_id}, {"$set": doc}, upsert=True)
                        print(
                            f"✅ ALTENAR SALVO: {home_nick} {placar_final['ft_home']}-{placar_final['ft_away']} {away_nick} (HT: {placar_final['ht_home']}-{placar_final['ht_away']})")
                    else:
                        print(
                            f"⏭️ ALTENAR IGNORADO (Não é Valhalla/Valkyrie): {home_nick} vs {away_nick} na liga {league_mapped}")

                if finished_ids:
                    # Limita a coleção para manter no máximo 1000 jogos
                    total_matches = await matches.count_documents({})
                    if total_matches > 1000:
                        excess = total_matches - 1000
                        oldest_matches = await matches.find({}, {"_id": 1}).sort("finished_at", 1).limit(excess).to_list(length=excess)
                        old_ids = [m["_id"] for m in oldest_matches]
                        if old_ids:
                            await matches.delete_many({"_id": {"$in": old_ids}})
                            print(
                                f"🧹 [CLEANUP] {len(old_ids)} jogos mais antigos excluídos para manter no máximo 1000.")

                previous_event_ids = current_event_ids

        except Exception as e:
            print(f"Scraper error: {e}")

        await asyncio.sleep(8)

# ====================== ENDPOINTS ======================


@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.now(USER_TZ).isoformat()}



@app.get("/api/history")
async def get_history(page: int = 1, limit: int = 30):
    five_days_ago = datetime.now(USER_TZ) - timedelta(days=5)

    skip = (page - 1) * limit
    cursor = matches.find({"finished_at": {"$gte": five_days_ago}}).sort(
        "finished_at", -1).skip(skip).limit(limit)
    results = await cursor.to_list(length=limit)
    for r in results:
        r.pop("_id", None)
        if hasattr(r.get("finished_at"), "isoformat"):
            r["finished_at"] = r["finished_at"].isoformat()
            if not r["finished_at"].endswith('Z') and '+' not in r["finished_at"] and '-' not in r["finished_at"][19:]:
                # Note: isoformat for USER_TZ will already have offset, so we only add Z if manually needed/missing.
                # However, with astimezone(USER_TZ), isoformat() includes -04:00.
                pass
        if hasattr(r.get("started_at"), "isoformat"):
            r["started_at"] = r["started_at"].isoformat()
            if not r["started_at"].endswith('Z') and '+' not in r["started_at"] and '-' not in r["started_at"][19:]:
                pass

    total = await matches.count_documents({"finished_at": {"$gte": five_days_ago}})
    return {"results": results, "page": page, "total": total}


@app.get("/api/finished/{event_id}")
async def get_by_event_id(event_id: str):
    doc = await matches.find_one({"event_id": event_id})
    if doc:
        doc.pop("_id", None)
        if hasattr(doc.get("finished_at"), "isoformat"):
            doc["finished_at"] = doc["finished_at"].isoformat()
            if not doc["finished_at"].endswith('Z') and '+' not in doc["finished_at"] and '-' not in doc["finished_at"][19:]:
                pass
        if hasattr(doc.get("started_at"), "isoformat"):
            doc["started_at"] = doc["started_at"].isoformat()
            if not doc["started_at"].endswith('Z') and '+' not in doc["started_at"] and '-' not in doc["started_at"][19:]:
                pass

        return doc
    return {"error": "not found"}


@app.on_event("startup")
async def startup():
    print("🧹 [CLEANUP] Removendo jogos legados da Altenar (que não são Valhalla/Valkyrie)...")
    try:
        result = await matches.delete_many({
            "source": "desaparecimento_cache_tracker",
            "league_mapped": {"$not": {"$regex": "VALHALLA|VALKYRIE", "$options": "i"}}
        })
        print(
            f"🧹 [CLEANUP] {result.deleted_count} jogos legados removidos com sucesso!")
    except Exception as e:
        print(f"Erro no cleanup: {e}")

    asyncio.create_task(superbet_struct_cacher_loop())
    asyncio.create_task(superbet_scraper_loop())
    asyncio.create_task(scraper_loop())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

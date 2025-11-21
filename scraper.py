import requests
import pandas as pd
import time

BASE_URL_CLUBS = "https://compstats.uefa.com/v1/team-ranking"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ucl-research-bot/1.0)"
}


def safe_int(x):
    """Convierte a int o devuelve None si no se puede."""
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def get_club_stats_one_season(season_year: int,
                              limit: int = 200,
                              offset: int = 0) -> pd.DataFrame:
    """
    Descarga las estadísticas de clubs para una temporada concreta
    (seasonYear) de la Champions (competitionId=1).
    """

    params = {
        "competitionId": "1",          # Champions League
        "limit": str(limit),           # nº máximo de equipos a devolver
        "offset": str(offset),
        "optionalFields": "PLAYER,TEAM",
        "order": "DESC",
        "phase": "TOURNAMENT",
        "seasonYear": str(season_year),
        # aquí defines qué estadísticas quieres:
        "stats": ",".join([
            "matches_appearance",
            "matches_win",
            "matches_draw",
            "matches_loss",
        ]),
    }

    r = requests.get(BASE_URL_CLUBS, params=params,
                     headers=HEADERS, timeout=15)
    r.raise_for_status()

    data = r.json()     # es una lista de entradas, no un dict

    rows = []

    for entry in data:
        team = entry.get("team", {})
        stats_list = entry.get("statistics", [])
        team_id = entry.get("teamId")

        # Pasamos la lista de estadísticas a diccionario: nombre → valor
        stats_dict = {
            s.get("name"): s.get("value")
            for s in stats_list
        }

        translations = team.get("translations", {})
        display_name = translations.get("displayName", {})
        country_name = translations.get("countryName", {})

        row = {
            "season_year": season_year,
            "team_id": team_id,
            "team_code": team.get("teamCode"),
            "team_name_en": display_name.get("EN"),
            "team_name_es": display_name.get("ES"),
            "country_en": country_name.get("EN"),
            "country_es": country_name.get("ES"),

            # Stats principales (convertidas a int cuando se pueda)
            "matches": safe_int(stats_dict.get("matches_appearance")),
            "wins": safe_int(stats_dict.get("matches_win")),
            "draws": safe_int(stats_dict.get("matches_draw")),
            "losses": safe_int(stats_dict.get("matches_loss")),
        }

        rows.append(row)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    all_dfs = []

    # 👉 Aquí eliges el rango de temporadas
    # Empieza probando con 2011–2024 que seguro existe
    for season in range(1992, 2026):
        print(f"Descargando temporada {season}/{season+1}...")
        try:
            df_season = get_club_stats_one_season(season)
            if not df_season.empty:
                all_dfs.append(df_season)
            else:
                print(f"  (sin datos devueltos para {season})")
        except Exception as e:
            print(f"  Error en temporada {season}: {e}")

        # Pausa para no dar la tabarra al servidor
        time.sleep(1)

    if all_dfs:
        clubs_df = pd.concat(all_dfs, ignore_index=True)
        clubs_df.to_csv("data/ucl_clubs_stats_1992_2025.csv", index=False)
        print("✅ Archivo guardado: data/ucl_clubs_stats_1992_2025.csv")
    else:
        print("No se descargó nada.")

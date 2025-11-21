import requests
import pandas as pd
import time

BASE_URL_CLUBS = "https://compstats.uefa.com/v1/team-ranking"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ucl-research-bot/1.0)"
}

def safe_int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None

def scrape_stats_group(season_year: int, stats_list, group_name: str,
                       limit: int = 200, offset: int = 0) -> pd.DataFrame:
    """
    Descarga estadísticas de clubes para una temporada y un grupo de stats.
    Se mantiene la misma estructura (season_year, team_id, team_code, team_name_en,
    team_name_es, country_en, country_es) en todos los CSV.
    """
    params = {
        "competitionId": "1",
        "limit": str(limit),
        "offset": str(offset),
        "optionalFields": "PLAYER,TEAM",
        "order": "DESC",
        "phase": "TOURNAMENT",
        "seasonYear": str(season_year),
        "stats": ",".join(stats_list),
    }

    r = requests.get(BASE_URL_CLUBS, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json()

    rows = []
    for entry in data:
        team = entry.get("team", {})
        stats_list_resp = entry.get("statistics", [])
        stats_dict = {s.get("name"): s.get("value") for s in stats_list_resp}
        translations = team.get("translations", {})
        display_name = translations.get("displayName", {})
        country_name = translations.get("countryName", {})

        row = {
            "season_year": season_year,
            "team_id": entry.get("teamId"),
            "team_code": team.get("teamCode"),
            "team_name_en": display_name.get("EN"),
            "team_name_es": display_name.get("ES"),
            "country_en": country_name.get("EN"),
            "country_es": country_name.get("ES"),
        }

        # Añadimos SOLO las estadísticas específicas de este grupo
        for stat in stats_list:
            row[f"{group_name}__{stat}"] = safe_int(stats_dict.get(stat))

        rows.append(row)

    return pd.DataFrame(rows)


if __name__ == "__main__":
    # 📌 Stats por pestaña que quieres scrapear
    STAT_GROUPS = {
        "key": [
            "matches_appearance",
            "matches_win",
            "matches_draw",
            "matches_loss",
        ],
        "goals": [
            "goals",
            "goals_scored_with_right",
            "goals_scored_with_left",
            "goals_scored_head",
            "goals_scored_inside_penalty_area",
            "goals_scored_outside_penalty_area",
            "penalty_scored",
            "matches_appearance",
        ],
        "attempts": [
            "attempts",
            "attempts_on_target",
            "attempts_off_target",
            "attempts_blocked",
            "matches_appearance",
        ],
        "distribution": [
            "passes_accuracy",
            "passes_attempted",
            "passes_completed",
            "ball_possession",
            "cross_accuruacy",
            "cross_attempted",
            "cross_completed",
            "free_kick",
            "matches_appearance",
        ],
        "attacking": [
            "attacks",
            "assists",
            "corners",
            "offsides",
            "dribbling",
            "matches_appearance",
        ],
        "defending": [
            "recovered_ball",
            "tackles"
            "tackles_won",
            "tackles_lost",
            "clearance_attempted",
            "matches_appearance",
        ],
        "goalkeeping": [
            "saves",
            "goals_conceded",
            "own_goal_conceded",
            "saves_on_penalty",
            "clean_sheet",
            "punches",
            "matches_appearance",
        ],
        "disciplinary": [
            "fouls_committed",
            "fouls_suffered",
            "yellow_cards",
            "red_cards",
            "matches_appearance",
        ],

    }

    for group_name, stats_list in STAT_GROUPS.items():
        print(f"\n📊 Extrayendo estadísticas: {group_name}")
        all_dfs = []

        for season in range(1992, 2026):
            print(f"  ➤ Temporada {season}/{season+1}…")
            try:
                df_season = scrape_stats_group(season, stats_list, group_name)
                if not df_season.empty:
                    all_dfs.append(df_season)
            except Exception as e:
                print(f"    ⚠️ Error en temporada {season}: {e}")
            time.sleep(1)

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)

            # Ordenamos las columnas para que los datos básicos siempre estén delante
            col_order = [
                "season_year", "team_id", "team_code", "team_name_en",
                "team_name_es", "country_en", "country_es"
            ]
            other_cols = [c for c in final_df.columns if c not in col_order]
            final_df = final_df[col_order + other_cols]

            file_name = f"data/ucl_clubs_{group_name}_stats_1992_2025.csv"
            final_df.to_csv(file_name, index=False)
            print(f"📁 Guardado correctamente: {file_name}")
        else:
            print(f"❌ No se han generado datos para {group_name}.")

import requests
import pandas as pd
import time
import re
import warnings
import os

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ucl-research-bot/1.0)"
}

BASE_WIKI_URL = "https://en.wikipedia.org/wiki/"


def clean_score(score_str):
    """
    Convierte strings tipo:
        '3–2', '1–1 (a.e.t.)', '1–1 (pens 4–3)'
    en:
        home_goals, away_goals, extra_time(bool), penalties(bool)
    """
    if not isinstance(score_str, str):
        return None, None, False, False

    s = score_str.strip()
    extra_time = bool(re.search(r"a\.e\.t|after extra time", s, flags=re.IGNORECASE))
    pens = bool(re.search(r"pen", s, flags=re.IGNORECASE))

    # Parte antes del primer paréntesis
    base = s.split("(")[0].strip()

    # Wikipedia suele usar '–', pero aceptamos '-'
    if "–" in base:
        parts = base.split("–")
    elif "-" in base:
        parts = base.split("-")
    else:
        return None, None, extra_time, pens

    if len(parts) != 2:
        return None, None, extra_time, pens

    h, a = parts
    try:
        home_goals = int(h.strip())
        away_goals = int(a.strip())
    except ValueError:
        home_goals, away_goals = None, None

    return home_goals, away_goals, extra_time, pens


def looks_like_match_table(df: pd.DataFrame) -> bool:
    """
    Heurística para decidir si una tabla parece de partidos.
    Buscamos columnas tipo 'Home team', 'Away team', 'Score', 'Result', 'Team 1', 'Team 2'.
    """
    cols = [str(c).lower() for c in df.columns]

    has_score = any("score" in c or "result" in c for c in cols)
    has_team = (
        any("home" in c and "team" in c for c in cols) or
        any("away" in c and "team" in c for c in cols) or
        any("team 1" in c or "team1" in c for c in cols) or
        any("team 2" in c or "team2" in c for c in cols)
    )

    return has_score and has_team


def normalize_match_table(df: pd.DataFrame, season_label: str, stage_hint: str = None) -> pd.DataFrame:
    """
    Normaliza una tabla de partidos a columnas estándar:
    season, stage, date, home_team, away_team, score, home_goals, away_goals, extra_time, penalties
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    colmap = {
        "date": None,
        "home_team": None,
        "away_team": None,
        "score": None,
        "stage": None,
    }

    for c in df.columns:
        cl = c.lower()
        if colmap["date"] is None and "date" in cl:
            colmap["date"] = c
        elif colmap["home_team"] is None and (
            ("home" in cl and "team" in cl) or "team 1" in cl or "team1" in cl
        ):
            colmap["home_team"] = c
        elif colmap["away_team"] is None and (
            ("away" in cl and "team" in cl) or "team 2" in cl or "team2" in cl
        ):
            colmap["away_team"] = c
        elif colmap["score"] is None and ("score" in cl or "result" in cl):
            colmap["score"] = c
        elif colmap["stage"] is None and ("round" in cl or "stage" in cl or "group" in cl):
            colmap["stage"] = c

    out = pd.DataFrame()
    out["season"] = season_label

    out["date"] = df[colmap["date"]].astype(str) if colmap["date"] else None
    out["home_team"] = df[colmap["home_team"]].astype(str) if colmap["home_team"] else None
    out["away_team"] = df[colmap["away_team"]].astype(str) if colmap["away_team"] else None
    out["score"] = df[colmap["score"]].astype(str) if colmap["score"] else None
    out["stage"] = (
        df[colmap["stage"]].astype(str) if colmap["stage"] else stage_hint
    )

    home_goals_list, away_goals_list, extra_time_list, pens_list = [], [], [], []

    for s in out["score"]:
        hg, ag, et, pe = clean_score(s)
        home_goals_list.append(hg)
        away_goals_list.append(ag)
        extra_time_list.append(et)
        pens_list.append(pe)

    out["home_goals"] = home_goals_list
    out["away_goals"] = away_goals_list
    out["extra_time"] = extra_time_list
    out["penalties"] = pens_list

    return out


def season_to_wiki_title(start_year: int) -> str:
    """
    Convierte un año de inicio de temporada (1992) en el título Wikipedia:
        '1992–93_UEFA_Champions_League'
    (usa EN DASH '–' entre años)
    """
    end_year_short = str((start_year + 1) % 100).zfill(2)
    season_str = f"{start_year}–{end_year_short}"
    title = f"{season_str}_UEFA_Champions_League"
    return title


def scrape_season_matches(start_year: int) -> pd.DataFrame:
    """
    Scrapea la página de una temporada concreta de la Champions en Wikipedia
    y devuelve un DF con todos los partidos detectados.
    """
    title = season_to_wiki_title(start_year)
    url = BASE_WIKI_URL + title
    print(f"  → Wikipedia: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"    ⚠️ Error al descargar {url}: {e}")
        return pd.DataFrame()

    try:
        tables = pd.read_html(resp.text)
    except ValueError:
        print("    ⚠️ No se han encontrado tablas en esta página.")
        return pd.DataFrame()

    season_label = title.replace("_", " ")
    match_dfs = []

    for i, df in enumerate(tables):
        if df.empty:
            continue
        if not looks_like_match_table(df):
            continue

        norm = normalize_match_table(df, season_label, stage_hint=None)

        mask_valid = norm["home_team"].notna() | norm["away_team"].notna()
        norm = norm[mask_valid]

        if not norm.empty:
            match_dfs.append(norm)

    if match_dfs:
        season_df = pd.concat(match_dfs, ignore_index=True)
        return season_df
    else:
        print("    ℹ️ No se han detectado tablas de partidos en esta página.")
        return pd.DataFrame()


if __name__ == "__main__":
    # Temporadas desde 1992–93 hasta 2024–25 (start_year = 1992..2024)
    START_YEARS = list(range(1992, 2026))

    all_seasons = []

    print("📊 Scrapeando partidos de Champions en Wikipedia...")

    # Crear carpeta data si no existe
    os.makedirs("data", exist_ok=True)

    for y in START_YEARS:
        print(f"\n Temporada {y}/{y+1}")
        df_season = scrape_season_matches(y)
        if not df_season.empty:
            all_seasons.append(df_season)
        time.sleep(1)  # para no abusar de Wikipedia

    if all_seasons:
        matches = pd.concat(all_seasons, ignore_index=True)

        # Filtrado básico de filas sin equipos ni goles
        mask_ok = (
            matches["home_team"].notna() &
            matches["away_team"].notna() &
            (matches["home_goals"].notna() | matches["away_goals"].notna())
        )
        matches = matches[mask_ok]

        col_order = [
            "season",
            "stage",
            "date",
            "home_team",
            "away_team",
            "score",
            "home_goals",
            "away_goals",
            "extra_time",
            "penalties",
        ]
        base_cols = [c for c in col_order if c in matches.columns]
        other_cols = [c for c in matches.columns if c not in base_cols]
        matches = matches[base_cols + other_cols]

        file_name = "data/ucl_matches_wikipedia_1992_2025.csv"
        matches.to_csv(file_name, index=False)
        print(f"\n✅ CSV de partidos guardado en: {file_name}")
        print(f"   Nº filas: {matches.shape[0]}, Nº columnas: {matches.shape[1]}")
    else:
        print("\n❌ No se han obtenido datos de partidos.")

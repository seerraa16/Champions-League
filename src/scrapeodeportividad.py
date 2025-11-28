import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def to_int(text: str):
    """Convierte '12' o '1.234' → int. Devuelve None si no puede."""
    if text is None:
        return None
    text = text.strip()
    if text == "":
        return None
    text = text.replace(".", "").replace(" ", "")
    return int(text) if text.isdigit() else None


def season_label_from_year(year: int) -> str:
    """
    1992 -> '92/93'
    1999 -> '99/00'
    2000 -> '00/01'
    2025 -> '25/26'
    """
    a = year % 100
    b = (year + 1) % 100
    return f"{a:02d}/{b:02d}"


def scrape_fairplay_season(season_id: int) -> pd.DataFrame:
    """
    Scrapea la tabla de deportividad (fair play) de UNA temporada concreta.
    season_id = año de inicio de temporada (ej: 1992 -> 92/93).
    """
    url = f"https://www.transfermarkt.es/uefa-champions-league/fairnesstabelle/pokalwettbewerb/CL/saison_id/{season_id}"
    print(f"\n🌍 Temporada {season_label_from_year(season_id)}  |  URL: {url}")

    resp = requests.get(url, headers=HEADERS, timeout=20)
    print("   Status code:", resp.status_code)
    if resp.status_code != 200:
        print("   ⚠️ No se pudo acceder a esta temporada, se salta.")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "lxml")

    table = soup.find("table", class_="items")
    if table is None:
        print("   ❌ No se encontró tabla 'items' en esta temporada.")
        return pd.DataFrame()

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    records = []
    season_str = season_label_from_year(season_id)

    for row in rows:
        tds = row.find_all("td", recursive=False)
        # Esperamos:
        # 0: #
        # 1: wappen
        # 2: Club
        # 3: amarillas
        # 4: segunda amarilla
        # 5: roja
        # 6: total expulsiones
        # 7: puntos
        if len(tds) < 8:
            continue

        rank = to_int(tds[0].get_text(strip=True))

        # Club
        club_cell = tds[2]
        club_link = club_cell.find("a", href=True)
        if club_link:
            club_name = club_link.get_text(strip=True)
            club_url = "https://www.transfermarkt.es" + club_link["href"]
        else:
            club_name = club_cell.get_text(" ", strip=True) or None
            club_url = None

        yellow = to_int(tds[3].get_text(strip=True))
        yellow_red = to_int(tds[4].get_text(strip=True))
        red = to_int(tds[5].get_text(strip=True))
        dismissals = to_int(tds[6].get_text(strip=True))
        points = to_int(tds[7].get_text(strip=True))

        records.append({
            "Season_id": season_id,
            "Season": season_str,
            "Rank": rank,
            "Club": club_name,
            "Club_url": club_url,
            "Yellow": yellow,
            "YellowRed": yellow_red,
            "Red": red,
            "Dismissals": dismissals,
            "Points": points,
        })

    df = pd.DataFrame(records)
    df = df.dropna(subset=["Club"]).reset_index(drop=True)
    print(f"   ✔ Equipos recogidos: {df.shape[0]}")
    return df


def scrape_fairplay_1992_to_now(start_year: int = 1992, end_year: int = 2025) -> pd.DataFrame:
    """
    Scrapea todas las tablas de deportividad de Champions
    desde start_year (92/93) hasta end_year (25/26, en tu caso 2025).
    """
    all_dfs = []

    for year in range(start_year, end_year + 1):
        try:
            df_season = scrape_fairplay_season(year)
            if not df_season.empty:
                all_dfs.append(df_season)
        except Exception as e:
            print(f"   ❗ Error en temporada {season_label_from_year(year)}: {e}")
        # pequeño sleep para no ser agresivos con el servidor
        time.sleep(0.5)

    if not all_dfs:
        return pd.DataFrame()

    df_all = pd.concat(all_dfs, ignore_index=True)
    # ordenamos por temporada y puntos (por si quieres algo ordenado)
    df_all = df_all.sort_values(["Season_id", "Points", "Rank"]).reset_index(drop=True)
    return df_all


if __name__ == "__main__":
    print("📊 Scrapeando TABLA DE DEPORTIVIDAD Champions 92/93–ahora...")

    os.makedirs("data", exist_ok=True)

    df = scrape_fairplay_1992_to_now(start_year=1992, end_year=2025)

    if not df.empty:
        out = "data/tfmkt_cl_fairplay_1992_2025.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print("   Registros totales:", df.shape[0])
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

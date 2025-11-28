import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re

URL = "https://www.transfermarkt.es/uefa-champions-league/alleEndspiele/pokalwettbewerb/CL"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def parse_score(text: str):
    """
    Extrae los goles local/visitante de un texto tipo:
    '2:0', '1:1 (4:3 pen)', '3:1 n.V.', etc.
    Devuelve (home_goals, away_goals) como int o (None, None).
    """
    if not text:
        return None, None

    m = re.search(r"(\d+)\s*:\s*(\d+)", text)
    if not m:
        return None, None

    return int(m.group(1)), int(m.group(2))


def scrape_cl_finals_transfermarkt():
    print(f"🌍 Descargando todas las finales de Champions:\n{URL}")

    # 1) Descargar HTML
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    print("   Status code:", resp.status_code)
    resp.raise_for_status()

    # 2) Parsear con BeautifulSoup
    soup = BeautifulSoup(resp.text, "lxml")

    # 3) Localizar tabla de finales
    table = soup.find("table", class_="items")
    if table is None:
        print("❌ No se encontró la tabla con class='items'")
        return pd.DataFrame()

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    records = []

    for row in rows:
        # Solo los <td> de primer nivel
        tds = row.find_all("td", recursive=False)
        if len(tds) < 6:
            # Fila rara / separador
            continue

        # --- Temporada (td[0]) ---
        season = tds[0].get_text(strip=True)

        # --- Resultado (td[3]) ---
        result_text = tds[3].get_text(" ", strip=True)
        home_goals, away_goals = parse_score(result_text or "")

        # --- Equipo local (td[1]) ---
        home_cell = tds[1]
        home_link = home_cell.find("a", href=True)
        if home_link:
            home_team = home_link.get_text(strip=True)
            home_url = "https://www.transfermarkt.es" + home_link["href"]
        else:
            home_team = home_cell.get_text(" ", strip=True) or None
            home_url = None

        # --- Equipo visitante (último td: tds[-1]) ---
        away_cell = tds[-1]
        away_link = away_cell.find("a", href=True)
        if away_link:
            away_team = away_link.get_text(strip=True)
            away_url = "https://www.transfermarkt.es" + away_link["href"]
        else:
            away_team = away_cell.get_text(" ", strip=True) or None
            away_url = None

        records.append({
            "Season": season,
            "HomeTeam": home_team,
            "Result_raw": result_text,
            "HomeGoals": home_goals,
            "AwayGoals": away_goals,
            "AwayTeam": away_team,
            
        })

    df = pd.DataFrame(records)

    # Limpiamos filas sin temporada o sin equipos
    df = df.dropna(subset=["Season", "HomeTeam", "AwayTeam"])
    df = df.reset_index(drop=True)

    return df


if __name__ == "__main__":
    print("📊 Scrapeando finales de la Champions (Transfermarkt)...")

    os.makedirs("data", exist_ok=True)

    df = scrape_cl_finals_transfermarkt()

    if not df.empty:
        out = "data/tfmkt_champions_finals_alltime.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print("   Registros:", df.shape[0])
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

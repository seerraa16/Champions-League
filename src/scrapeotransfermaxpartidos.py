import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

URL = "https://www.transfermarkt.es/uefa-champions-league/rekordspieler/pokalwettbewerb/CL"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def to_int(text):
    """Convierte texto a int o devuelve None si no se puede."""
    text = (text or "").strip()
    if text == "":
        return None
    # Por si viene con puntos como separador de miles: 15.758
    text = text.replace(".", "").replace(",", "")
    return int(text) if text.isdigit() else None


def scrape_most_appearances_transfermarkt():
    print(f"🌍 Descargando jugadores con más partidos de Champions:\n{URL}")

    # 1) Descargar HTML
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    print("   Status code:", resp.status_code)
    resp.raise_for_status()

    # 2) Parsear con BeautifulSoup
    soup = BeautifulSoup(resp.text, "lxml")

    # 3) Localizar la tabla principal
    table = soup.find("table", class_="items")
    if table is None:
        print("❌ No se encontró la tabla con class='items'")
        return pd.DataFrame()

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")  # aquí no siempre hay odd/even, por si acaso

    records = []

    for row in rows:
        # Saltar filas sin <td> de datos
        tds = row.find_all("td", recursive=False)
        if len(tds) < 7:
            continue

        # --- Rank ---
        rank = to_int(tds[0].get_text(strip=True))

        # --- Jugador + posición (tabla interna en la celda) ---
        player_cell = tds[1]

        name_tag = player_cell.find("a", title=True)
        if name_tag:
            player_name = name_tag["title"].strip()
            player_url = "https://www.transfermarkt.es" + name_tag["href"]
        else:
            player_name = player_cell.get_text(" ", strip=True)
            player_url = None

        # Posición = segunda fila de la inline-table
        position = None
        inline_table = player_cell.find("table", class_="inline-table")
        if inline_table:
            inner_rows = inline_table.find_all("tr")
            if len(inner_rows) >= 2:
                pos_td = inner_rows[1].find("td")
                if pos_td:
                    position = pos_td.get_text(strip=True)

        # --- País (bandera) ---
        nationality = None
        flag_img = tds[2].find("img")
        if flag_img:
            nationality = flag_img.get("title") or flag_img.get("alt")
            if nationality:
                nationality = nationality.strip()

        # --- Club / nº de clubes (texto del <a>) ---
        club_cell = tds[3]
        club_text = club_cell.get_text(strip=True) or None

        # --- Minutos jugados ---
        minutes = to_int(tds[4].get_text(strip=True))

        # --- Goles (texto del <a>) ---
        goals_text = tds[5].get_text(strip=True)
        goals = to_int(goals_text)

        # --- Alineaciones (partidos) ---
        matches_text = tds[6].get_text(strip=True)
        matches = to_int(matches_text)

        records.append({
            "Rank": rank,
            "Player": player_name,
            "Player_url": player_url,
            "Position": position,
            "Country": nationality,
            "Clubs_info": club_text,   # ej. "3 Clubes" o nombre de club
            "Minutes": minutes,
            "Goals": goals,
            "Matches": matches,
        })

    df = pd.DataFrame(records)
    df = df.dropna(subset=["Rank"]).sort_values("Rank").reset_index(drop=True)

    return df


if __name__ == "__main__":
    print("📊 Scrapeando 'Most Appearances' de Transfermarkt...")

    os.makedirs("data", exist_ok=True)

    df = scrape_most_appearances_transfermarkt()

    if not df.empty:
        out = "data/tfmkt_most_appearances_alltime.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print(f"   Registros: {df.shape[0]}")
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

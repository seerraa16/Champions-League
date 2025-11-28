import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

URL = "https://www.transfermarkt.es/uefa-champions-league/ewigetorschuetzenliste/pokalwettbewerb/CL"

HEADERS = {
    # User-Agent "humano" para evitar bloqueos tontos
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
    # por si viniera con puntos tipo '1.234'
    text = text.replace(".", "").replace(",", "")
    return int(text) if text.isdigit() else None


def scrape_top_scorers_transfermarkt():
    print(f"🌍 Descargando máximos goleadores históricos:\n{URL}")

    # 1) Descargar HTML
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    print("   Status code:", resp.status_code)
    resp.raise_for_status()

    # 2) Parsear con BeautifulSoup
    soup = BeautifulSoup(resp.text, "lxml")

    # 3) Localizar la tabla principal de datos
    table = soup.find("table", class_="items")
    if table is None:
        print("❌ No se encontró la tabla con class='items'")
        return pd.DataFrame()

    tbody = table.find("tbody")
    rows = tbody.find_all("tr", class_=["odd", "even"])  # filas de jugadores

    records = []

    # 4) Recorrer cada fila (jugador)
    for row in rows:
        # Solo los <td> de primer nivel (no los de la tabla interna)
        tds = row.find_all("td", recursive=False)
        if len(tds) < 8:
            # Por si hubiese alguna fila rara/agrupadora
            continue

        # --- Rank ---
        rank_text = tds[0].get_text(strip=True)
        rank = to_int(rank_text)

        # --- Jugador + posición (celda con tabla interna) ---
        player_cell = tds[1]

        # Nombre del jugador: <a title="Nombre">
        name_tag = player_cell.find("a", title=True)
        if name_tag:
            player_name = name_tag["title"].strip()
        else:
            # fallback: texto plano
            player_name = player_cell.get_text(" ", strip=True)

        # Posición: segunda fila de la tabla interna
        position = None
        inline_table = player_cell.find("table", class_="inline-table")
        if inline_table:
            inner_rows = inline_table.find_all("tr")
            if len(inner_rows) >= 2:
                pos_td = inner_rows[1].find("td")
                if pos_td:
                    position = pos_td.get_text(strip=True)

        # --- Clubs_info (Ej. 'Para 3 clubes') ---
        clubs_info = tds[2].get_text(strip=True) or None

        # --- Nacionalidad (title o alt de la bandera) ---
        nationality = None
        flag_img = tds[3].find("img")
        if flag_img:
            nationality = flag_img.get("title") or flag_img.get("alt")
            if nationality:
                nationality = nationality.strip()

        # --- Edad, temporadas, partidos, goles ---
        age = to_int(tds[4].get_text(strip=True))
        seasons = to_int(tds[5].get_text(strip=True))
        matches = to_int(tds[6].get_text(strip=True))
        goals = to_int(tds[7].get_text(strip=True))

        records.append({
            "Rank": rank,
            "Player": player_name,
            "Position": position,
            "Clubs_info": clubs_info,
            "Nationality": nationality,
            "Age": age,
            "Seasons": seasons,
            "Matches": matches,
            "Goals": goals,
        })

    # 5) Convertir a DataFrame
    df = pd.DataFrame(records)

    # Ordenar por Rank por si acaso
    df = df.dropna(subset=["Rank"]).sort_values("Rank").reset_index(drop=True)

    return df


if __name__ == "__main__":
    print("📊 Scrapeando 'Top Scorers All Time' de Transfermarkt...")

    os.makedirs("data", exist_ok=True)

    df = scrape_top_scorers_transfermarkt()

    if not df.empty:
        out = "data/tfmkt_topscorers_alltime.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print(f"   Registros: {df.shape[0]}")
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

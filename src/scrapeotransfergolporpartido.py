import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# 👇 Puedes usar la misma URL de "rekordspieler" de Champions.
# Si en Transfermarkt cambias el orden a "goles por partido",
# copia-pega aquí la URL que te salga en el navegador.
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


def to_float(text):
    """Convierte texto a float, tolerando puntos/comas y miles."""
    text = (text or "").strip()
    if text == "":
        return None
    # Primero quitamos separadores de miles tipo 1.234 ó 1,234
    # y luego usamos el último separador como decimal si lo hay.
    # Para números tipo '0,75' → 0.75
    text = text.replace(" ", "")
    # Si contiene coma pero no punto, asumimos coma decimal:
    if "," in text and "." not in text:
        text = text.replace(".", "")  # por si acaso miles
        text = text.replace(",", ".")
    else:
        # Si hay punto como decimal, solo quitamos comas de miles
        parts = text.split(".")
        if len(parts) > 2:  # algo tipo 15.758.0 (raro)
            text = text.replace(".", "")
        text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def scrape_goals_per_match_transfermarkt():
    print(f"🌍 Descargando jugadores de Champions (para goles/partido):\n{URL}")

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
    rows = tbody.find_all("tr")

    records = []

    for row in rows:
        tds = row.find_all("td", recursive=False)
        # En esta tabla esperamos:
        # 0: rank
        # 1: jugador (tabla interna)
        # 2: país
        # 3: club / nº de clubes
        # 4: minutos jugados
        # 5: goles
        # 6: alineaciones (partidos)
        if len(tds) < 7:
            continue

        # --- Rank ---
        rank = to_int(tds[0].get_text(strip=True))

        # --- Jugador + posición ---
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

        # --- País ---
        nationality = None
        flag_img = tds[2].find("img")
        if flag_img:
            nationality = flag_img.get("title") or flag_img.get("alt")
            if nationality:
                nationality = nationality.strip()

        # --- Club / nº de clubes ---
        club_text = tds[3].get_text(strip=True) or None

        # --- Minutos (por si luego quieres usarlos) ---
        minutes = to_int(tds[4].get_text(strip=True))

        # --- Goles ---
        goals = to_int(tds[5].get_text(strip=True))

        # --- Partidos (alineaciones) ---
        matches = to_int(tds[6].get_text(strip=True))

        # --- Goles por partido ---
        if goals is not None and matches and matches > 0:
            goals_per_match = goals / matches
        else:
            goals_per_match = None

        records.append({
            "Rank": rank,
            "Player": player_name,
            "Player_url": player_url,
            "Position": position,
            "Country": nationality,
            "Club_info": club_text,
            "Minutes": minutes,
            "Goals": goals,
            "Matches": matches,
            "Goals_per_match": goals_per_match,
        })

    df = pd.DataFrame(records)

    # Ordenamos por goles por partido de forma descendente
    if "Goals_per_match" in df.columns:
        df = df.sort_values("Goals_per_match", ascending=False)

    # Y si quieres mantener el rank original también, lo dejamos ahí
    df = df.reset_index(drop=True)

    return df


if __name__ == "__main__":
    print("📊 Scrapeando 'Goles por partido' de Transfermarkt...")

    os.makedirs("data", exist_ok=True)

    df = scrape_goals_per_match_transfermarkt()

    if not df.empty:
        out = "data/tfmkt_goals_per_match_alltime.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print(f"   Registros: {df.shape[0]}")
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

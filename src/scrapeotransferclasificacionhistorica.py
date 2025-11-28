import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

URL = "https://www.transfermarkt.es/uefa-champions-league/ewigeTabelle/pokalwettbewerb/CL"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


def to_int_signed(text: str):
    """Convierte texto como 1.234, +56, -12 en int o None."""
    if text is None:
        return None
    text = text.strip()
    if text == "":
        return None

    # quitar separadores de miles
    text = text.replace(".", "").replace(" ", "")

    # signo
    sign = 1
    if text[0] in "+-":
        if text[0] == "-":
            sign = -1
        text = text[1:]

    if not text.isdigit():
        return None

    return sign * int(text)


def scrape_alltime_table_transfermarkt():
    print(f"🌍 Descargando clasificación histórica de la Champions:\n{URL}")

    resp = requests.get(URL, headers=HEADERS, timeout=20)
    print("   Status code:", resp.status_code)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    table = soup.find("table", class_="items")
    if table is None:
        print("❌ No se encontró la tabla con class='items'")
        return pd.DataFrame()

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    records = []

    for row in rows:
        tds = row.find_all("td", recursive=False)
        if len(tds) < 9:
            continue

        # Rank (td[0])
        rank = to_int_signed(tds[0].get_text(strip=True))

        # Club name (td[2])
        club_cell = tds[2]
        club_link = club_cell.find("a", href=True)
        club_name = club_link.get_text(strip=True) if club_link else None

        # Numerical stats
        matches = to_int_signed(tds[3].get_text(strip=True))
        wins = to_int_signed(tds[4].get_text(strip=True))
        draws = to_int_signed(tds[5].get_text(strip=True))
        losses = to_int_signed(tds[6].get_text(strip=True))
        goal_diff = to_int_signed(tds[7].get_text(strip=True))
        points = to_int_signed(tds[8].get_text(strip=True))

        records.append({
            "Rank": rank,
            "Club": club_name,
            "Matches": matches,
            "Wins": wins,
            "Draws": draws,
            "Losses": losses,
            "Goal_diff": goal_diff,
            "Points": points,
        })

    df = pd.DataFrame(records)
    df = df.dropna(subset=["Rank"]).sort_values("Rank").reset_index(drop=True)

    return df


if __name__ == "__main__":
    print("📊 Scrapeando clasificación histórica de la Champions (Transfermarkt)...")

    os.makedirs("data", exist_ok=True)

    df = scrape_alltime_table_transfermarkt()

    if not df.empty:
        out = "data/tfmkt_alltime_club_table.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\n✅ Archivo creado: {out}")
        print(f"   Registros: {df.shape[0]}")
        print("   Columnas:", list(df.columns))
        print("\nEjemplo primeras filas:\n", df.head(10))
    else:
        print("❌ No se obtuvo ningún dato.")

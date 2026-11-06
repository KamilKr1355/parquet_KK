"""Tworzy pliki Parquet i CSV z danymi z prezentacji."""
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DANE = Path(__file__).resolve().parent.parent / "data"
DANE.mkdir(exist_ok=True)

# 1. Mała tabela ze slajdów: 8 wierszy, 14 kolumn
kolumny = ["id", "data", "klient", "miasto", "woj", "kategoria", "produkt",
           "ilosc", "cena", "rabat", "kwota", "platnosc", "status", "kurier"]
wiersze = [
    (1, "14.01", "K102", "Kraków", "MP", "sport", 501, 1, 120, 0, 120, "BLIK", "nowe", "DPD"),
    (2, "14.01", "K311", "Kraków", "MP", "dom", 502, 2, 40, 0, 80, "BLIK", "nowe", "DPD"),
    (3, "15.01", "K102", "Kraków", "MP", "sport", 503, 1, 200, 0, 200, "karta", "nowe", "InPost"),
    (4, "15.01", "K451", "Tarnów", "MP", "moda", 504, 1, 50, 0, 50, "BLIK", "nowe", "InPost"),
    (5, "16.01", "K451", "Tarnów", "MP", "dom", 505, 1, 100, 10, 90, "karta", "wysłane", "DPD"),
    (6, "16.01", "K230", "Kraków", "MP", "sport", 506, 3, 50, 0, 150, "BLIK", "wysłane", "DPD"),
    (7, "17.01", "K887", "Warszawa", "MZ", "moda", 507, 2, 600, 0, 1200, "karta", "wysłane", "DHL"),
    (8, "17.01", "K887", "Warszawa", "MZ", "dom", 508, 1, 70, 0, 70, "BLIK", "wysłane", "DHL"),
]
mala = pa.table({n: [w[i] for w in wiersze] for i, n in enumerate(kolumny)})
pq.write_table(mala, str(DANE / "zamowienia.parquet"), row_group_size=4, compression="NONE")
pacsv.write_csv(mala, str(DANE / "zamowienia.csv"))

# 2. Duża tabela ze slajdu 15: 1 mln wierszy, losowe dane, stałe ziarno
r = np.random.default_rng(42)
N = 1_000_000
wybor = lambda wartosci: pa.array(r.choice(wartosci, N).tolist())
duza = pa.table({
    "id": np.arange(1, N + 1),
    "data": pa.array((np.datetime64("2026-01-01") + r.integers(0, 365, N).astype("timedelta64[D]")).astype("datetime64[D]")),
    "klient": r.integers(1, 50001, N),
    "miasto": wybor(["Kraków", "Tarnów", "Warszawa", "Gdańsk", "Poznań", "Wrocław", "Łódź", "Lublin"]),
    "woj": wybor(["MP", "MZ", "PM", "WP", "DS", "LD", "LU"]),
    "kategoria": wybor(["książki", "elektronika", "dom", "sport", "moda", "zabawki"]),
    "produkt": r.integers(1, 5001, N),
    "ilosc": r.integers(1, 11, N),
    "cena": r.integers(5, 1001, N),
    "rabat": r.choice([0, 5, 10, 15, 20], N),
    "kwota": r.integers(1, 5001, N),
    "platnosc": wybor(["karta", "BLIK", "przelew", "gotówka"]),
    "status": wybor(["nowe", "opłacone", "wysłane", "dostarczone", "zwrócone"]),
    "kurier": wybor(["DPD", "InPost", "DHL", "Poczta"]),
})
pq.write_table(duza, str(DANE / "zamowienia_1mln.parquet"), row_group_size=100_000, compression="SNAPPY")
pacsv.write_csv(duza, str(DANE / "zamowienia_1mln.csv"))

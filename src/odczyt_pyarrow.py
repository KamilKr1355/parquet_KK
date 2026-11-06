"""Odczyt plików z prezentacji i podgląd struktury Parqueta w PyArrow.

Instalacja: pip install -r requirements.txt
Uruchomienie: python src/generuj_dane.py, potem python src/odczyt_pyarrow.py
"""
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DANE = Path(__file__).resolve().parent.parent / "data"
MALY = str(DANE / "zamowienia.parquet")
DUZY = str(DANE / "zamowienia_1mln.parquet")
MALY_CSV = str(DANE / "zamowienia.csv")
DUZY_CSV = str(DANE / "zamowienia_1mln.csv")
MB = 1e6

# 1. Zwykły odczyt danych
tabela = pq.read_table(MALY)
print(tabela.to_pandas().to_string(index=False))
print("Suma kwot:", sum(tabela["kwota"].to_pylist()))

# 2. Struktura pliku: same metadane, bez czytania danych
meta = pq.read_metadata(MALY)
rozmiar_pliku = os.path.getsize(MALY)
rozmiar_stopki = meta.serialized_size

print("\n=== UKŁAD PLIKU PARQUET ===")
print("""  +--------------------------------------------+
  | PAR1                  (4 B, znacznik początku)|
  | Row group 0           (blok kilku wierszy)    |
  |   Column chunk: id      (cała kolumna w grupie)|
  |   Column chunk: data                           |
  |   ...                                          |
  | Row group 1                                    |
  |   Column chunk ...                             |
  | FOOTER                (metadane: schemat, row  |
  |                        groups, offsety, min/max)|
  | długość footera       (4 B)                    |
  | PAR1                  (4 B, znacznik końca)    |
  +--------------------------------------------+""")
print("Row group    = poziomy kawałek tabeli (np. 4 wiersze), czytany niezależnie.")
print("Column chunk = wszystkie wartości JEDNEJ kolumny w obrębie JEDNEJ row group.")
print("Footer       = spis treści na końcu pliku; czytnik zaczyna od niego.")

print("\n=== FOOTER (stopka) ===")
print(f"Rozmiar pliku:       {rozmiar_pliku} B")
print(f"Rozmiar footera:     {rozmiar_stopki} B ({rozmiar_stopki / rozmiar_pliku:.0%} pliku)")
print(f"Początek footera:    bajt {rozmiar_pliku - rozmiar_stopki - 8} "
      f"(plik kończy się: footer + 4 B długości + 4 B 'PAR1')")
print(f"Liczba wierszy:      {meta.num_rows}")
print(f"Liczba kolumn:       {meta.num_columns}")
print(f"Liczba row groups:   {meta.num_row_groups}")
print(f"Wersja formatu:      {meta.format_version}")
print(f"Twórca:              {meta.created_by}")
print(f"Kolejność bajtów:    {getattr(meta, 'is_little_endian', 'nieznana') and 'little-endian'}")
razem_skomp = sum(meta.row_group(g).column(c).total_compressed_size
                  for g in range(meta.num_row_groups) for c in range(meta.num_columns))
razem_nieskomp = sum(meta.row_group(g).column(c).total_uncompressed_size
                     for g in range(meta.num_row_groups) for c in range(meta.num_columns))
print(f"Dane (suma column chunków): {razem_skomp} B skompresowane, {razem_nieskomp} B po rozpakowaniu")
print(f"Pozostałe:           {rozmiar_pliku - razem_skomp - rozmiar_stopki} B "
      f"(2 x 'PAR1' + 4 B długości footera; nagłówki stron są już w column chunkach)")
for klucz, wartosc in (meta.metadata or {}).items():
    tekst = wartosc.decode("utf-8", "replace")
    print(f"Metadane klucz-wartość: {klucz.decode()} = {tekst[:60]}{'...' if len(tekst) > 60 else ''} "
          f"({len(wartosc)} B)")

print("\n=== SCHEMAT (z footera) ===")
print(f"  {'nr':>2}  {'kolumna':10} {'typ fizyczny':12} {'typ logiczny':14} {'def':>3} {'rep':>3}  ścieżka")
for i in range(meta.num_columns):
    kol = meta.schema.column(i)
    print(f"  {i:2}  {kol.name:10} {kol.physical_type:12} {str(kol.logical_type):14} "
          f"{kol.max_definition_level:3} {kol.max_repetition_level:3}  {kol.path}")
print("  def = maks. poziom definicji (1 = kolumna może mieć null), rep = poziom powtórzeń (0 = brak list)")

for g in range(meta.num_row_groups):
    grupa = meta.row_group(g)
    kolumny = [grupa.column(c) for c in range(grupa.num_columns)]
    skomp = sum(k.total_compressed_size for k in kolumny)
    nieskomp = sum(k.total_uncompressed_size for k in kolumny)
    print(f"\n=== ROW GROUP {g} ===")
    print(f"  Liczba wierszy:            {grupa.num_rows}")
    print(f"  Liczba column chunków:     {grupa.num_columns}")
    print(f"  total_byte_size:           {grupa.total_byte_size} B (rozmiar po rozpakowaniu wg metadanych)")
    print(f"  Suma column chunków:       {skomp} B skompresowane / {nieskomp} B po rozpakowaniu")
    print(f"  Zakres w pliku:            bajty {min(k.dictionary_page_offset or k.data_page_offset for k in kolumny)}"
          f" - {max((k.dictionary_page_offset or k.data_page_offset) + k.total_compressed_size for k in kolumny)}")
    print(f"  Sortowanie:                {grupa.sorting_columns or 'brak zadeklarowanego'}")

    print("\n  Układ column chunków (gdzie leżą w pliku):")
    print(f"  {'kolumna':10} {'wartości':>8} {'offs.słown.':>11} {'offs.danych':>11} "
          f"{'skomp.':>6} {'rozp.':>6} {'kompresja':>12}  strona słownika?")
    for k in kolumny:
        slow = k.dictionary_page_offset if k.has_dictionary_page else "-"
        print(f"  {k.path_in_schema:10} {k.num_values:8} {str(slow):>11} {k.data_page_offset:11} "
              f"{k.total_compressed_size:6} {k.total_uncompressed_size:6} {k.compression:>12}  "
              f"{'tak' if k.has_dictionary_page else 'nie'}")

    print("\n  Statystyki i kodowania (po nich czytnik pomija row groups):")
    print(f"  {'kolumna':10} {'min':>8} {'max':>8} {'null':>4} {'distinct':>8}  kodowania")
    for k in kolumny:
        s = k.statistics
        if s is None or not s.has_min_max:
            print(f"  {k.path_in_schema:10} {'brak statystyk':>30}  {', '.join(k.encodings)}")
            continue
        distinct = s.distinct_count if getattr(s, "has_distinct_count", False) else "-"
        print(f"  {k.path_in_schema:10} {str(s.min):>8} {str(s.max):>8} {s.null_count:4} "
              f"{str(distinct):>8}  {', '.join(k.encodings)}")

# 2a. Min i max per row group: row groups obok siebie
def min_max(k):
    s = k.statistics
    return f"{s.min} .. {s.max}" if s is not None and s.has_min_max else "brak"


def tabela_min_max(sciezka, kolumny=None, limit_grup=None):
    m = pq.read_metadata(sciezka)
    ile_grup = min(m.num_row_groups, limit_grup or m.num_row_groups)
    nazwy = kolumny or [m.schema.column(i).name for i in range(m.num_columns)]
    wiersze = [[n] + [min_max(next(m.row_group(g).column(c) for c in range(m.num_columns)
                                  if m.row_group(g).column(c).path_in_schema == n))
                      for g in range(ile_grup)] for n in nazwy]
    szer = [max(len(w[i]) for w in wiersze + [["kolumna"] + [f"row group {g}" for g in range(ile_grup)]])
            for i in range(ile_grup + 1)]
    naglowek = ["kolumna"] + [f"row group {g}" for g in range(ile_grup)]
    print("  " + " | ".join(h.ljust(szer[i]) for i, h in enumerate(naglowek)))
    print("  " + "-+-".join("-" * s for s in szer))
    for w in wiersze:
        print("  " + " | ".join(x.ljust(szer[i]) for i, x in enumerate(w)))
    return m


print("\n=== MIN .. MAX PER ROW GROUP (mały plik, 8 wierszy) ===")
m_maly = tabela_min_max(MALY)
print("\nLiczba wierszy w row groups:",
      [m_maly.row_group(g).num_rows for g in range(m_maly.num_row_groups)])

print("\nPomijanie row groups dzięki min/max, np. filtr id > 5:")
for g in range(m_maly.num_row_groups):
    s = m_maly.row_group(g).column(0).statistics          # kolumna 0 = id
    wynik = "CZYTAM" if s.max > 5 else "POMIJAM (max <= 5, nic nie pasuje)"
    print(f"  row group {g}: id {s.min}..{s.max} -> {wynik}")

meta_duzy = pq.read_metadata(DUZY)
print(f"\n=== MIN .. MAX PER ROW GROUP (duży plik, {meta_duzy.num_rows:,} wierszy, "
      f"{meta_duzy.num_row_groups} row groups po {meta_duzy.row_group(0).num_rows:,}) ===".replace(",", " "))
tabela_min_max(DUZY, ["id", "klient", "cena", "kwota", "miasto"], limit_grup=5)
print("  (pokazano 5 pierwszych row groups). id rośnie, więc zakresy się nie nakładają:"
      " filtr po id pominie prawie wszystkie grupy.")
print("  klient/cena/kwota są losowe: każda grupa ma niemal pełny zakres, więc min/max tu nie pomaga.")

# 2b. Strony (pages) w column chunku: strona słownika i strony danych.
# PyArrow nie pokazuje stron w API, więc czytamy surowe bajty i dekodujemy
# nagłówki stron (format Thrift Compact) oraz zawartość (plik jest bez kompresji).
import struct

TYPY_STRON = {0: "DATA_PAGE", 1: "INDEX_PAGE", 2: "DICTIONARY_PAGE", 3: "DATA_PAGE_V2"}
KODOWANIA = {0: "PLAIN", 2: "PLAIN_DICTIONARY", 3: "RLE", 4: "BIT_PACKED", 8: "RLE_DICTIONARY"}


def varint(bufor, poz):
    wynik, przesuniecie = 0, 0
    while True:
        b = bufor[poz]
        poz += 1
        wynik |= (b & 0x7F) << przesuniecie
        if not b & 0x80:
            return wynik, poz
        przesuniecie += 7


def thrift_struct(bufor, poz):
    """Minimalny dekoder Thrift Compact: zwraca ({id_pola: wartość}, nowa_pozycja)."""
    pola, id_pola = {}, 0
    while True:
        b = bufor[poz]
        poz += 1
        if b == 0:
            return pola, poz
        typ, delta = b & 0x0F, b >> 4
        if delta:
            id_pola += delta
        else:
            z, poz = varint(bufor, poz)
            id_pola = (z >> 1) ^ -(z & 1)
        if typ in (1, 2):                      # bool zakodowany w typie
            pola[id_pola] = typ == 1
        elif typ in (3, 4, 5, 6):              # liczby całkowite (zigzag)
            z, poz = varint(bufor, poz)
            pola[id_pola] = (z >> 1) ^ -(z & 1)
        elif typ == 8:                         # dane binarne (np. min/max w statystykach)
            dl, poz = varint(bufor, poz)
            pola[id_pola] = bytes(bufor[poz:poz + dl])
            poz += dl
        elif typ == 12:                        # zagnieżdżona struktura
            pola[id_pola], poz = thrift_struct(bufor, poz)
        else:
            raise ValueError(f"nieobsługiwany typ Thrift {typ}")


def dekoduj_rle(bufor, szerokosc_bitow, ile):
    """Dekoder RLE/bit-packed hybrid (tak zapisane są indeksy słownika)."""
    wartosci, poz = [], 0
    while len(wartosci) < ile:
        h, poz = varint(bufor, poz)
        if h & 1:                              # paczki po 8 wartości upakowane bitowo
            liczba = (h >> 1) * 8
            bajty = (h >> 1) * szerokosc_bitow
            bity = int.from_bytes(bufor[poz:poz + bajty], "little")
            poz += bajty
            maska = (1 << szerokosc_bitow) - 1
            wartosci += [(bity >> (i * szerokosc_bitow)) & maska for i in range(liczba)]
        else:                                  # powtórzenie jednej wartości
            liczba = h >> 1
            szer_bajtow = (szerokosc_bitow + 7) // 8
            w = int.from_bytes(bufor[poz:poz + szer_bajtow], "little")
            poz += szer_bajtow
            wartosci += [w] * liczba
    return wartosci[:ile]


def dekoduj_plain(bufor, typ, ile):
    wartosci, poz = [], 0
    for _ in range(ile):
        if typ == "INT64":
            wartosci.append(struct.unpack_from("<q", bufor, poz)[0])
            poz += 8
        else:                                  # BYTE_ARRAY: 4 B długości + bajty
            dl = struct.unpack_from("<I", bufor, poz)[0]
            wartosci.append(bufor[poz + 4:poz + 4 + dl].decode("utf-8"))
            poz += 4 + dl
    return wartosci


def pokaz_strony(sciezka, nr_grupy, kolumna):
    meta = pq.read_metadata(sciezka)
    k = next(meta.row_group(nr_grupy).column(c) for c in range(meta.num_columns)
             if meta.row_group(nr_grupy).column(c).path_in_schema == kolumna)
    start = k.dictionary_page_offset if k.has_dictionary_page else k.data_page_offset
    with open(sciezka, "rb") as f:
        f.seek(start)
        chunk = f.read(k.total_compressed_size)
    print(f"\n--- Row group {nr_grupy}, column chunk '{kolumna}' ({k.physical_type}, "
          f"{k.total_compressed_size} B od offsetu {start}, kompresja: {k.compression}) ---")

    poz, slownik = 0, None
    while poz < len(chunk):
        naglowek, koniec = thrift_struct(chunk, poz)
        typ = TYPY_STRON[naglowek[1]]
        rozmiar = naglowek[3]
        tresc = chunk[koniec:koniec + rozmiar]
        print(f"  [{typ}] offset={start + poz}  nagłówek={koniec - poz} B  "
              f"dane={rozmiar} B (po rozpakowaniu {naglowek[2]} B)")
        if naglowek[1] == 2:                   # strona słownika
            opis = naglowek[7]
            slownik = dekoduj_plain(tresc, k.physical_type, opis[1])
            print(f"      liczba wartości: {opis[1]}, kodowanie: {KODOWANIA[opis[2]]}")
            print("      słownik (indeks -> wartość):", dict(enumerate(slownik)))
        elif naglowek[1] == 0:                 # strona danych v1
            opis = naglowek[5]
            n = opis[1]
            print(f"      liczba wartości: {n}, kodowanie: {KODOWANIA[opis[2]]}, "
                  f"poziomy definicji: {KODOWANIA[opis[3]]}")
            dl = struct.unpack_from("<I", tresc, 0)[0]            # poziomy definicji (null / nie-null)
            poziomy = dekoduj_rle(tresc[4:4 + dl], 1, n)
            reszta = tresc[4 + dl:]
            if opis[2] in (2, 8) and slownik is not None:
                szer = reszta[0]                                   # 1. bajt = szerokość indeksu w bitach
                indeksy = dekoduj_rle(reszta[1:], szer, n)
                print(f"      poziomy definicji (1 = wartość jest): {poziomy}")
                print(f"      indeksy do słownika ({szer} bit/wartość): {indeksy}")
                print(f"      odtworzone wartości: {[slownik[i] for i in indeksy]}")
            else:
                print(f"      poziomy definicji (1 = wartość jest): {poziomy}")
                print(f"      wartości PLAIN: {dekoduj_plain(reszta, k.physical_type, n)}")
        poz = koniec + rozmiar


print("\n=== STRONY: STRONA SŁOWNIKA I STRONA DANYCH ===")
print("Column chunk = [strona słownika] + [strona danych] (+ kolejne strony danych przy dużej kolumnie).")
print("Słownik trzyma unikalne wartości; strona danych trzyma tylko małe indeksy do niego.")
for kolumna in ("miasto", "kategoria", "kwota"):
    pokaz_strony(MALY, 0, kolumna)

# 3. Rozmiar każdej kolumny w dużym pliku (slajd 15)
meta = pq.read_metadata(DUZY)
rozmiary = {}
for g in range(meta.num_row_groups):
    for c in range(meta.num_columns):
        k = meta.row_group(g).column(c)
        rozmiary[k.path_in_schema] = rozmiary.get(k.path_in_schema, 0) + k.total_compressed_size
print(f"\n=== ROZMIARY KOLUMN: {DUZY} ===")
for nazwa, b in sorted(rozmiary.items(), key=lambda x: -x[1]):
    print(f"{nazwa:10} {b / MB:7.2f} MB")
print(f"{'razem':10} {sum(rozmiary.values()) / MB:7.2f} MB")


# 4. Ile bajtów naprawdę czytamy: plik z licznikiem odczytów
class PlikZLicznikiem:
    def __init__(self, sciezka):
        self.f = open(sciezka, "rb")
        self.odczytane = 0
        self.closed = False
        self.mode = "rb"

    def read(self, n=-1):
        dane = self.f.read(n)
        self.odczytane += len(dane)
        return dane

    def seek(self, pozycja, skad=0):
        return self.f.seek(pozycja, skad)

    def tell(self):
        return self.f.tell()

    def seekable(self):
        return True

    def readable(self):
        return True

    def writable(self):
        return False

    def close(self):
        self.closed = True
        self.f.close()


def zmierz(opis, kolumny=None, filtr=None):
    plik = PlikZLicznikiem(DUZY)
    wynik = pq.read_table(pa.PythonFile(plik, mode="r"), columns=kolumny, filters=filtr, use_threads=False)
    print(f"{opis:32} {plik.odczytane / MB:7.2f} MB, wierszy: {wynik.num_rows}")


print("\n=== FAKTYCZNIE ODCZYTANE BAJTY ===")
zmierz("Wszystkie kolumny")
zmierz("Tylko kwota", ["kwota"])
zmierz("Tylko kurier", ["kurier"])
zmierz("Tylko id", ["id"])
zmierz("miasto + kwota", ["miasto", "kwota"])

# 5. Porównanie z CSV
def ludzko(b):
    """Bajty w czytelnej jednostce (B, KB, MB)."""
    if b < 1000:
        return f"{b} B"
    if b < 1e6:
        return f"{b / 1e3:.2f} KB"
    return f"{b / 1e6:.2f} MB"


def porownaj(opis, parquet, csv):
    p, c = os.path.getsize(parquet), os.path.getsize(csv)
    print(f"{opis:20} Parquet: {ludzko(p):>9}   CSV: {ludzko(c):>9}   CSV/Parquet: {c / p:.2f}x")


print("\n=== PARQUET A CSV ===")
porownaj("8 rekordów", MALY, MALY_CSV)
porownaj("1 000 000 rekordów", DUZY, DUZY_CSV)

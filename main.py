import pandas as pd
import sqlite3

# Fájl beolvasása
file_path = 'resources/Adagok_corrected.csv'

# Delimiter beállítása
data = pd.read_csv(file_path, ';')

# Üres sorok törlése
data = data.dropna(how='all')

# Az utolsó előtti, redundáns oszlop törlése
data = data.drop(data.columns[-2], axis=1)

# Első és utolsó oszlop int-té alakítása, hogy egész szám legyen
data[data.columns[0]] = data[data.columns[0]].astype(int)
data[data.columns[-1]] = data[data.columns[-1]].astype(int)

# Dátum és idő mezők konvertálása
data['KEZDET_DÁTUM'] = pd.to_datetime(data.iloc[:, 1] + ' ' + data.iloc[:, 2])
data['VÉGE_DÁTUM'] = pd.to_datetime(data.iloc[:, 3] + ' ' + data.iloc[:, 4])

# Felesleges dátum és idő oszlopok törlése
data = data.drop(data.columns[[1, 2, 4]], axis=1)

# Oszlop sorrendezés, a kezdő dátum előrébb kerül mint a vég dátum
last_column = data.columns[-1]
cols = data.columns.tolist()
cols.remove(last_column)
cols.insert(1, last_column)
data = data[cols]

# Felesleges adagidő column törlése
data = data.drop(data.columns[3], axis=1)

# print(data)


file_path = 'resources/Hűtőpanelek.csv'  # a csv fájl elérési útvonala
df = pd.read_csv(file_path, sep=';')

# Map-ek létrehozása minden panelhez, kivéve a 7-est
panel_maps = {}

# Feltételezve, hogy minden panelhoz 2 oszlop tartozik: az idő ('Time') és a hőmérséklet ('ValueY')
# Itt 14 panelt feltételezünk, tehát 28 oszlop van összesen (idő és hőmérséklet párokban)
num_panels = 14

for i in range(num_panels):
    if i + 1 == 7:  # Kihagyjuk a 7-es panelt
        continue

    time_col = f"Panel hőfok {i + 1} [°C] Time"
    value_col = f"Panel hőfok {i + 1} [°C] ValueY"

    # Dátum oszlop konvertálása "YYYY-MM-DD HH:MM:SS" formátumra
    df[time_col] = pd.to_datetime(
        df[time_col].str.replace(r"\.", "-", regex=True),  # Pontok cseréje kötőjelre
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"  # Hibás formátum esetén NaT érték használata
    )

    panel_map = []  # Minden pár tárolására létrehozott lista

    # Adatok hozzáadása a panel_map-hez
    for time, value in zip(df[time_col], df[value_col]):
        if pd.isna(time):
            continue  # Hibás dátumokat kihagyjuk
        pair = (time.strftime("%Y-%m-%d %H:%M:%S"), value)  # Formázott idő használata
        panel_map.append(pair)

    # Panel map elmentése
    panel_maps[f'Panel {i + 1}'] = panel_map

conn = sqlite3.connect('resources/Adatbazis_beadando_ZP.db')  # Az adatbázis fájl elérése
cur = conn.cursor()

# Többszöri, egymás után történő futtatás esetén kukázza az adatbázist, így mindig újra lehet tölteni
# Vagy az összes tábla adattartalma törlődik, vagy egyik sem. Emellett újraindítja az ID számlálót
cur.execute('BEGIN TRANSACTION; ')
cur.execute("DELETE FROM ADAG")
cur.execute("DELETE FROM PANELOK")
cur.execute("DELETE FROM MERESEK")
# Az ID counter visszaállítása
cur.execute("DELETE FROM sqlite_sequence WHERE name='ADAG';")
cur.execute("DELETE FROM sqlite_sequence WHERE name='PANELOK';")
cur.execute("DELETE FROM sqlite_sequence WHERE name='MERESEK';")
conn.commit()
cur.execute("VACUUM;")

for index, (panel_name, measurements) in enumerate(panel_maps.items(), start=1):
    panel_nev = f'Panel {index}'

    # Panel beszúrása a PANELOK táblába
    cur.execute("INSERT INTO PANELOK (nev) VALUES (?)", (panel_nev,))
    panel_id = cur.lastrowid  # Visszakapjuk a beszúrt panel azonosítóját (panel_id)

    # Mérések beszúrása a MÉRÉSEK táblába
    for time, value in measurements:
        cur.execute("""
            INSERT INTO MERESEK (panel_id, ido, hofok)
            VALUES (?, ?, ?)
            """,
                    (panel_id, time, value)
                    )

data['KEZDET_DÁTUM'] = data['KEZDET_DÁTUM'].astype(str)
data['VÉGE_DÁTUM'] = data['VÉGE_DÁTUM'].astype(str)

# Adagok beszúrása
for index, row in data.iterrows():
    kezdet_datum = row['KEZDET_DÁTUM']
    veg_datum = row['VÉGE_DÁTUM']

    cur.execute("""
        INSERT INTO ADAG (KEZDET_DATUM, VEG_DATUM)
        VALUES (?, ?)
        """,
                (kezdet_datum, veg_datum)
                )

conn.commit()


# Az egyes adagokhoz tartozó mérések maximális és minimális hőmérsékletét
cur.execute("""
    SELECT a.ID AS Adag_ID, MAX(m.HOFOK) AS Max_hofok, MIN(m.HOFOK) AS Min_hofok
    FROM ADAG a
    JOIN MERESEK m ON m.IDO BETWEEN a.KEZDET_DATUM AND a.VEG_DATUM
    GROUP BY a.ID
    ORDER BY Max_hofok
""")

rows = cur.fetchall()
dataFrame = pd.DataFrame(rows, columns=["Adag_ID", "Max_hofok", "Min_hofok"])

print(dataFrame)

# DuckDB Setup Guide für Search Analytics

Diese Anleitung führt dich Schritt für Schritt durch die Installation und Nutzung von DuckDB für deine Search Analytics Daten.

---

## Inhaltsverzeichnis

1. [Was ist DuckDB?](#1-was-ist-duckdb)
2. [Installation](#2-installation)
3. [Erste Schritte - Die Basics](#3-erste-schritte---die-basics)
4. [Datenbank und Tabellen erstellen](#4-datenbank-und-tabellen-erstellen)
5. [Daten importieren](#5-daten-importieren)
6. [SQL Queries - Die wichtigsten Befehle](#6-sql-queries---die-wichtigsten-befehle)
7. [DuckDB mit Python nutzen](#7-duckdb-mit-python-nutzen)
8. [UI Tools für DuckDB](#8-ui-tools-für-duckdb)
9. [Tipps und Best Practices](#9-tipps-und-best-practices)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Was ist DuckDB?

DuckDB ist eine **eingebettete analytische Datenbank** - ähnlich wie SQLite, aber optimiert für analytische Queries (Aggregationen, Joins über große Datenmengen).

### Vorteile für dich:

| Feature | Bedeutung |
|---------|-----------|
| **Keine Server-Installation** | Läuft direkt in Python, keine Hintergrundprozesse |
| **Eine Datei = Datenbank** | Die komplette DB ist eine `.db` Datei (einfach zu kopieren/backupen) |
| **SQL-kompatibel** | Standard SQL wie PostgreSQL/MySQL |
| **Extrem schnell** | Optimiert für analytische Queries über Millionen von Zeilen |
| **Liest alles** | CSV, Parquet, JSON, Excel direkt - ohne Import |

### Wie es funktioniert:

```
┌─────────────────────────────────────────────────────────┐
│  Dein Python Script / Jupyter Notebook                  │
│                                                         │
│    import duckdb                                        │
│    con = duckdb.connect('analytics.db')  ◄── Verbindung │
│    con.execute("SELECT * FROM searches")               │
│                                                         │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  analytics.db  (Eine einzelne Datei auf deiner Platte)  │
│                                                         │
│    ┌──────────────┐  ┌──────────────┐                  │
│    │   searches   │  │    users     │   ◄── Tabellen   │
│    └──────────────┘  └──────────────┘                  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 2. Installation

### Schritt 1: Anaconda Prompt öffnen

- Windows: Start → "Anaconda Prompt" suchen und öffnen
- Mac/Linux: Terminal öffnen (Anaconda sollte bereits im PATH sein)

### Schritt 2: DuckDB installieren

```bash
# DuckDB und Python-Bindings installieren
conda install -c conda-forge python-duckdb

# Optional: CLI Tool (für Terminal-Nutzung ohne Python)
conda install -c conda-forge duckdb
```

### Schritt 3: Installation prüfen

```bash
python -c "import duckdb; print(duckdb.__version__)"
```

Wenn eine Versionsnummer erscheint (z.B. `0.10.0`), war die Installation erfolgreich.

---

## 3. Erste Schritte - Die Basics

### Option A: Python/Jupyter (empfohlen für Anfänger)

```python
import duckdb

# Verbindung zur Datenbank herstellen
# Falls die Datei nicht existiert, wird sie automatisch erstellt
con = duckdb.connect('searchanalytics.db')

# Ein einfacher Test
result = con.execute("SELECT 'Hallo DuckDB!' AS greeting").fetchone()
print(result[0])  # Ausgabe: Hallo DuckDB!
```

### Option B: DuckDB CLI

```bash
# Datenbank öffnen (wird erstellt falls nicht vorhanden)
duckdb searchanalytics.db
```

Du siehst dann einen Prompt:
```
v0.10.0
Enter ".help" for usage hints.
D
```

Hier kannst du SQL direkt eingeben:
```sql
D SELECT 'Hallo DuckDB!' AS greeting;
┌───────────────┐
│   greeting    │
│    varchar    │
├───────────────┤
│ Hallo DuckDB! │
└───────────────┘
```

### Wichtige CLI-Befehle:

| Befehl | Beschreibung |
|--------|--------------|
| `.help` | Alle Befehle anzeigen |
| `.tables` | Alle Tabellen auflisten |
| `.schema tabelle` | Struktur einer Tabelle anzeigen |
| `.mode markdown` | Ausgabe als Markdown-Tabelle |
| `.quit` oder `Ctrl+D` | Beenden |

---

## 4. Datenbank und Tabellen erstellen

### Konzept: Was ist eine Tabelle?

Eine Tabelle ist wie eine Excel-Tabelle mit:
- **Spalten** (Columns): Definieren welche Daten gespeichert werden (Name, Typ)
- **Zeilen** (Rows): Die eigentlichen Datensätze

### Tabelle erstellen

```sql
-- Tabelle für Search Analytics erstellen
CREATE TABLE searches (
    id              INTEGER PRIMARY KEY,    -- Eindeutige ID
    timestamp       TIMESTAMP,              -- Wann wurde gesucht
    search_query    VARCHAR,                -- Der Suchbegriff
    user_id         VARCHAR,                -- Wer hat gesucht
    results_count   INTEGER,                -- Wie viele Ergebnisse
    response_time   DOUBLE,                 -- Antwortzeit in ms
    country         VARCHAR                 -- Land des Users
);
```

### Datentypen in DuckDB

| Typ | Beschreibung | Beispiel |
|-----|--------------|----------|
| `INTEGER` | Ganze Zahlen | 42, -17, 0 |
| `DOUBLE` | Dezimalzahlen | 3.14, 99.99 |
| `VARCHAR` | Text beliebiger Länge | 'Hallo', 'search query' |
| `BOOLEAN` | Wahr/Falsch | true, false |
| `DATE` | Datum | '2024-01-15' |
| `TIMESTAMP` | Datum + Uhrzeit | '2024-01-15 14:30:00' |
| `JSON` | JSON-Daten | '{"key": "value"}' |

### Tabelle löschen (falls nötig)

```sql
DROP TABLE IF EXISTS searches;
```

### Tabelle ändern

```sql
-- Neue Spalte hinzufügen
ALTER TABLE searches ADD COLUMN device_type VARCHAR;

-- Spalte umbenennen
ALTER TABLE searches RENAME COLUMN device_type TO device;
```

---

## 5. Daten importieren

### CSV-Dateien importieren

Dies ist vermutlich dein häufigster Use Case.

#### Methode 1: Direkt lesen (ohne Import)

```sql
-- CSV direkt abfragen, ohne sie zu importieren
SELECT * FROM 'export_2024_01.csv' LIMIT 10;

-- Mehrere CSVs mit Wildcard
SELECT * FROM 'exports/*.csv';
```

#### Methode 2: In Tabelle importieren

```sql
-- Neue Tabelle aus CSV erstellen (Spalten werden automatisch erkannt)
CREATE TABLE searches AS
SELECT * FROM read_csv('search_data.csv');

-- Mit expliziten Optionen
CREATE TABLE searches AS
SELECT * FROM read_csv('search_data.csv',
    header = true,           -- Erste Zeile ist Header
    delimiter = ';',         -- Trennzeichen (Standard ist ,)
    dateformat = '%d.%m.%Y'  -- Deutsches Datumsformat
);
```

#### Methode 3: In bestehende Tabelle einfügen

```sql
-- Daten an bestehende Tabelle anhängen
INSERT INTO searches
SELECT * FROM read_csv('neue_daten.csv');
```

### Python-Variante für CSV-Import

```python
import duckdb

con = duckdb.connect('searchanalytics.db')

# CSV einlesen und als Tabelle speichern
con.execute("""
    CREATE TABLE IF NOT EXISTS searches AS
    SELECT * FROM read_csv('search_data.csv')
""")

# Prüfen wie viele Zeilen importiert wurden
count = con.execute("SELECT COUNT(*) FROM searches").fetchone()[0]
print(f"{count} Zeilen importiert")
```

### Parquet-Dateien (effizienter als CSV)

```sql
-- Parquet lesen
SELECT * FROM 'data.parquet';

-- Als Parquet exportieren (viel schneller beim späteren Lesen)
COPY searches TO 'searches_backup.parquet' (FORMAT PARQUET);
```

### Excel-Dateien

```sql
-- Benötigt spatial extension
INSTALL spatial;
LOAD spatial;

SELECT * FROM st_read('daten.xlsx');
```

---

## 6. SQL Queries - Die wichtigsten Befehle

### SELECT - Daten abfragen

```sql
-- Alle Spalten
SELECT * FROM searches;

-- Bestimmte Spalten
SELECT search_query, results_count, timestamp
FROM searches;

-- Mit Limit (wichtig bei großen Daten!)
SELECT * FROM searches LIMIT 100;
```

### WHERE - Filtern

```sql
-- Nach Datum filtern
SELECT * FROM searches
WHERE timestamp >= '2024-01-01';

-- Nach Text filtern
SELECT * FROM searches
WHERE search_query LIKE '%error%';

-- Mehrere Bedingungen
SELECT * FROM searches
WHERE country = 'DE'
  AND results_count = 0
  AND timestamp >= '2024-01-01';
```

### ORDER BY - Sortieren

```sql
-- Nach Datum sortieren (neueste zuerst)
SELECT * FROM searches
ORDER BY timestamp DESC;

-- Nach mehreren Spalten
SELECT * FROM searches
ORDER BY country, timestamp DESC;
```

### GROUP BY - Aggregieren

Dies ist das Herzstück von Analytics!

```sql
-- Suchen pro Tag zählen
SELECT
    DATE_TRUNC('day', timestamp) AS tag,
    COUNT(*) AS anzahl_suchen
FROM searches
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY tag;

-- Suchen pro Land
SELECT
    country,
    COUNT(*) AS anzahl,
    AVG(response_time) AS avg_response_time
FROM searches
GROUP BY country
ORDER BY anzahl DESC;

-- Top Suchbegriffe
SELECT
    search_query,
    COUNT(*) AS anzahl
FROM searches
GROUP BY search_query
ORDER BY anzahl DESC
LIMIT 20;
```

### Aggregations-Funktionen

| Funktion | Beschreibung |
|----------|--------------|
| `COUNT(*)` | Anzahl Zeilen |
| `COUNT(DISTINCT spalte)` | Anzahl eindeutiger Werte |
| `SUM(spalte)` | Summe |
| `AVG(spalte)` | Durchschnitt |
| `MIN(spalte)` | Minimum |
| `MAX(spalte)` | Maximum |
| `MEDIAN(spalte)` | Median |
| `PERCENTILE_CONT(0.95)` | 95. Perzentil |

### JOIN - Tabellen verknüpfen

```sql
-- Beispiel: Suchen mit User-Informationen verknüpfen
SELECT
    s.search_query,
    s.timestamp,
    u.user_name,
    u.department
FROM searches s
LEFT JOIN users u ON s.user_id = u.user_id;
```

### Subqueries und CTEs

```sql
-- CTE (Common Table Expression) - lesbarere Queries
WITH daily_stats AS (
    SELECT
        DATE_TRUNC('day', timestamp) AS tag,
        COUNT(*) AS searches,
        COUNT(DISTINCT user_id) AS unique_users
    FROM searches
    GROUP BY DATE_TRUNC('day', timestamp)
)
SELECT
    tag,
    searches,
    unique_users,
    searches / unique_users AS searches_per_user
FROM daily_stats
ORDER BY tag;
```

---

## 7. DuckDB mit Python nutzen

### Grundlegende Nutzung

```python
import duckdb

# Verbindung herstellen
con = duckdb.connect('searchanalytics.db')

# Query ausführen und alle Ergebnisse holen
results = con.execute("SELECT * FROM searches LIMIT 10").fetchall()
for row in results:
    print(row)

# Nur eine Zeile
single = con.execute("SELECT COUNT(*) FROM searches").fetchone()
print(f"Anzahl: {single[0]}")
```

### Mit Pandas DataFrames arbeiten

```python
import duckdb
import pandas as pd

con = duckdb.connect('searchanalytics.db')

# Query-Ergebnis direkt als DataFrame
df = con.execute("""
    SELECT
        DATE_TRUNC('day', timestamp) AS tag,
        COUNT(*) AS anzahl
    FROM searches
    GROUP BY 1
    ORDER BY 1
""").df()

print(df)

# DataFrame in DuckDB laden
neue_daten = pd.DataFrame({
    'search_query': ['test', 'beispiel'],
    'timestamp': ['2024-01-15', '2024-01-16'],
    'results_count': [10, 5]
})

con.execute("INSERT INTO searches SELECT * FROM neue_daten")
```

### Komplettes Beispiel-Script

```python
"""
Search Analytics mit DuckDB - Beispiel Script
"""
import duckdb
from datetime import datetime, timedelta

# Datenbank verbinden
con = duckdb.connect('searchanalytics.db')

# Tabelle erstellen (falls nicht vorhanden)
con.execute("""
    CREATE TABLE IF NOT EXISTS searches (
        timestamp       TIMESTAMP,
        search_query    VARCHAR,
        user_id         VARCHAR,
        results_count   INTEGER,
        response_time   DOUBLE,
        country         VARCHAR
    )
""")

# CSV-Daten importieren
con.execute("""
    INSERT INTO searches
    SELECT * FROM read_csv('search_export.csv', header=true)
""")

# Analyse 1: Tägliche Statistiken
print("\n📊 Tägliche Suchstatistiken (letzte 7 Tage):")
print("-" * 50)

daily_stats = con.execute("""
    SELECT
        DATE_TRUNC('day', timestamp)::DATE AS datum,
        COUNT(*) AS suchen,
        COUNT(DISTINCT user_id) AS unique_users,
        ROUND(AVG(response_time), 2) AS avg_response_ms
    FROM searches
    WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
    ORDER BY 1 DESC
""").df()

print(daily_stats.to_string(index=False))

# Analyse 2: Top Suchbegriffe
print("\n🔍 Top 10 Suchbegriffe:")
print("-" * 50)

top_queries = con.execute("""
    SELECT
        search_query,
        COUNT(*) AS anzahl,
        ROUND(AVG(results_count), 1) AS avg_results
    FROM searches
    GROUP BY search_query
    ORDER BY anzahl DESC
    LIMIT 10
""").df()

print(top_queries.to_string(index=False))

# Analyse 3: Null-Ergebnis-Suchen
print("\n⚠️ Suchen ohne Ergebnisse:")
print("-" * 50)

zero_results = con.execute("""
    SELECT
        search_query,
        COUNT(*) AS anzahl
    FROM searches
    WHERE results_count = 0
    GROUP BY search_query
    ORDER BY anzahl DESC
    LIMIT 10
""").df()

print(zero_results.to_string(index=False))

# Verbindung schließen
con.close()
print("\n✅ Analyse abgeschlossen!")
```

---

## 8. UI Tools für DuckDB

### Option 1: Harlequin (Terminal UI)

Moderne Terminal-Oberfläche mit Autocomplete und Syntax-Highlighting.

```bash
# Installation
pip install harlequin harlequin-duckdb

# Starten
harlequin searchanalytics.db
```

**Tastenkürzel:**
- `Ctrl+Enter` - Query ausführen
- `Ctrl+E` - Zwischen Editor und Ergebnis wechseln
- `F1` - Hilfe
- `Ctrl+Q` - Beenden

### Option 2: Jupyter Lab

Interaktive Notebooks - ideal für Exploration und Dokumentation.

```bash
# Installation (falls nicht vorhanden)
conda install jupyterlab

# Starten
jupyter lab
```

Dann im Notebook:
```python
import duckdb
con = duckdb.connect('searchanalytics.db')

# Ergebnisse werden schön als Tabelle angezeigt
con.execute("SELECT * FROM searches LIMIT 10").df()
```

### Option 3: JupySQL Magic Commands

Ermöglicht SQL direkt in Jupyter Zellen zu schreiben.

```bash
pip install jupysql duckdb-engine
```

```python
# In Jupyter Notebook
%load_ext sql
%sql duckdb:///searchanalytics.db

# Ab jetzt kannst du SQL-Zellen nutzen:
```

```sql
%%sql
SELECT country, COUNT(*) as anzahl
FROM searches
GROUP BY country
ORDER BY anzahl DESC
```

### Option 4: DBeaver (falls erlaubt)

Falls du DBeaver installieren darfst:

1. DBeaver öffnen
2. Neue Verbindung → DuckDB auswählen
3. Pfad zur `.db` Datei angeben
4. Fertig - du hast eine vollständige SQL IDE

---

## 9. Tipps und Best Practices

### Performance-Tipps

```sql
-- 1. LIMIT nutzen bei Exploration
SELECT * FROM searches LIMIT 100;  -- Nicht: SELECT * FROM searches;

-- 2. Nur benötigte Spalten abfragen
SELECT search_query, timestamp FROM searches;  -- Nicht: SELECT *

-- 3. Parquet statt CSV für wiederkehrende Daten
COPY searches TO 'searches.parquet' (FORMAT PARQUET);
-- Parquet ist 5-10x schneller zu lesen
```

### Daten-Backup

```sql
-- Als Parquet exportieren (komprimiert, schnell)
COPY searches TO 'backup/searches_2024_01.parquet' (FORMAT PARQUET);

-- Als CSV exportieren (lesbar, kompatibel)
COPY searches TO 'backup/searches_2024_01.csv' (HEADER, DELIMITER ',');
```

### Nützliche Abfragen für Search Analytics

```sql
-- Suchtrend über Zeit
SELECT
    DATE_TRUNC('hour', timestamp) AS stunde,
    COUNT(*) AS suchen
FROM searches
WHERE timestamp >= CURRENT_DATE
GROUP BY 1
ORDER BY 1;

-- Null-Ergebnis-Rate
SELECT
    DATE_TRUNC('day', timestamp) AS tag,
    COUNT(*) AS total,
    SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) AS null_results,
    ROUND(100.0 * SUM(CASE WHEN results_count = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_rate_pct
FROM searches
GROUP BY 1
ORDER BY 1;

-- Response Time Perzentile
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY response_time) AS p50,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY response_time) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY response_time) AS p99
FROM searches;
```

### In-Memory vs. Persistente Datenbank

```python
# In-Memory (Daten weg nach Programmende)
con = duckdb.connect(':memory:')
# oder
con = duckdb.connect()

# Persistente Datenbank (Daten bleiben erhalten)
con = duckdb.connect('searchanalytics.db')
```

---

## 10. Troubleshooting

### Problem: "Table does not exist"

```python
# Alle Tabellen anzeigen
con.execute("SHOW TABLES").fetchall()

# Prüfen ob richtige Datenbank verbunden
con.execute("SELECT current_database()").fetchone()
```

### Problem: CSV-Import schlägt fehl

```python
# CSV erst mal inspizieren
con.execute("SELECT * FROM read_csv('datei.csv') LIMIT 5").df()

# Mit expliziten Optionen
con.execute("""
    SELECT * FROM read_csv('datei.csv',
        header = true,
        delimiter = ';',
        quote = '"',
        escape = '\\',
        null_padding = true  -- Falls Spalten fehlen
    )
""")
```

### Problem: Encoding-Fehler

```sql
-- UTF-8 explizit angeben
SELECT * FROM read_csv('datei.csv', encoding='UTF-8');

-- Oder Windows-Encoding
SELECT * FROM read_csv('datei.csv', encoding='WINDOWS-1252');
```

### Problem: Speicher voll bei großen Daten

```sql
-- Temporäres Verzeichnis setzen (für Spilling)
SET temp_directory='/pfad/mit/viel/platz';

-- Memory Limit setzen
SET memory_limit='4GB';
```

### Problem: Langsame Queries

```sql
-- Query Plan anzeigen
EXPLAIN ANALYZE SELECT ... ;

-- Index erstellen (selten nötig bei DuckDB)
CREATE INDEX idx_timestamp ON searches(timestamp);
```

---

## Schnellreferenz

```sql
-- Datenbank
.tables                              -- Alle Tabellen zeigen
.schema tablename                    -- Tabellenstruktur zeigen

-- Daten laden
SELECT * FROM 'file.csv';            -- CSV direkt lesen
SELECT * FROM 'data/*.parquet';      -- Mehrere Parquet-Dateien

-- Tabellen
CREATE TABLE t AS SELECT ...;        -- Tabelle aus Query erstellen
DROP TABLE t;                        -- Tabelle löschen
INSERT INTO t SELECT ...;            -- Daten einfügen

-- Analyse
COUNT(*), SUM(), AVG(), MIN(), MAX() -- Aggregationen
GROUP BY spalte                      -- Gruppieren
ORDER BY spalte DESC                 -- Sortieren
LIMIT 100                            -- Begrenzen

-- Export
COPY t TO 'out.parquet' (FORMAT PARQUET);
COPY t TO 'out.csv' (HEADER, DELIMITER ',');
```

---

## Nächste Schritte

1. ✅ DuckDB installieren (`conda install -c conda-forge python-duckdb`)
2. ✅ Diese Anleitung durcharbeiten
3. ⬜ Erste CSV-Datei importieren
4. ⬜ Basis-Queries auf deinen Daten ausführen
5. ⬜ Jupyter Notebook für regelmäßige Reports einrichten

Bei Fragen: Die [offizielle DuckDB Dokumentation](https://duckdb.org/docs/) ist sehr gut und hat viele Beispiele.

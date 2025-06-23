import os
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
import psycopg
from psycopg.errors import DuplicateDatabase

# Load environment variables
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = "emotion-coding-postgres-db"
DB_PORT = 5433

DB_NAME = "base_dict"
DATA_DIR = "data-nrc-encoded.csv" 

EMOTION_COLS = ['anger', 'disgust', 'fear', 'joy', 'sadness']

def generate_calendar(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def connect_db(db_name=None):
    """Connects to the specified database or the default 'postgres' database."""
    conn_string = f"user={DB_USER} password={DB_PASS} host={DB_HOST} port={DB_PORT}"
    if db_name:
        conn_string += f" dbname={db_name}"
    return psycopg.connect(conn_string)

def create_database_if_not_exists():
    """
    Connects to the default 'postgres' database and creates the target database
    if it does not already exist.
    """
    print(f"Attempting to connect to default database 'postgres' to create '{DB_NAME}' if it doesn't exist...")
    try:
        with connect_db("postgres") as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    cur.execute(f"CREATE DATABASE {DB_NAME};")
                    print(f"✅ Database '{DB_NAME}' created successfully.")
                except DuplicateDatabase:
                    print(f"ℹ️ Database '{DB_NAME}' already exists. Skipping creation.")
                except Exception as e:
                    print(f"❌ Error creating database '{DB_NAME}': {e}")
                    raise
    except Exception as e:
        print(f"❌ Could not connect to default database 'postgres': {e}")
        raise

def create_tables():
    """Creates all necessary tables and sets up foreign key constraints."""
    print("Setting up database schema (tables and foreign keys)...")
    with connect_db(DB_NAME) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                -- Drop tables in reverse order of dependency to avoid foreign key issues during recreation
                DROP TABLE IF EXISTS speech CASCADE;
                DROP TABLE IF EXISTS politician CASCADE;
                DROP TABLE IF EXISTS nrc_encoding CASCADE;
                DROP TABLE IF EXISTS date_dim CASCADE;
                DROP TABLE IF EXISTS party CASCADE;

                -- 1) Create all tables (no circular references)

                CREATE TABLE party (
                  id          SERIAL PRIMARY KEY,
                  name        TEXT NOT NULL UNIQUE
                );

                CREATE TABLE date_dim (
                  id          SERIAL PRIMARY KEY,
                  date        DATE NOT NULL UNIQUE,
                  day         INTEGER NOT NULL,
                  month       INTEGER NOT NULL,
                  year        INTEGER NOT NULL
                );

                CREATE TABLE nrc_encoding (
                  id          SERIAL PRIMARY KEY,
                  anger       REAL NOT NULL,
                  disgust     REAL NOT NULL,
                  fear        REAL NOT NULL,
                  joy         REAL NOT NULL,
                  sadness     REAL NOT NULL
                );

                CREATE TABLE politician (
                  id          SERIAL PRIMARY KEY,
                  name        TEXT NOT NULL,
                  party       INTEGER,
                  UNIQUE(name, party)
                );

                CREATE TABLE speech (
                  id            SERIAL PRIMARY KEY,
                  speaker       INTEGER,
                  speech_date   INTEGER,
                  text          TEXT,
                  nrc_encoding  INTEGER
                );

                -- 2) Add foreign key constraints

                ALTER TABLE politician
                    ADD CONSTRAINT fk_politician_party
                        FOREIGN KEY (party) REFERENCES party(id);

                ALTER TABLE speech
                    ADD CONSTRAINT fk_speech_speaker
                        FOREIGN KEY (speaker) REFERENCES politician(id),
                    ADD CONSTRAINT fk_speech_date
                        FOREIGN KEY (speech_date) REFERENCES date_dim(id),
                    ADD CONSTRAINT fk_speech_nrc
                        FOREIGN KEY (nrc_encoding) REFERENCES nrc_encoding(id);
            """)
        conn.commit()
    print("✅ Database schema created successfully.")

def main():
    """
    Process Documentation

    - Creates the database if it doesn't exist.
    - Loads dataset specified in .env file (now CSV).
    - Converts Date column to dt.date object.
    - Fills any NaN values in emotion columns with 0.0 for database insertion.
    - Populates the database with processed data.
    """

    # Step 1: Create the database and tables
    create_database_if_not_exists()
    create_tables()

    # Step 2: Load and process data
    print(f"📂 Loading CSV file from {DATA_DIR}...")
    try:
        df = pd.read_csv(DATA_DIR)
        print(f"✅ CSV file loaded successfully. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"❌ Error: CSV file not found at {DATA_DIR}. Please ensure the file exists.")
        return
    except pd.errors.EmptyDataError:
        print(f"❌ Error: CSV file at {DATA_DIR} is empty.")
        return
    except Exception as e:
        print(f"❌ Error loading CSV file: {e}")
        return

    # Conversion to date
    if 'Date' not in df.columns:
        print("❌ Error: 'Date' column not found in the CSV file.")
        return
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    print("✅ 'Date' column converted to datetime.date objects.")

    print("🧹 Handling potential NaN values in emotion columns by filling with 0.0...")
    for col in EMOTION_COLS:
        if col not in df.columns:
            print(f"❌ Warning: Emotion column '{col}' not found in the CSV file. Please check column names.")
    df[EMOTION_COLS] = df[EMOTION_COLS].fillna(0.0)
    print("✅ NaN values in emotion columns filled with 0.0.")

    # Step 3: Populate the database
    with connect_db(DB_NAME) as conn:
        with conn.cursor() as cur:
            print("🏛️ Inserting parties...")
            party_names = set(df['Speaker_party_name'].dropna().unique())
            cur.executemany(
                "INSERT INTO party (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;",
                [(name,) for name in party_names]
            )
            conn.commit()
            print(f"✅ {len(party_names)} unique parties processed.")

            cur.execute("SELECT id, name FROM party;")
            party_map = {name: pid for pid, name in cur.fetchall()}

            print("📅 Inserting calendar...")
            # Initialize date_map here, before it's used in the loop
            date_map = {} 
            
            min_date = df['Date'].min() if not df['Date'].empty else date(2000, 1, 1)
            max_date = df['Date'].max() if not df['Date'].empty else date(2025, 12, 31)
            if isinstance(min_date, pd.Timestamp): min_date = min_date.date()
            if isinstance(max_date, pd.Timestamp): max_date = max_date.date()

            inserted_dates = 0
            for d in generate_calendar(min_date, max_date):
                cur.execute("""
                    INSERT INTO date_dim (date, day, month, year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date) DO NOTHING RETURNING id;
                """, (d, d.day, d.month, d.year))
                result = cur.fetchone()
                if result:
                    date_map[d] = result[0]
                    inserted_dates += 1
                else:
                    cur.execute("SELECT id FROM date_dim WHERE date = %s;", (d,))
                    date_map[d] = cur.fetchone()[0]
            conn.commit()
            print(f"✅ Calendar dates inserted/verified. {inserted_dates} new dates added (approx).")

            print("👤 Inserting politicians...")
            politician_map = {}
            for name, party_name in df[['Speaker_name', 'Speaker_party_name']].drop_duplicates().itertuples(index=False):
                if pd.isna(name) or pd.isna(party_name):
                    continue

                party_id = party_map.get(party_name)
                if party_id is None:
                    print(f"⚠️ Skipping politician '{name}' due to unknown party '{party_name}' (ID not found).")
                    continue

                key = (name, party_id)
                if key not in politician_map:
                    try:
                        cur.execute("""
                            INSERT INTO politician (name, party)
                            VALUES (%s, %s)
                            ON CONFLICT (name, party) DO NOTHING RETURNING id;
                        """, (name, party_id))
                        result = cur.fetchone()
                        if result:
                            politician_map[key] = result[0]
                        else:
                            cur.execute("SELECT id FROM politician WHERE name = %s AND party = %s;", (name, party_id))
                            politician_map[key] = cur.fetchone()[0]
                    except Exception as e:
                        print(f"❌ Error inserting politician '{name}' ({party_name}): {e}")
                        continue
            conn.commit()
            print(f"✅ {len(politician_map)} unique politicians processed.")

            print("🗣️ Inserting speeches...")
            inserted, skipped = 0, 0
            for idx, row in df.iterrows():
                try:
                    speaker_name = row.get('Speaker_name')
                    party_name = row.get('Speaker_party_name')
                    speech_date = row.get('Date')

                    if pd.isna(speaker_name) or pd.isna(party_name) or pd.isna(speech_date):
                        skipped += 1
                        continue

                    date_id = date_map.get(speech_date)
                    party_id = party_map.get(party_name)
                    speaker_key = (speaker_name, party_id)
                    speaker_id = politician_map.get(speaker_key)
                    
                    if not all([date_id, speaker_id]) or any(pd.isna(row[col]) for col in EMOTION_COLS):
                        skipped += 1
                        continue
                    
                    cur.execute("""
                        INSERT INTO nrc_encoding (anger, disgust, fear, joy, sadness)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id;
                    """, tuple(row[col] for col in EMOTION_COLS))
                    nrc_id = cur.fetchone()[0]

                    cur.execute("""
                        INSERT INTO speech (speaker, speech_date, text, nrc_encoding)
                        VALUES (%s, %s, %s, %s);
                    """, (speaker_id, date_id, row['Text'], nrc_id))
                    inserted += 1

                    if inserted % 500 == 0:
                        print(f"  → {inserted} speeches inserted...")

                except Exception as e:
                    skipped += 1
                    print(f"⚠️ Row {idx} skipped due to error: {e}. Data (partial): Speaker={row.get('Speaker_name')}, Party={row.get('Speaker_party_name')}, Date={row.get('Date')}")

            conn.commit()
            print(f"✅ Done. {inserted} speeches inserted. {skipped} rows skipped.")


if __name__ == "__main__":
    main()
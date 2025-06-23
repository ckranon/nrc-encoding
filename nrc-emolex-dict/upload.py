import os
import pandas as pd
import pyreadr # This import is already present, but good to note
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

DB_NAME = "emolex_dict"
DATA_DIR = "parlmint_with_emotions.rds"

# Define EMOTION_COLS globally as it's used in main
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
            conn.autocommit = True # Autocommit is needed for CREATE DATABASE
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
                DROP TABLE IF EXISTS speech CASCADE; -- CASCADE will drop dependent objects
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
                  date        DATE NOT NULL UNIQUE, -- ADDED UNIQUE CONSTRAINT HERE
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
                  UNIQUE(name, party) -- ADDED UNIQUE CONSTRAINT HERE
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

# --- NEW FUNCTION TO BE ADDED ---
def load_rdata(file_path: str) -> pd.DataFrame:
    """
    Reads an RData file (.rds) using pyreadr and returns the first DataFrame found.

    Args:
        file_path (str): The path to the .rds file.

    Returns:
        pd.DataFrame: The first DataFrame extracted from the .rds file.

    Raises:
        FileNotFoundError: If the file_path does not exist.
        ValueError: If no DataFrame can be extracted from the .rds file.
        Exception: For other errors during reading.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The RData file was not found at: {file_path}")

    try:
        # pyreadr.read_r returns a dictionary-like object
        # The keys are the names of the R objects in the file
        rdata_objects = pyreadr.read_r(file_path)

        # Iterate through the objects and return the first DataFrame found
        for key, value in rdata_objects.items():
            if isinstance(value, pd.DataFrame):
                print(f"Successfully loaded DataFrame '{key}' from '{file_path}'.")
                return value
        
        raise ValueError(f"No DataFrame found in the RData file: {file_path}")

    except Exception as e:
        # Re-raise the original exception or a more descriptive one
        raise Exception(f"Failed to read RData file '{file_path}': {e}")
# --- END OF NEW FUNCTION ---


def main():
    """
    Process Documentation

    - Creates the database if it doesn't exist.
    - Loads dataset specified in .env file.
    - Converts Date column to dt.date object
    - Normalizes specified emotion count to proportions.
    - Populates the database with processed data.
    """

    # Step 1: Create the database and tables
    create_database_if_not_exists()
    create_tables()

    # Step 2: Load and process data
    print(f"📂 Loading RData file from {DATA_DIR}...")
    try:
        # The call to load_rdata is here
        df = load_rdata(DATA_DIR)
    except FileNotFoundError:
        print(f"❌ Error: RData file not found at {DATA_DIR}. Please check your .env configuration.")
        return
    except Exception as e:
        print(f"❌ Error loading RData file: {e}")
        return

    # Conversion to date
    df['Date'] = pd.to_datetime(df['Date']).dt.date

    # Normalize NRC emotions to proportions per row
    print("📊 Normalizing NRC emotion values to proportions...")
    emotion_sum = df[EMOTION_COLS].sum(axis=1)
    zero_sum_mask = emotion_sum == 0
    nonzero_mask = ~zero_sum_mask

    df.loc[nonzero_mask, EMOTION_COLS] = (
        df.loc[nonzero_mask, EMOTION_COLS].div(emotion_sum[nonzero_mask], axis=0)
    )
    df.loc[zero_sum_mask, EMOTION_COLS] = 0.0
    print(f"✅ Proportional encoding applied. {zero_sum_mask.sum()} rows had zero emotion sum.")

    # Step 3: Populate the database
    with connect_db(DB_NAME) as conn: # Connect to the specific database for data insertion
        with conn.cursor() as cur:
            print("🏛️ Inserting parties...")
            party_names = set(df['Speaker_party_name'].dropna().unique()) # Handle potential NaN values
            cur.executemany(
                "INSERT INTO party (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;", # Use ON CONFLICT (name)
                [(name,) for name in party_names]
            )
            conn.commit()

            cur.execute("SELECT id, name FROM party;")
            party_map = {name: pid for pid, name in cur.fetchall()}

            print("📅 Inserting calendar...")
            date_map = {}
            for d in generate_calendar(date(2000, 1, 1), date(2025, 12, 31)):
                cur.execute("""
                    INSERT INTO date_dim (date, day, month, year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date) DO NOTHING RETURNING id; -- Use ON CONFLICT (date)
                """, (d, d.day, d.month, d.year))
                result = cur.fetchone()
                if result:
                    date_map[d] = result[0]
                else:
                    cur.execute("SELECT id FROM date_dim WHERE date = %s;", (d,))
                    date_map[d] = cur.fetchone()[0]
            conn.commit() # Commit the calendar insertions

            print("👤 Inserting politicians...")
            politician_map = {}
            # Iterate through unique combinations of speaker name and party to avoid redundant inserts
            for name, party_name in df[['Speaker_name', 'Speaker_party_name']].drop_duplicates().itertuples(index=False):
                if pd.isna(name) or pd.isna(party_name): # Skip if name or party_name is NaN
                    continue

                party_id = party_map.get(party_name)
                if party_id is None:
                    print(f"⚠️ Skipping politician '{name}' due to unknown party '{party_name}'.")
                    continue

                key = (name, party_id)
                if key not in politician_map:
                    cur.execute("""
                        INSERT INTO politician (name, party)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING RETURNING id; -- ON CONFLICT handles existing politicians
                    """, (name, party_id))
                    result = cur.fetchone()
                    if result:
                        politician_map[key] = result[0]
                    else:
                        cur.execute("SELECT id FROM politician WHERE name = %s AND party = %s;", (name, party_id))
                        politician_map[key] = cur.fetchone()[0]
            conn.commit() # Commit the politician insertions

            print("🗣️ Inserting speeches...")
            inserted, skipped = 0, 0
            for idx, row in df.iterrows():
                try:
                    # Insert NRC encoding
                    cur.execute("""
                        INSERT INTO nrc_encoding (anger, disgust, fear, joy, sadness)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id;
                    """, tuple(row[col] for col in EMOTION_COLS))
                    nrc_id = cur.fetchone()[0]

                    date_id = date_map.get(row['Date'])
                    party_id = party_map.get(row['Speaker_party_name'])
                    speaker_key = (row['Speaker_name'], party_id)
                    speaker_id = politician_map.get(speaker_key)

                    if not all([nrc_id, date_id, speaker_id]):
                        skipped += 1
                        print(f"⚠️ Row {idx} skipped due to missing ID (NRC: {nrc_id}, Date: {date_id}, Speaker: {speaker_id}). Data: {row.to_dict()}")
                        continue

                    cur.execute("""
                        INSERT INTO speech (speaker, speech_date, text, nrc_encoding)
                        VALUES (%s, %s, %s, %s);
                    """, (speaker_id, date_id, row['Text'], nrc_id))
                    inserted += 1

                    if inserted % 500 == 0:
                        print(f"  → {inserted} speeches inserted...")

                except Exception as e:
                    skipped += 1
                    print(f"⚠️ Row {idx} skipped due to error: {e}. Data: {row.to_dict()}")

            conn.commit()
            print(f"✅ Done. {inserted} speeches inserted. {skipped} rows skipped.")


if __name__ == "__main__":
    main()
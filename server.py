import os
from fastapi import FastAPI, HTTPException, Query
import psycopg

app = FastAPI()

# --- Konfig aus Render-Umgebungsvariablen ---
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SYNC_TOKEN = os.environ.get("SYNC_TOKEN", "")

# --- DB-Schema (Tabellen) ---
SCHEMA_SQL = """
create table if not exists cards (
  id serial primary key,
  id_product bigint unique,
  name text not null,
  set_code text,
  number text,
  language text,
  is_foil boolean default false
);
create table if not exists holdings (
  id serial primary key,
  card_id int not null references cards(id) on delete cascade,
  quantity int not null check (quantity >= 0),
  condition text default 'NM'
);
create table if not exists prices_daily (
  id serial primary key,
  id_product bigint not null,
  date date not null,
  avg_price numeric,
  low_price numeric,
  trend_price numeric,
  data jsonb,
  unique (id_product, date)
);
"""

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL fehlt – bitte in Render → Service → Environment setzen.")
    return psycopg.connect(DATABASE_URL)

@app.get("/health")
def health():
    return {"ok": True}

# POST-Variante (für Tools / später)
@app.post("/admin/init-db")
def init_db_post(token: str = Query(..., alias="token")):
    if token != SYNC_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    with get_conn() as cx:
        cx.execute(SCHEMA_SQL)
    return {"status": "db-initialized"}

# GET-Variante (bequem per Browser anklickbar)
@app.get("/admin/init-db")
def init_db_get(token: str = Query(..., alias="token")):
    if token != SYNC_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    with get_conn() as cx:
        cx.execute(SCHEMA_SQL)
    return {"status": "db-initialized"}

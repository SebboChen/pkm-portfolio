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

from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class NewCard(BaseModel):
    id_product: Optional[int] = None
    name: str
    set_code: Optional[str] = None
    number: Optional[str] = None
    language: Optional[str] = "EN"
    is_foil: bool = False
    quantity: int = 1
    condition: str = "NM"

@app.post("/cards")
def add_card(card: NewCard):
    with get_conn() as cx:
        if card.id_product is not None:
            cur = cx.execute(
                "insert into cards(id_product,name,set_code,number,language,is_foil) "
                "values (%s,%s,%s,%s,%s,%s) "
                "on conflict (id_product) do update set "
                "name=excluded.name, set_code=excluded.set_code, number=excluded.number, "
                "language=excluded.language, is_foil=excluded.is_foil "
                "returning id",
                (card.id_product, card.name, card.set_code, card.number, card.language, card.is_foil)
            )
        else:
            cur = cx.execute(
                "insert into cards(id_product,name,set_code,number,language,is_foil) "
                "values (NULL,%s,%s,%s,%s,%s) returning id",
                (card.name, card.set_code, card.number, card.language, card.is_foil)
            )
        card_id = cur.fetchone()[0]
        cx.execute(
            "insert into holdings(card_id, quantity, condition) values (%s,%s,%s)",
            (card_id, card.quantity, card.condition)
        )
    return {"ok": True, "card_id": card_id}

@app.get("/cards")
def list_cards():
    with get_conn() as cx:
        cur = cx.execute(
            "select c.id, c.id_product, c.name, c.set_code, c.number, c.language, c.is_foil, "
            "coalesce(sum(h.quantity),0) as qty "
            "from cards c left join holdings h on h.card_id=c.id "
            "group by c.id order by c.name"
        )
        rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
    return rows

import os
from fastapi import FastAPI, HTTPException, Query
import psycopg
from psycopg.types.json import Json


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

from typing import List, Dict, Any
from datetime import date, datetime
from fastapi import Query, HTTPException

@app.post("/admin/import")
def import_prices(
    rows: List[Dict[str, Any]],
    token: str = Query(..., alias="token"),
    when: str | None = Query(None)  # z.B. "2025-09-18"
):
    if token != SYNC_TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        today = date.today() if not when else datetime.strptime(when, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="when muss YYYY-MM-DD sein")
    inserted = 0
    with get_conn() as cx:
        for row in rows:
            idp = int(row.get("idProduct"))
            avg = row.get("avgPrice")
            low = row.get("lowPrice")
            trend = row.get("trendPrice")
            cx.execute(
                "insert into prices_daily(id_product,date,avg_price,low_price,trend_price,data) "
                "values (%s,%s,%s,%s,%s,%s) "
                "on conflict (id_product,date) do update set "
                "avg_price=excluded.avg_price, low_price=excluded.low_price, "
                "trend_price=excluded.trend_price, data=excluded.data",
                (idp, today, avg, low, trend, Json(row))  # Json(row) hast du bereits eingebaut
            )
            inserted += 1
    return {"inserted": inserted, "date": str(today)}

@app.get("/api/portfolio")
def portfolio_value():
    with get_conn() as cx:
        cur = cx.execute("""
            with latest as (
              select id_product, max(date) d from prices_daily group by id_product
            )
            select coalesce(sum(h.quantity * coalesce(p.trend_price,p.avg_price,p.low_price,0)),0)
            from holdings h
            join cards c on c.id = h.card_id
            left join latest l on l.id_product = c.id_product
            left join prices_daily p on p.id_product = c.id_product and p.date = l.d
        """)
        total = float(cur.fetchone()[0] or 0.0)
    return {"total_eur": round(total, 2)}

# --- Plot-Endpoint ---
import io
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from fastapi.responses import Response

@app.get("/api/plot")
def plot_portfolio():
    with get_conn() as cx:
        df = pd.read_sql("""
            select p.date::date as date, c.id_product, h.quantity,
                   coalesce(p.trend_price, p.avg_price, p.low_price) as price
            from prices_daily p
            join cards c on c.id_product = p.id_product
            join holdings h on h.card_id = c.id
        """, cx)
    if df.empty:
        raise HTTPException(status_code=400, detail="Keine Preisdaten vorhanden.")

    df["value"] = df["quantity"] * df["price"].astype(float)
    ts = df.groupby("date")["value"].sum().sort_index()

    buf = io.BytesIO()
    plt.figure()
    plt.plot(ts.index, ts.values)
    plt.title("Portfolio-Wert (EUR)")
    plt.xlabel("Datum")
    plt.ylabel("Wert")
    plt.tight_layout()
    plt.savefig(buf, format="png", dpi=150)
    plt.close()
    buf.seek(0)
    return Response(content=buf.read(), media_type="image/png")

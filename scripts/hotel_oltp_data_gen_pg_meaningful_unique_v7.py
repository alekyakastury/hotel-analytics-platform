#!/usr/bin/env python
# coding: utf-8

# In[1]:


# seed_faker_one_time.py
# One-time Faker seeder for your Hotel OLTP Postgres schema.
#
# ✅ Fixes included (based on your errors):
# - Introspects schema: tables, columns, PK, FK, ENUMs, UNIQUE (single-column only)
# - Loads in FK dependency order
# - TRUNCATE ... RESTART IDENTITY CASCADE (optional)
# - Generates FK-valid data
# - Handles 1:1 tables automatically via UNIQUE(FK) without replacement
# - Special-cases:
#     - booking_room UNIQUE(booking_id, room_id)
#     - room_night UNIQUE(room_id, night_date)
#     - booking_discount UNIQUE(booking_id, promotion_id)
#     - stay_check: actual_checkout_at >= actual_checkin_at when both present
#     - booking_check: checkout_date >= checkin_date when both present
# - ENUM-safe: never hardcode enum labels (samples from pg enums)
#
# ✅ CRITICAL FIX (for your current crash):
# - After loading EACH table, if it has a single-column PK, we query the DB and store the
#   actual PK values into ref_ids[table_lc].
#   This guarantees downstream FK tables (like check_in.stay_id) get real stay_id values,
#   instead of falling back to "1" (which caused check_in_stay_id_key duplicates).
#
# Run:
#   python seed_faker_one_time.py
#
# Requirements:
#   pip install psycopg2-binary faker

from __future__ import annotations

import csv
import random
import uuid
from dataclasses import dataclass
from datetime import date, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
from faker import Faker


# =========================
# CONFIG (edit these)
# =========================
@dataclass(frozen=True)
class PostgresCreds:
    host: str
    port: str
    dbname: str
    user: str
    password: str
    schema: str = "public"


PG = PostgresCreds(
    host="postgres",
    port="5432",
    dbname="hotel_oltp",
    user="postgres",
    password="postgres",
    schema="public",
)

OUT_DIR = Path("scripts/data/faker")  # change to "data/faker" if you want
SEED = 42


# -------------------------
# MEANINGFUL DIMENSIONS
# -------------------------
# Faker's city/state are independent draws, which creates nonsense combos.
# We keep a small, realistic location pool and sample consistently per-row.

LOCATION_POOL = [
    # US (mix of timezones)
    {"city": "Boston", "state": "MA", "country": "US", "postal_prefix": "02", "timezone": "America/New_York"},
    {"city": "New York", "state": "NY", "country": "US", "postal_prefix": "10", "timezone": "America/New_York"},
    {"city": "Chicago", "state": "IL", "country": "US", "postal_prefix": "60", "timezone": "America/Chicago"},
    {"city": "Austin", "state": "TX", "country": "US", "postal_prefix": "78", "timezone": "America/Chicago"},
    {"city": "Denver", "state": "CO", "country": "US", "postal_prefix": "80", "timezone": "America/Denver"},
    {"city": "Seattle", "state": "WA", "country": "US", "postal_prefix": "98", "timezone": "America/Los_Angeles"},
    {"city": "San Francisco", "state": "CA", "country": "US", "postal_prefix": "94", "timezone": "America/Los_Angeles"},
    {"city": "San Jose", "state": "CA", "country": "US", "postal_prefix": "95", "timezone": "America/Los_Angeles"},
    {"city": "Miami", "state": "FL", "country": "US", "postal_prefix": "33", "timezone": "America/New_York"},
    # India (for variety)
    {"city": "Bengaluru", "state": "KA", "country": "IN", "postal_prefix": "560", "timezone": "Asia/Kolkata"},
    {"city": "Hyderabad", "state": "TS", "country": "IN", "postal_prefix": "500", "timezone": "Asia/Kolkata"},
    {"city": "Mumbai", "state": "MH", "country": "IN", "postal_prefix": "400", "timezone": "Asia/Kolkata"},
    {"city": "Chennai", "state": "TN", "country": "IN", "postal_prefix": "600", "timezone": "Asia/Kolkata"},
]

HOTEL_BRANDS = [
    "Marriott", "Hilton", "Hyatt", "Westin", "Sheraton", "Ibis", "Novotel", "Taj", "Oberoi", "Radisson"
]

ROOM_TYPE_NAMES = [
    "Standard King", "Standard Queen", "Deluxe King", "Deluxe Queen",
    "Studio", "Junior Suite", "Executive Suite", "Family Suite"
]

# Per-row cache so city/state/country/timezone stay consistent within a generated row.
_ROW_LOCATION: dict[tuple[str, int], dict] = {}


def get_row_location(fake, table: str, row_idx: int) -> dict:
    key = (table.lower(), row_idx)
    loc = _ROW_LOCATION.get(key)
    if loc is None:
        loc = dict(random.choice(LOCATION_POOL))
        # derive a plausible postal code
        pfx = loc.get("postal_prefix", "")
        if loc["country"] == "US":
            loc["postal_code"] = f"{pfx}{random.randint(0, 999):03d}"
        else:  # IN
            loc["postal_code"] = f"{pfx}{random.randint(0, 999):03d}" if len(pfx) == 3 else f"{pfx}{random.randint(0, 9999):04d}"
        # plausible address lines
        loc["street1"] = f"{random.randint(10, 9999)} {fake.street_name()}"
        loc["street2"] = random.choice([None, f"Apt {random.randint(1, 999)}", f"Suite {random.randint(100, 1999)}"])
        _ROW_LOCATION[key] = loc
    return loc
TRUNCATE_FIRST = True

# Override row counts (applied only if table exists)
ROW_COUNTS_OVERRIDE = {
    "booking": 70_000,
    "booking_room": 90_000,
    "guest": 120_000,
    "invoice": 70_000,
    "payment": 60_000,
    "refund": 8_000,
    "booking_cancellation": 8_000,
    "room": 1_000,
    "hotel": 12,
    "customer": 30_000,
    "channel": 12,
    "promotion": 2_000,
}
# =========================


def pg_dsn(pg: PostgresCreds) -> str:
    return f"host={pg.host} port={pg.port} dbname={pg.dbname} user={pg.user} password={pg.password}"


@dataclass(frozen=True)
class ColumnInfo:
    table: str
    column: str
    data_type: str
    udt_name: str
    is_nullable: bool
    char_max_len: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]


@dataclass(frozen=True)
class ForeignKey:
    table: str
    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class PrimaryKey:
    table: str
    columns: Tuple[str, ...]


# -------------------------
# Introspection
# -------------------------
def fetch_tables(conn, schema: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type='BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        return [r[0] for r in cur.fetchall()]


def fetch_columns(conn, schema: str) -> Dict[str, List[ColumnInfo]]:
    out: Dict[str, List[ColumnInfo]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              table_name,
              column_name,
              data_type,
              udt_name,
              is_nullable,
              character_maximum_length,
              numeric_precision,
              numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position
            """,
            (schema,),
        )
        for t, c, dt, udt, nul, cmax, prec, scale in cur.fetchall():
            out.setdefault(t, []).append(
                ColumnInfo(
                    table=t,
                    column=c,
                    data_type=dt,
                    udt_name=udt,
                    is_nullable=(nul == "YES"),
                    char_max_len=cmax,
                    numeric_precision=prec,
                    numeric_scale=scale,
                )
            )
    return out


def fetch_primary_keys(conn, schema: str) -> Dict[str, PrimaryKey]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tc.table_name, kcu.column_name, kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY tc.table_name, kcu.ordinal_position
            """,
            (schema,),
        )
        tmp: Dict[str, List[str]] = {}
        for t, c, _ in cur.fetchall():
            tmp.setdefault(t, []).append(c)
    return {t: PrimaryKey(table=t, columns=tuple(cols)) for t, cols in tmp.items()}


def fetch_foreign_keys(conn, schema: str) -> List[ForeignKey]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              tc.table_name,
              kcu.column_name,
              ccu.table_name AS ref_table_name,
              ccu.column_name AS ref_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = %s
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name, kcu.column_name
            """,
            (schema,),
        )
        return [ForeignKey(t, c, rt, rc) for t, c, rt, rc in cur.fetchall()]


def fetch_enum_values(conn) -> Dict[str, List[str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.typname AS enum_name, e.enumlabel AS enum_value
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            ORDER BY t.typname, e.enumsortorder
            """
        )
        out: Dict[str, List[str]] = {}
        for enum_name, enum_value in cur.fetchall():
            out.setdefault(enum_name.lower(), []).append(enum_value)
        return out


def fetch_unique_columns(conn, schema: str) -> Dict[str, Set[str]]:
    """
    ✅ ONLY single-column UNIQUE constraints: {table_lc: {col,...}}
    Store table keys lowercased to avoid casing mismatches.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH uniq AS (
              SELECT
                tc.table_schema,
                tc.table_name,
                tc.constraint_name,
                COUNT(kcu.column_name) AS col_count
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
              WHERE tc.table_schema = %s
                AND tc.constraint_type = 'UNIQUE'
              GROUP BY 1,2,3
            )
            SELECT kcu.table_name, kcu.column_name
            FROM uniq
            JOIN information_schema.key_column_usage kcu
              ON uniq.constraint_name = kcu.constraint_name
             AND uniq.table_schema = kcu.table_schema
            WHERE uniq.col_count = 1
            """,
            (schema,),
        )
        out: Dict[str, Set[str]] = {}
        for t, c in cur.fetchall():
            out.setdefault(t.lower(), set()).add(c)
        return out


# -------------------------
# Dependency ordering
# -------------------------
def topo_sort_tables(tables: List[str], fks: List[ForeignKey]) -> List[str]:
    deps: Dict[str, Set[str]] = {t: set() for t in tables}
    rdeps: Dict[str, Set[str]] = {t: set() for t in tables}

    for fk in fks:
        if fk.table in deps and fk.ref_table in deps:
            deps[fk.table].add(fk.ref_table)
            rdeps[fk.ref_table].add(fk.table)

    q = sorted([t for t in tables if not deps[t]])
    out: List[str] = []

    while q:
        n = q.pop(0)
        out.append(n)
        for m in sorted(rdeps[n]):
            deps[m].discard(n)
            if not deps[m] and m not in out and m not in q:
                q.append(m)
        q.sort()

    remaining = [t for t in tables if t not in out]
    return out + sorted(remaining)


# -------------------------
# Default row counts
# -------------------------
def default_row_counts(tables: List[str]) -> Dict[str, int]:
    rc: Dict[str, int] = {}
    for t in tables:
        tl = t.lower()
        if any(k in tl for k in ["lookup", "type", "status", "code", "catalog", "policy", "rate_plan", "rate_calendar"]):
            rc[t] = 50
        elif tl == "hotel":
            rc[t] = 12
        elif tl == "room":
            rc[t] = 1000
        elif "customer" in tl:
            rc[t] = 30_000
        elif "booking" in tl:
            rc[t] = 70_000
        elif any(k in tl for k in ["payment", "invoice", "transaction", "charge"]):
            rc[t] = 60_000
        elif any(k in tl for k in ["refund", "cancellation"]):
            rc[t] = 8_000
        else:
            rc[t] = 2_000
    return rc


# -------------------------
# Unique text registry
# -------------------------
_UNIQUE_SEEN: Dict[Tuple[str, str], Set[str]] = {}


def unique_text(key: Tuple[str, str], make_value, max_tries: int = 200000) -> str:
    seen = _UNIQUE_SEEN.setdefault(key, set())
    for _ in range(max_tries):
        v = str(make_value()).strip()
        if v and v not in seen:
            seen.add(v)
            return v


# Deterministic-ish unique choice from a small list (for UNIQUE(name) dimension tables)
_UNIQUE_POOL_STATE: dict = {}

def unique_choice_from_pool(key, base_list, maxlen: int | None = None) -> str:
    """
    Returns values from base_list without repeats until exhausted; then falls back to suffixing.
    key: (table, column) or any hashable identifier
    """
    st=_UNIQUE_POOL_STATE.get(key)
    if st is None:
        pool=list(base_list)
        random.shuffle(pool)
        st={"pool": pool}
        _UNIQUE_POOL_STATE[key]=st
    pool=st["pool"]
    if pool:
        v=pool.pop()
    else:
        v=f"{random.choice(base_list)}_{uuid.uuid4().hex[:6]}"
    if maxlen is not None:
        return str(v)[:maxlen]
    return str(v)
    raise RuntimeError(f"Could not generate unique value for {key}")


# -------------------------
# Value generator
# -------------------------
def generate_value(fake: Faker, col: ColumnInfo, row_idx: int, enums: Dict[str, List[str]]) -> Any:
    name = col.column.lower()
    dt = col.data_type.lower()
    udt = col.udt_name.lower()

    # ENUM
    if udt in enums:
        if col.is_nullable and random.random() < 0.03:
            return None
        return random.choice(enums[udt])

    # standard timestamps
    if name in {"created_at", "updated_at", "loaded_at", "ingested_at"}:
        base = fake.date_time_between(start_date="-2y", end_date="now", tzinfo=timezone.utc)
        if name == "updated_at":
            base = base + timedelta(days=random.randint(0, 180))
        return base

    # date
    if dt == "date":
        return fake.date_between(start_date="-2y", end_date="+1y")

    if dt in {"timestamp without time zone", "timestamp with time zone"} or udt in {"timestamp", "timestamptz"}:
        return fake.date_time_between(start_date="-2y", end_date="now", tzinfo=timezone.utc)

    # int
    if dt in {"integer", "bigint", "smallint"} or udt in {"int2", "int4", "int8"}:
        if name.endswith("_id"):
            return row_idx
        if name == "score":
            return random.randint(1, 5)
        if any(k in name for k in ["rating", "stars", "score"]):
            return random.randint(1, 5)
        if any(k in name for k in ["count", "qty", "quantity", "nights", "floor", "occupancy"]):
            return random.randint(1, 10)
        return random.randint(1, 100000)

    # uuid
    if dt == "uuid" or udt == "uuid":
        return str(uuid.uuid4())

    # bool
    if dt == "boolean":
        return random.random() < 0.85 if ("is_" in name or name.endswith("_flag")) else (random.random() < 0.5)

    # numeric/decimal
    if dt in {"numeric", "decimal"} or udt == "numeric":
        scale = col.numeric_scale or 2
        if "percent" in name or name.endswith("_pct") or name.endswith("pct"):
            return round(random.uniform(0, 100), scale)
        if "ratio" in name or "fraction" in name:
            return round(random.uniform(0, 1), scale)
        if col.table.lower() == "promotion" and name in {"value", "discount_value", "discount_amount", "discount"}:
            return round(random.uniform(5, 50), scale)
        if any(k in name for k in ["amount", "price", "rate", "cost", "fee", "total", "tax"]):
            return round(random.uniform(20, 2000), scale)
        return round(random.uniform(0, 1000), scale)

    # text/varchar
    if dt in {"character varying", "character", "text"}:
        maxlen = col.char_max_len or 255
        # location-aware fields (keep city/state/country/timezone consistent per row)
        if name in {"city", "state", "country", "postal_code", "zipcode", "zip"} or "timezone" in name or name in {"address_line1", "address_line2", "street", "street1", "street2"}:
            loc = get_row_location(fake, col.table, row_idx)
            if "timezone" in name:
                return loc.get("timezone", "America/New_York")[:maxlen]
            if name in {"city"}:
                return loc["city"][:maxlen]
            if name in {"state"}:
                return loc["state"][:maxlen]
            if name in {"country"}:
                return loc["country"][:maxlen]
            if name in {"postal_code", "zipcode", "zip"}:
                return str(loc.get("postal_code", "00000"))[:maxlen]
            if name in {"address_line1", "street", "street1"}:
                return loc.get("street1")[:maxlen]
            if name in {"address_line2", "street2"}:
                v = loc.get("street2")
                return ("" if v is None else str(v))[:maxlen]

        # more meaningful domain strings
        if col.table.lower() == "hotel" and name in {"name", "hotel_name"}:
            brand = random.choice(HOTEL_BRANDS)
            loc = get_row_location(fake, col.table, row_idx)
            base = f"{brand} {loc['city']}"
            suffix = random.choice(["Hotel", "Resort", "Suites", "Inn"])
            return f"{base} {suffix}"[:maxlen]
        if col.table.lower() == "room_type" and name in {"name", "room_type_name"}:
            return unique_choice_from_pool((col.table, col.column), ROOM_TYPE_NAMES, maxlen)
        if name in {"phone", "phone_number"}:
            return fake.phone_number()[:maxlen]
        if name in {"currency", "currency_code"}:
            return random.choice(["USD", "INR"])[:maxlen]
        if name in {"state_code", "state_abbr"}:
            loc = get_row_location(fake, col.table, row_idx)
            return loc.get("state", "NA")[:maxlen]
        if "timezone" in name:
            # fallback if location table not used in this row
            return "America/New_York"[:maxlen]
        if name == "email":
            return unique_text((col.table, col.column), lambda: fake.email())[:maxlen]
        if name.endswith("_name") or name in {"name", "code"}:
            return unique_text((col.table, col.column), lambda: f"{fake.word().title()}_{uuid.uuid4().hex[:6]}")[:maxlen]
        if "timezone" in name:
            return "America/New_York"[:maxlen]
        if maxlen <= 20:
            return fake.word()[:maxlen]
        if maxlen <= 80:
            return fake.sentence(nb_words=6)[:maxlen]
        return fake.sentence(nb_words=10)[:maxlen]

    return None


def build_fk_map(fks: List[ForeignKey]) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    ✅ Normalize table keys to lowercase to avoid casing mismatches.
    """
    return {(fk.table.lower(), fk.column): (fk.ref_table.lower(), fk.ref_column) for fk in fks}


# -------------------------
# Special-case generators
# -------------------------
def generate_booking_room_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    booking_ids = list(ref_ids.get("booking", []))
    room_ids = list(ref_ids.get("room", []))
    if not booking_ids or not room_ids:
        raise RuntimeError("booking_room needs booking and room ids available before generation.")

    booking_id_col = col_lc.get("booking_id")
    room_id_col = col_lc.get("room_id")
    if not booking_id_col or not room_id_col:
        raise RuntimeError("booking_room expected columns booking_id and room_id.")

    seen: set[tuple[Any, Any]] = set()
    rows: List[Dict[str, Any]] = []

    random.shuffle(booking_ids)

    i = 0
    while len(rows) < n_rows:
        b = booking_ids[i % len(booking_ids)]
        rooms_for_booking = random.randint(1, 3)
        for _ in range(rooms_for_booking):
            r = random.choice(room_ids)
            pair = (b, r)
            if pair in seen:
                continue
            seen.add(pair)

            row: Dict[str, Any] = {booking_id_col: b, room_id_col: r}
            for c in cols:
                if c.column in row:
                    continue
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                else:
                    v = generate_value(fake, c, len(rows) + 1, enums)
                    if v is None and not c.is_nullable:
                        v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                    row[c.column] = v

            rows.append(row)
            if len(rows) >= n_rows:
                break
        i += 1

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)
        for row in rows[:n_rows]:
            w.writerow([row.get(cn) for cn in colnames])

    return path


def generate_room_night_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    room_ids = list(ref_ids.get("room", []))
    if not room_ids:
        raise RuntimeError("room_night needs room ids available before generation.")

    room_id_col = col_lc.get("room_id")
    night_date_col = col_lc.get("night_date")
    if not room_id_col or not night_date_col:
        raise RuntimeError("room_night expected columns room_id and night_date.")

    start = date.today() - timedelta(days=730)
    end = date.today() + timedelta(days=365)
    total_days = (end - start).days

    seen: set[tuple[Any, date]] = set()
    rows: List[Dict[str, Any]] = []

    per_room = max(1, n_rows // len(room_ids))
    random.shuffle(room_ids)

    for rid in room_ids:
        if len(rows) >= n_rows:
            break
        k = min(per_room, total_days)
        offsets = random.sample(range(total_days), k=k)
        for off in offsets:
            nd = start + timedelta(days=off)
            pair = (rid, nd)
            if pair in seen:
                continue
            seen.add(pair)

            row: Dict[str, Any] = {room_id_col: rid, night_date_col: nd}
            for c in cols:
                if c.column in row:
                    continue
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                else:
                    v = generate_value(fake, c, len(rows) + 1, enums)
                    if v is None and not c.is_nullable:
                        v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                    row[c.column] = v

            rows.append(row)
            if len(rows) >= n_rows:
                break

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)
        for row in rows[:n_rows]:
            w.writerow([row.get(cn) for cn in colnames])

    return path


def generate_stay_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
    unique_cols: Dict[str, Set[str]],
) -> Path:
    """
    Enforces:
      actual_checkout_at IS NULL OR actual_checkin_at IS NULL OR actual_checkout_at >= actual_checkin_at

    ✅ UNIQUE(FK): if stay.booking_id is UNIQUE, assign booking_id without replacement.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    booking_id_col = col_lc.get("booking_id")
    status_col = col_lc.get("stay_status") or col_lc.get("status")
    aci_col = col_lc.get("actual_checkin_at")
    aco_col = col_lc.get("actual_checkout_at")

    booking_ids = list(ref_ids.get("booking", []))

    is_booking_unique = bool(booking_id_col and booking_id_col in unique_cols.get(table_lc, set()))
    if is_booking_unique:
        if len(booking_ids) < n_rows:
            raise RuntimeError(
                f'{table_lc}."{booking_id_col}" is UNIQUE but only {len(booking_ids)} booking ids exist '
                f"for requested n_rows={n_rows}."
            )
        random.shuffle(booking_ids)

    status_ci = next((c for c in cols if c.column == (status_col or "")), None)
    stay_status_choices = enums.get(status_ci.udt_name.lower(), []) if status_ci else []

    uniq_cols_in_table: Set[str] = set(unique_cols.get(table_lc, set()))
    seen_uniques: Dict[str, Set[Any]] = {c: set() for c in uniq_cols_in_table}

    def _register_unique(col: str, val: Any) -> bool:
        if val is None:
            return True
        s = seen_uniques.get(col)
        if s is None:
            return True
        if val in s:
            return False
        s.add(val)
        return True


    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)

        for i in range(1, n_rows + 1):
            row: Dict[str, Any] = {}

            if booking_id_col:
                if is_booking_unique:
                    row[booking_id_col] = booking_ids[i - 1]
                else:
                    row[booking_id_col] = random.choice(booking_ids) if booking_ids else 1

            scenario = None
            if status_col and stay_status_choices:
                scenario = random.choice(stay_status_choices)
                row[status_col] = scenario

            s = (scenario or "").upper()
            is_cancel = "CANCEL" in s
            is_out = "OUT" in s
            is_in = ("IN" in s) and not is_out

            if is_cancel:
                if aci_col:
                    row[aci_col] = None
                if aco_col:
                    row[aco_col] = None
            elif is_out:
                ci = fake.date_time_between(start_date="-180d", end_date="now", tzinfo=timezone.utc)
                co = ci + timedelta(days=random.randint(1, 10), hours=random.randint(0, 6), minutes=random.randint(0, 59))
                if aci_col:
                    row[aci_col] = ci
                if aco_col:
                    row[aco_col] = co
            elif is_in:
                ci = fake.date_time_between(start_date="-180d", end_date="now", tzinfo=timezone.utc)
                if aci_col:
                    row[aci_col] = ci
                if aco_col:
                    row[aco_col] = None
            else:
                if aci_col:
                    row[aci_col] = None
                if aco_col:
                    row[aco_col] = None

            for c in cols:
                if c.column in row:
                    continue
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                else:
                    v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)
                if v is None and not c.is_nullable:
                    if c.data_type.lower() in {"character varying", "character", "text"}:
                        v = unique_text((c.table, c.column), lambda: f"VAL_{uuid.uuid4().hex[:6]}")
                    elif c.data_type.lower() in {"integer", "bigint", "smallint"}:
                        v = 1
                    elif c.data_type.lower() == "boolean":
                        v = False
                    elif c.data_type.lower() == "date":
                        v = date.today()
                    else:
                        v = "VAL"

                # Enforce UNIQUE(column) constraints (single-column) to prevent COPY UniqueViolation
                if c.column in uniq_cols_in_table:
                    # Try a few regenerations before forcing uniqueness
                    for _ in range(50):
                        if _register_unique(c.column, v):
                            break
                        v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)

                row[c.column] = v

            if aci_col and aco_col:
                ci = row.get(aci_col)
                co = row.get(aco_col)
                if ci is not None and co is not None and co < ci:
                    row[aco_col] = ci + timedelta(days=1)

            w.writerow([row.get(cn) for cn in colnames])

    return path


def generate_booking_discount_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
) -> Path:
    """
    booking_discount has UNIQUE(booking_id, promotion_id)
    Generate rows with unique (booking_id, promotion_id) pairs.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    booking_id_col = col_lc.get("booking_id")
    promo_id_col = col_lc.get("promotion_id") or col_lc.get("promo_id")

    if not booking_id_col or not promo_id_col:
        raise RuntimeError("booking_discount expected booking_id and promotion_id columns.")

    booking_ids = list(ref_ids.get("booking", []))
    promo_ids = list(ref_ids.get("promotion", []))

    if not booking_ids or not promo_ids:
        raise RuntimeError("booking_discount needs booking + promotion ids loaded first.")

    seen: set[tuple[Any, Any]] = set()
    rows: List[Dict[str, Any]] = []

    random.shuffle(booking_ids)

    max_pairs = len(booking_ids) * len(promo_ids)
    if n_rows > max_pairs:
        raise RuntimeError(
            f"Requested {n_rows} booking_discount rows but only {max_pairs} unique (booking_id,promotion_id) pairs exist."
        )

    i = 0
    while len(rows) < n_rows:
        b = booking_ids[i % len(booking_ids)]
        promos_for_booking = random.randint(0, 2)
        if promos_for_booking == 0:
            i += 1
            continue

        for _ in range(promos_for_booking):
            p = random.choice(promo_ids)
            pair = (b, p)
            if pair in seen:
                continue
            seen.add(pair)

            row: Dict[str, Any] = {booking_id_col: b, promo_id_col: p}

            for c in cols:
                if c.column in row:
                    continue
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                else:
                    v = generate_value(fake, c, len(rows) + 1, enums)
                    if v is None and not c.is_nullable:
                        v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                    row[c.column] = v

            rows.append(row)
            if len(rows) >= n_rows:
                break

        i += 1

        if i > n_rows * 20 and len(rows) < n_rows:
            for b2 in booking_ids:
                for p2 in promo_ids:
                    if len(rows) >= n_rows:
                        break
                    pair = (b2, p2)
                    if pair in seen:
                        continue
                    seen.add(pair)
                    row = {booking_id_col: b2, promo_id_col: p2}
                    for c in cols:
                        if c.column in row:
                            continue
                        fk_key = (table_lc, c.column)
                        if fk_key in fk_map:
                            parent_table, _ = fk_map[fk_key]
                            candidates = ref_ids.get(parent_table, [])
                            row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                        else:
                            v = generate_value(fake, c, len(rows) + 1, enums)
                            if v is None and not c.is_nullable:
                                v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                            row[c.column] = v
                    rows.append(row)
                if len(rows) >= n_rows:
                    break

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)
        for row in rows[:n_rows]:
            w.writerow([row.get(cn) for cn in colnames])

    return path


def generate_review_score_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
) -> Path:
    """
    review_score has UNIQUE(review_id, review_category_id)
    Generate rows with unique (review_id, review_category_id) pairs.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    review_id_col = col_lc.get("review_id")
    category_id_col = col_lc.get("review_category_id") or col_lc.get("category_id")

    if not review_id_col or not category_id_col:
        raise RuntimeError("review_score expected review_id and review_category_id columns.")

    review_ids = list(ref_ids.get("review", []))
    category_ids = list(ref_ids.get("review_category", []))

    if not review_ids or not category_ids:
        raise RuntimeError("review_score needs review + review_category ids loaded first.")

    # Cap to maximum unique pairs available
    max_pairs = len(review_ids) * len(category_ids)
    if n_rows > max_pairs:
        raise RuntimeError(
            f"Requested {n_rows} review_score rows but only {max_pairs} unique (review_id,review_category_id) pairs exist."
        )

    seen: set[tuple[Any, Any]] = set()
    rows: List[Dict[str, Any]] = []

    # Generate by sampling categories per review without replacement to guarantee uniqueness.
    random.shuffle(review_ids)
    i = 0
    while len(rows) < n_rows:
        rid = review_ids[i % len(review_ids)]
        # 1-5 categories per review (bounded by available categories)
        k = random.randint(1, min(5, len(category_ids)))
        for cid in random.sample(category_ids, k=k):
            if len(rows) >= n_rows:
                break
            pair = (rid, cid)
            if pair in seen:
                continue
            seen.add(pair)

            row: Dict[str, Any] = {review_id_col: rid, category_id_col: cid}

            for c in cols:
                if c.column in row:
                    continue
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                else:
                    v = generate_value(fake, c, len(rows) + 1, enums)
                    if v is None and not c.is_nullable:
                        v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                    row[c.column] = v

            rows.append(row)

        i += 1

        # Fallback: if somehow not enough rows (very unlikely), fill deterministically over the cartesian product
        if i > n_rows * 10 and len(rows) < n_rows:
            for rid2 in review_ids:
                for cid2 in category_ids:
                    if len(rows) >= n_rows:
                        break
                    pair = (rid2, cid2)
                    if pair in seen:
                        continue
                    seen.add(pair)
                    row = {review_id_col: rid2, category_id_col: cid2}
                    for c in cols:
                        if c.column in row:
                            continue
                        fk_key = (table_lc, c.column)
                        if fk_key in fk_map:
                            parent_table, _ = fk_map[fk_key]
                            candidates = ref_ids.get(parent_table, [])
                            row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                        else:
                            v = generate_value(fake, c, len(rows) + 1, enums)
                            if v is None and not c.is_nullable:
                                v = 1 if c.data_type.lower() in {"integer", "bigint", "smallint"} else f"VAL_{uuid.uuid4().hex[:6]}"
                            row[c.column] = v
                    rows.append(row)
                if len(rows) >= n_rows:
                    break
            break

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)
        for r in rows:
            w.writerow([r.get(cn) for cn in colnames])

    return path



def generate_booking_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
    unique_cols: Dict[str, Set[str]],
    pk: Optional[PrimaryKey],
) -> Path:
    """
    Enforces booking_check ordering (if columns exist): checkout_date >= checkin_date
    Status is ENUM-safe.
    Also supports UNIQUE(FK) pools for single-column unique FK constraints.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    col_lc = {c.column.lower(): c.column for c in cols}
    table_lc = table.lower()

    checkin_col = col_lc.get("checkin_date")
    checkout_col = col_lc.get("checkout_date")
    status_col = col_lc.get("booking_status") or col_lc.get("status")

    pk_col = pk.columns[0] if (pk and len(pk.columns) == 1) else None
    pk_vals: List[Any] = []

    status_ci = next((c for c in cols if c.column == (status_col or "")), None)
    booking_status_choices = enums.get(status_ci.udt_name.lower(), []) if status_ci else []

    fk_cols_in_table = {c.column for c in cols if (table_lc, c.column) in fk_map}
    uniq_cols_in_table = unique_cols.get(table_lc, set())
    unique_fk_cols = fk_cols_in_table.intersection(uniq_cols_in_table)

    unique_fk_pools: Dict[str, List[Any]] = {}
    for fk_col in unique_fk_cols:
        parent_table, _ = fk_map[(table_lc, fk_col)]
        parent_ids = list(ref_ids.get(parent_table, []))
        random.shuffle(parent_ids)
        unique_fk_pools[fk_col] = parent_ids[:n_rows]

    # Track seen values for single-column UNIQUE constraints
    seen_uniques: Dict[str, Set[Any]] = {c: set() for c in uniq_cols_in_table}



    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)

        for i in range(1, n_rows + 1):
            row: Dict[str, Any] = {}

            if status_col and booking_status_choices:
                row[status_col] = random.choice(booking_status_choices)

            if checkin_col and checkout_col:
                ci = fake.date_between(start_date="-180d", end_date="+365d")
                co = ci + timedelta(days=random.randint(1, 14))
                row[checkin_col] = ci
                row[checkout_col] = co

            for c in cols:
                if c.column in row:
                    continue

                if pk_col and c.column == pk_col:
                    v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)
                    row[c.column] = v
                    pk_vals.append(v)
                    continue

                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]
                    if c.column in unique_fk_pools and unique_fk_pools[c.column]:
                        idx = i - 1
                        row[c.column] = unique_fk_pools[c.column][idx] if idx < len(unique_fk_pools[c.column]) else (
                            None if c.is_nullable else unique_fk_pools[c.column][-1]
                        )
                        continue

                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                    continue

                v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)
                if v is None and not c.is_nullable:
                    if c.data_type.lower() in {"character varying", "character", "text"}:
                        v = unique_text((c.table, c.column), lambda: f"VAL_{uuid.uuid4().hex[:6]}")
                    elif c.data_type.lower() in {"integer", "bigint", "smallint"}:
                        v = 1
                    elif c.data_type.lower() == "boolean":
                        v = False
                    elif c.data_type.lower() == "date":
                        v = date.today()
                    else:
                        v = "VAL"
                row[c.column] = v

            if checkin_col and checkout_col:
                ci = row.get(checkin_col)
                co = row.get(checkout_col)
                if ci is not None and co is not None and co < ci:
                    row[checkout_col] = ci + timedelta(days=1)

            w.writerow([row.get(cn) for cn in colnames])

    # NOTE: we still track pk_vals here, but the main loop will overwrite with DB-truth anyway.
    if pk_col:
        ref_ids[table_lc] = pk_vals
    return path


# -------------------------
# Main CSV generator
# -------------------------
def generate_table_csv(
    *,
    fake: Faker,
    out_dir: Path,
    table: str,
    cols: List[ColumnInfo],
    pk: Optional[PrimaryKey],
    fk_map: Dict[Tuple[str, str], Tuple[str, str]],
    ref_ids: Dict[str, List[Any]],
    n_rows: int,
    enums: Dict[str, List[str]],
    unique_cols: Dict[str, Set[str]],
) -> Path:
    tl = table.lower()

    if tl == "booking_room":
        return generate_booking_room_csv(
            fake=fake, out_dir=out_dir, table=table, cols=cols, fk_map=fk_map, ref_ids=ref_ids, n_rows=n_rows, enums=enums
        )
    if tl == "room_night":
        return generate_room_night_csv(
            fake=fake, out_dir=out_dir, table=table, cols=cols, fk_map=fk_map, ref_ids=ref_ids, n_rows=n_rows, enums=enums
        )
    if tl == "stay":
        return generate_stay_csv(
            fake=fake,
            out_dir=out_dir,
            table=table,
            cols=cols,
            fk_map=fk_map,
            ref_ids=ref_ids,
            n_rows=n_rows,
            enums=enums,
            unique_cols=unique_cols,
        )
    if tl == "booking":
        return generate_booking_csv(
            fake=fake,
            out_dir=out_dir,
            table=table,
            cols=cols,
            fk_map=fk_map,
            ref_ids=ref_ids,
            n_rows=n_rows,
            enums=enums,
            unique_cols=unique_cols,
            pk=pk,
        )
    if tl == "booking_discount":
        return generate_booking_discount_csv(
            fake=fake,
            out_dir=out_dir,
            table=table,
            cols=cols,
            fk_map=fk_map,
            ref_ids=ref_ids,
            n_rows=n_rows,
            enums=enums,
        )
    if tl == "review_score":
        return generate_review_score_csv(
            fake=fake,
            out_dir=out_dir,
            table=table,
            cols=cols,
            fk_map=fk_map,
            ref_ids=ref_ids,
            n_rows=n_rows,
            enums=enums,
        )


    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{table}.csv"
    colnames = [c.column for c in cols]
    table_lc = table.lower()

    pk_col = pk.columns[0] if (pk and len(pk.columns) == 1) else None
    pk_vals: List[Any] = []

    # UNIQUE(FK) auto-detection for 1:1 tables
    fk_cols_in_table = {c.column for c in cols if (table_lc, c.column) in fk_map}
    uniq_cols_in_table = unique_cols.get(table_lc, set())
    unique_fk_cols = fk_cols_in_table.intersection(uniq_cols_in_table)

    unique_fk_pools: Dict[str, List[Any]] = {}
    for fk_col in unique_fk_cols:
        parent_table, _ = fk_map[(table_lc, fk_col)]
        parent_ids = list(ref_ids.get(parent_table, []))
        random.shuffle(parent_ids)
        unique_fk_pools[fk_col] = parent_ids[:n_rows]

    # Track seen values for single-column UNIQUE constraints
    seen_uniques: Dict[str, Set[Any]] = {c: set() for c in uniq_cols_in_table}

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(colnames)

        # Start/end date coherence (generic)
        col_lc = {c.column.lower(): c.column for c in cols}
        start_keys = {"start_date", "from_date", "valid_from", "effective_start_date", "block_start_date"}
        end_keys = {"end_date", "to_date", "valid_to", "effective_end_date", "block_end_date", "expires_on"}

        start_col = next((col_lc[k] for k in start_keys if k in col_lc), None)
        end_col = next((col_lc[k] for k in end_keys if k in col_lc), None)

        for i in range(1, n_rows + 1):
            row: Dict[str, Any] = {}

            if start_col and end_col:
                d_from = fake.date_between(start_date="-365d", end_date="+365d")
                d_to = d_from + timedelta(days=random.randint(1, 60))
                row[start_col] = d_from
                row[end_col] = d_to

            for c in cols:
                if c.column in row:
                    continue

                # PK
                if pk_col and c.column == pk_col:
                    v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)
                    row[c.column] = v
                    pk_vals.append(v)
                    continue

                # FK
                fk_key = (table_lc, c.column)
                if fk_key in fk_map:
                    parent_table, _ = fk_map[fk_key]

                    # UNIQUE(FK): assign without replacement
                    if c.column in unique_fk_pools and unique_fk_pools[c.column]:
                        idx = i - 1
                        row[c.column] = unique_fk_pools[c.column][idx] if idx < len(unique_fk_pools[c.column]) else (
                            None if c.is_nullable else unique_fk_pools[c.column][-1]
                        )
                        continue

                    candidates = ref_ids.get(parent_table, [])
                    row[c.column] = random.choice(candidates) if candidates else (None if c.is_nullable else 1)
                    continue

                v = generate_value(fake, c, i, enums)

                # Enforce single-column UNIQUE constraints (safe for NULLs)
                if c.column in seen_uniques and v is not None:
                    tries = 0
                    max_tries = 50
                    while v in seen_uniques[c.column] and tries < max_tries:
                        tries += 1
                        v = generate_value(fake, c, i + tries, enums)

                    if v in seen_uniques[c.column]:
                        # Force uniqueness deterministically as a last resort
                        if isinstance(v, str):
                            suffix = uuid.uuid4().hex[:6]
                            maxlen = c.character_maximum_length or 255
                            keep = max(1, maxlen - (1 + len(suffix)))
                            v = (str(v)[:keep] + "_" + suffix)[:maxlen]
                        elif isinstance(v, int):
                            v = int(v) + (i * 1000) + tries
                        else:
                            v = f"{v}_{uuid.uuid4().hex[:6]}"

                    seen_uniques[c.column].add(v)
                if v is None and not c.is_nullable:
                    if c.data_type.lower() in {"character varying", "character", "text"}:
                        v = unique_text((c.table, c.column), lambda: f"VAL_{uuid.uuid4().hex[:6]}")
                    elif c.data_type.lower() in {"integer", "bigint", "smallint"}:
                        v = 1
                    elif c.data_type.lower() == "boolean":
                        v = False
                    elif c.data_type.lower() == "date":
                        v = date.today()
                    else:
                        v = "VAL"
                row[c.column] = v

            w.writerow([row.get(cn) for cn in colnames])

    # NOTE: main loop overwrites with DB-truth anyway.
    if pk_col:
        ref_ids[table_lc] = pk_vals

    return path


# -------------------------
# COPY load + TRUNCATE + PK cache
# -------------------------
def copy_csv_to_postgres(conn, schema: str, table: str, csv_path: Path, columns: List[str]):
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}"')
        with csv_path.open("r", encoding="utf-8") as f:
            next(f)
            cols_sql = ", ".join([f'"{c}"' for c in columns])
            cur.copy_expert(
                f'COPY "{table}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV)',
                f,
            )


def truncate_tables(conn, schema: str, load_order: List[str]):
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}"')
        for t in reversed(load_order):
            cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE;')


def cache_pk_values(conn, schema: str, table: str, pk: Optional[PrimaryKey], ref_ids: Dict[str, List[Any]]):
    """
    ✅ After loading a table, cache its single-column PK values into ref_ids[table_lc].
    This fixes cases like check_in.stay_id where stay ids must exist in ref_ids["stay"].
    """
    if not pk or len(pk.columns) != 1:
        return

    table_lc = table.lower()
    pk_col = pk.columns[0]

    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(f'SELECT "{pk_col}" FROM "{table}" ORDER BY "{pk_col}"')
        ref_ids[table_lc] = [r[0] for r in cur.fetchall()]


# -------------------------
# MAIN
# -------------------------
def main():
    random.seed(SEED)
    fake = Faker()
    Faker.seed(SEED)

    conn = psycopg2.connect(pg_dsn(PG))
    conn.autocommit = True
    schema = PG.schema

    tables = fetch_tables(conn, schema)
    cols_by_table = fetch_columns(conn, schema)
    pks = fetch_primary_keys(conn, schema)
    fks = fetch_foreign_keys(conn, schema)
    enums = fetch_enum_values(conn)
    unique_cols = fetch_unique_columns(conn, schema)

    fk_map = build_fk_map(fks)
    load_order = topo_sort_tables(tables, fks)

    rc = default_row_counts(tables)
    for k, v in ROW_COUNTS_OVERRIDE.items():
        if k in rc:
            rc[k] = v

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Schema: {schema}", flush=True)
    print(f"Tables: {len(tables)}", flush=True)
    print(f"Enums detected: {len(enums)}", flush=True)
    print(f"Output dir: {OUT_DIR.resolve()}", flush=True)

    if TRUNCATE_FIRST:
        print("Truncating tables...", flush=True)
        truncate_tables(conn, schema, load_order)
        print("Truncate done.", flush=True)

    # ✅ Always use lowercase keys in ref_ids
    ref_ids: Dict[str, List[Any]] = {}

    for t in load_order:
        cols = cols_by_table.get(t, [])
        if not cols:
            continue
        n = int(rc.get(t, 0))
        if n <= 0:
            continue

        print(f"→ {t}: generating {n:,}", flush=True)
        csv_path = generate_table_csv(
            fake=fake,
            out_dir=OUT_DIR,
            table=t,
            cols=cols,
            pk=pks.get(t),
            fk_map=fk_map,
            ref_ids=ref_ids,
            n_rows=n,
            enums=enums,
            unique_cols=unique_cols,
        )

        print(f"→ {t}: loading via COPY", flush=True)
        copy_csv_to_postgres(conn, schema, t, csv_path, [c.column for c in cols])
        print(f"✅ {t}: generated+loaded {n:,} rows", flush=True)

        # ✅ CRITICAL: cache real PK ids for downstream FK generation
        cache_pk_values(conn, schema, t, pks.get(t), ref_ids)

    conn.close()
    print("✅ DONE", flush=True)


if __name__ == "__main__":
    main()


# In[ ]:





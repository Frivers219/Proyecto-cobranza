"""
etl_pipeline.py
───────────────────────────────────────────────────────────────────────────
ETL: Excel/CSV  →  SQL Server (o SQLite para demo local)
Cobranza Analytics — Proyecto 1

Uso:
    # Con SQL Server real:
    python etl_pipeline.py --mode sqlserver --server MISERVIDOR --db CobranzaDB

    # Modo demo local (SQLite, no requiere SQL Server):
    python etl_pipeline.py --mode sqlite

Autor: Fernando Ríos — github.com/fernandorios
"""

import argparse
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_cobranza.log"),
    ],
)
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"


# ══════════════════════════════════════════════════════════════════════════
# 1. EXTRACT
# ══════════════════════════════════════════════════════════════════════════
def extract() -> dict[str, pd.DataFrame]:
    """Lee los archivos fuente (Excel o CSV de respaldo)."""
    log.info("── EXTRACCIÓN ──────────────────────────────────────────")
    tables = {}
    for name in ["cartera_cobranza", "gestiones_cobranza", "pagos_cobranza"]:
        xlsx = DATA_DIR / f"{name}.xlsx"
        csv  = DATA_DIR / f"{name}.csv"
        if xlsx.exists():
            df = pd.read_excel(xlsx)
            log.info(f"  ✔ {name}.xlsx → {len(df):,} filas")
        elif csv.exists():
            df = pd.read_csv(csv)
            log.info(f"  ✔ {name}.csv  → {len(df):,} filas")
        else:
            log.error(f"  ✘ No se encontró {name}. Ejecuta generate_data.py primero.")
            sys.exit(1)
        tables[name] = df
    return tables


# ══════════════════════════════════════════════════════════════════════════
# 2. TRANSFORM
# ══════════════════════════════════════════════════════════════════════════
def transform(tables: dict) -> dict[str, pd.DataFrame]:
    log.info("── TRANSFORMACIÓN ──────────────────────────────────────")

    # ── Cartera ────────────────────────────────────────────────────────
    c = tables["cartera_cobranza"].copy()

    # Fechas
    for col in ["fecha_emision", "fecha_vencimiento"]:
        c[col] = pd.to_datetime(c[col], errors="coerce")

    # Nulos y tipos
    c["dias_mora"]      = c["dias_mora"].fillna(0).astype(int)
    c["monto_original"] = c["monto_original"].round(0).astype(float)
    c["monto_pagado"]   = c["monto_pagado"].fillna(0).round(0).astype(float)
    c["saldo_deudor"]   = (c["monto_original"] - c["monto_pagado"]).round(0)

    # Indicadores derivados
    c["pct_recuperado"] = (c["monto_pagado"] / c["monto_original"] * 100).round(2)
    c["mes_vencimiento"] = c["fecha_vencimiento"].dt.to_period("M").astype(str)
    c["anio_vencimiento"] = c["fecha_vencimiento"].dt.year

    # Score de riesgo simple (0–100, mayor = más riesgo)
    def risk_score(row):
        score = 0
        score += min(row["dias_mora"] / 3, 40)            # max 40 pts por mora
        score += {"Grande": 0, "Mediana": 10, "Pequeña": 20}.get(row["segmento"], 10)
        score += {"Castigada": 30, "Vencida": 20, "En Gestión": 15,
                  "Acuerdo de Pago": 10, "Vigente": 0, "Pagada": 0}.get(row["estado"], 0)
        return min(round(score, 1), 100)

    c["risk_score"] = c.apply(risk_score, axis=1)

    log.info(f"  ✔ cartera: {len(c):,} filas | saldo total: ${c['saldo_deudor'].sum():,.0f}")

    # ── Gestiones ──────────────────────────────────────────────────────
    g = tables["gestiones_cobranza"].copy()
    g["fecha"] = pd.to_datetime(g["fecha"], errors="coerce")
    g["observacion"] = g["observacion"].fillna("").str.strip()
    g["es_promesa"] = g["resultado"].str.contains("Promesa|Acuerdo", case=False, na=False).astype(int)
    g["es_contacto_exitoso"] = g["resultado"].isin(["Contactado", "Promesa de pago", "Acuerdo firmado"]).astype(int)
    log.info(f"  ✔ gestiones: {len(g):,} filas | promesas: {g['es_promesa'].sum()}")

    # ── Pagos ──────────────────────────────────────────────────────────
    p = tables["pagos_cobranza"].copy()
    p["fecha_pago"] = pd.to_datetime(p["fecha_pago"], errors="coerce")
    p["monto"] = p["monto"].round(0).astype(float)
    p["mes_pago"] = p["fecha_pago"].dt.to_period("M").astype(str)
    p["anio_pago"] = p["fecha_pago"].dt.year
    log.info(f"  ✔ pagos: {len(p):,} filas | total recaudado: ${p['monto'].sum():,.0f}")

    # ── Tabla de resumen por cliente ───────────────────────────────────
    resumen = (
        c.groupby(["cliente_id", "cliente_nombre", "industria", "segmento", "region", "ejecutivo"])
        .agg(
            total_documentos=("folio", "count"),
            monto_total=("monto_original", "sum"),
            saldo_total=("saldo_deudor", "sum"),
            monto_recaudado=("monto_pagado", "sum"),
            dias_mora_promedio=("dias_mora", "mean"),
            risk_score_promedio=("risk_score", "mean"),
        )
        .reset_index()
        .round(2)
    )
    resumen["pct_recuperacion"] = (resumen["monto_recaudado"] / resumen["monto_total"] * 100).round(2)
    log.info(f"  ✔ resumen_clientes: {len(resumen):,} clientes")

    return {
        "dim_cartera":        c,
        "fact_gestiones":     g,
        "fact_pagos":         p,
        "resumen_clientes":   resumen,
    }


# ══════════════════════════════════════════════════════════════════════════
# 3. LOAD
# ══════════════════════════════════════════════════════════════════════════
SQL_SCHEMA = """
-- ═══════════════════════════════════════════════════
-- DDL: Base de Datos Cobranza Analytics
-- Compatible: SQL Server 2019+ / Azure SQL
-- Generado por ETL Pipeline — Fernando Ríos
-- ═══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_cartera (
    folio             INTEGER PRIMARY KEY,
    cliente_id        VARCHAR(10),
    cliente_nombre    VARCHAR(100),
    industria         VARCHAR(50),
    segmento          VARCHAR(20),
    ejecutivo         VARCHAR(100),
    fecha_emision     DATE,
    fecha_vencimiento DATE,
    dias_mora         INTEGER,
    tramo_mora        VARCHAR(20),
    monto_original    DECIMAL(18,2),
    monto_pagado      DECIMAL(18,2),
    saldo_deudor      DECIMAL(18,2),
    estado            VARCHAR(30),
    region            VARCHAR(50),
    pct_recuperado    DECIMAL(6,2),
    mes_vencimiento   VARCHAR(10),
    anio_vencimiento  INTEGER,
    risk_score        DECIMAL(5,1)
);

CREATE TABLE IF NOT EXISTS fact_gestiones (
    gestion_id          INTEGER PRIMARY KEY,
    folio               INTEGER,
    cliente_id          VARCHAR(10),
    ejecutivo           VARCHAR(100),
    fecha               DATE,
    tipo                VARCHAR(30),
    resultado           VARCHAR(50),
    observacion         TEXT,
    es_promesa          INTEGER,
    es_contacto_exitoso INTEGER
);

CREATE TABLE IF NOT EXISTS fact_pagos (
    pago_id    INTEGER PRIMARY KEY,
    folio      INTEGER,
    cliente_id VARCHAR(10),
    fecha_pago DATE,
    monto      DECIMAL(18,2),
    metodo     VARCHAR(30),
    mes_pago   VARCHAR(10),
    anio_pago  INTEGER
);

CREATE TABLE IF NOT EXISTS resumen_clientes (
    cliente_id            VARCHAR(10) PRIMARY KEY,
    cliente_nombre        VARCHAR(100),
    industria             VARCHAR(50),
    segmento              VARCHAR(20),
    region                VARCHAR(50),
    ejecutivo             VARCHAR(100),
    total_documentos      INTEGER,
    monto_total           DECIMAL(18,2),
    saldo_total           DECIMAL(18,2),
    monto_recaudado       DECIMAL(18,2),
    dias_mora_promedio    DECIMAL(8,2),
    risk_score_promedio   DECIMAL(5,2),
    pct_recuperacion      DECIMAL(6,2)
);
"""


def load_sqlite(tables: dict, db_path: str = "cobranza.db"):
    """Carga en SQLite (modo demo/portátil)."""
    log.info(f"── CARGA → SQLite: {db_path} ───────────────────────────")
    conn = sqlite3.connect(db_path)

    for name, df in tables.items():
        df_clean = df.copy()
        # Convertir Period/Timestamp a string para SQLite
        for col in df_clean.columns:
            if df_clean[col].dtype == "object":
                pass
            elif hasattr(df_clean[col], "dt"):
                df_clean[col] = df_clean[col].astype(str)
        df_clean.to_sql(name, conn, if_exists="replace", index=False)
        log.info(f"  ✔ {name}: {len(df_clean):,} filas cargadas")

    conn.close()
    log.info(f"  ✅ Base de datos SQLite lista: {db_path}")


def load_sqlserver(tables: dict, server: str, db: str, trusted: bool = True,
                   user: str = None, password: str = None):
    """
    Carga en SQL Server usando pyodbc / SQLAlchemy.
    Requiere: pip install pyodbc sqlalchemy
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        log.error("Instala sqlalchemy y pyodbc: pip install sqlalchemy pyodbc")
        sys.exit(1)

    if trusted:
        conn_str = (
            f"mssql+pyodbc://{server}/{db}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes"
            f"&TrustServerCertificate=yes"
        )
    else:
        conn_str = (
            f"mssql+pyodbc://{user}:{password}@{server}/{db}"
            f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )

    log.info(f"── CARGA → SQL Server: {server}/{db} ───────────────────")
    engine = create_engine(conn_str, fast_executemany=True)

    with engine.connect() as conn:
        conn.execute(text(SQL_SCHEMA.replace("IF NOT EXISTS", "")))

    for name, df in tables.items():
        df_clean = df.copy()
        for col in df_clean.columns:
            if hasattr(df_clean[col], "dt"):
                df_clean[col] = df_clean[col].astype(str)
        df_clean.to_sql(name, engine, if_exists="replace", index=False, chunksize=500)
        log.info(f"  ✔ {name}: {len(df_clean):,} filas")

    log.info("  ✅ Carga en SQL Server completada.")


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="ETL Cobranza Analytics")
    parser.add_argument("--mode",     choices=["sqlite", "sqlserver"], default="sqlite")
    parser.add_argument("--server",   default="localhost")
    parser.add_argument("--db",       default="CobranzaDB")
    parser.add_argument("--trusted",  action="store_true", default=True)
    parser.add_argument("--user",     default=None)
    parser.add_argument("--password", default=None)
    args = parser.parse_args()

    log.info("═" * 55)
    log.info("  ETL COBRANZA ANALYTICS")
    log.info(f"  Inicio: {datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info("═" * 55)

    raw    = extract()
    clean  = transform(raw)

    if args.mode == "sqlite":
        load_sqlite(clean, db_path=str(Path(__file__).parent.parent / "data" / "cobranza.db"))
    else:
        load_sqlserver(clean, server=args.server, db=args.db,
                       trusted=args.trusted, user=args.user, password=args.password)

    log.info("═" * 55)
    log.info("  ETL FINALIZADO ✅")
    log.info("═" * 55)


if __name__ == "__main__":
    main()

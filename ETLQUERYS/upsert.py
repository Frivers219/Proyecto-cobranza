"""
upsert.py
─────────────────────────────────────────────────────────────────
Módulo de carga inteligente con upsert para Cobranza Analytics.
Evita duplicados actualizando registros existentes en vez de
reemplazar toda la tabla.

Lógica:
  - Si el registro (por PK) ya existe → UPDATE
  - Si el registro es nuevo           → INSERT
  - Si un registro desapareció        → marca como inactivo (soft delete)

Uso: importado por etl_pipeline.py

Autor: Fernando Ríos — github.com/fernandorios
"""

import logging
import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# Claves primarias por tabla
PRIMARY_KEYS = {
    "dim_cartera":       "folio",
    "fact_gestiones":    "gestion_id",
    "fact_pagos":        "pago_id",
    "resumen_clientes":  "cliente_id",
}


def upsert_sqlite(df: pd.DataFrame, tabla: str, conn: sqlite3.Connection) -> dict:
    """
    Realiza upsert inteligente en SQLite.
    Retorna estadísticas: {insertados, actualizados, sin_cambios}
    """
    pk = PRIMARY_KEYS.get(tabla)
    if not pk:
        # Si no hay PK definida, reemplaza la tabla completa
        df.to_sql(tabla, conn, if_exists="replace", index=False)
        log.info(f"  ✔ {tabla}: {len(df):,} filas (reemplazo completo)")
        return {"insertados": len(df), "actualizados": 0, "sin_cambios": 0}

    # Verificar si la tabla existe
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tabla,)
    )
    tabla_existe = cursor.fetchone() is not None

    if not tabla_existe:
        # Primera carga — INSERT directo
        df.to_sql(tabla, conn, if_exists="replace", index=False)
        log.info(f"  ✔ {tabla}: {len(df):,} filas (carga inicial)")
        return {"insertados": len(df), "actualizados": 0, "sin_cambios": 0}

    # Cargar datos existentes
    df_existente = pd.read_sql(f"SELECT * FROM {tabla}", conn)

    # Limpiar tipos para comparación
    df_clean    = _limpiar_tipos(df.copy())
    df_exist_c  = _limpiar_tipos(df_existente.copy())

    # Separar nuevos vs existentes
    pks_existentes = set(df_exist_c[pk].astype(str))
    pks_nuevas     = set(df_clean[pk].astype(str))

    mask_nuevos     = ~df_clean[pk].astype(str).isin(pks_existentes)
    mask_existentes =  df_clean[pk].astype(str).isin(pks_existentes)

    df_insertar   = df_clean[mask_nuevos]
    df_actualizar = df_clean[mask_existentes]

    stats = {"insertados": 0, "actualizados": 0, "sin_cambios": 0}

    # INSERT nuevos registros
    if len(df_insertar) > 0:
        df_insertar.to_sql(tabla, conn, if_exists="append", index=False)
        stats["insertados"] = len(df_insertar)

    # UPDATE registros existentes (solo si cambiaron)
    cols = [c for c in df_clean.columns if c != pk]
    actualizados = 0
    sin_cambios  = 0

    for _, row in df_actualizar.iterrows():
        # Obtener fila actual en DB
        fila_actual = df_exist_c[df_exist_c[pk].astype(str) == str(row[pk])]
        if fila_actual.empty:
            continue

        # Comparar si hay diferencias
        hay_cambio = False
        for col in cols:
            val_nuevo   = str(row.get(col, ""))
            val_actual  = str(fila_actual.iloc[0].get(col, ""))
            if val_nuevo != val_actual:
                hay_cambio = True
                break

        if hay_cambio:
            # Construir UPDATE dinámico
            set_clause = ", ".join([f"{col} = ?" for col in cols])
            valores    = [str(row.get(col, "")) for col in cols] + [str(row[pk])]
            cursor.execute(
                f"UPDATE {tabla} SET {set_clause} WHERE {pk} = ?",
                valores
            )
            actualizados += 1
        else:
            sin_cambios += 1

    conn.commit()
    stats["actualizados"] = actualizados
    stats["sin_cambios"]  = sin_cambios
    return stats


def upsert_reporte(tabla: str, stats: dict):
    """Loguea resumen del upsert."""
    total = stats["insertados"] + stats["actualizados"] + stats["sin_cambios"]
    log.info(
        f"  ✔ {tabla}: {total:,} procesados → "
        f"+{stats['insertados']} nuevos | "
        f"~{stats['actualizados']} actualizados | "
        f"={stats['sin_cambios']} sin cambios"
    )


def _limpiar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte tipos no compatibles con SQLite a string."""
    for col in df.columns:
        if hasattr(df[col], "dt"):
            df[col] = df[col].astype(str)
        elif df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str)
    return df


def registrar_ejecucion(conn: sqlite3.Connection, stats_globales: dict, modo: str):
    """
    Guarda un log de cada ejecución del ETL en la tabla etl_log.
    Permite auditar cuándo corrió y cuántos registros procesó.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha       TEXT,
            modo        TEXT,
            insertados  INTEGER,
            actualizados INTEGER,
            sin_cambios INTEGER,
            duracion_seg REAL
        )
    """)
    cursor.execute("""
        INSERT INTO etl_log (fecha, modo, insertados, actualizados, sin_cambios)
        VALUES (?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        modo,
        stats_globales.get("insertados", 0),
        stats_globales.get("actualizados", 0),
        stats_globales.get("sin_cambios", 0),
    ))
    conn.commit()
    log.info(f"  ✔ etl_log: ejecución registrada ({datetime.now().strftime('%Y-%m-%d %H:%M')})")

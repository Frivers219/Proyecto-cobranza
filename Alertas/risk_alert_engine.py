"""
risk_alert_engine.py
────────────────────────────────────────────────────────────────────
Motor de Alertas Nivel 2 — Detección de cambios de riesgo.

Detecta y notifica SOLO cuando ocurre algo relevante:
  1. Cliente sube de banda de riesgo (Bajo→Medio, Medio→Alto, Alto→Crítico)
  2. Risk score sube más de 15 puntos en una semana
  3. Cliente entra por PRIMERA VEZ a banda Crítica

Si no hay cambios relevantes → NO envía email (silencio total).

Uso:
    python risk_alert_engine.py              # ejecuta con config.json
    python risk_alert_engine.py --dry-run    # preview sin enviar

Autor: Fernando Ríos — github.com/fernandorios
"""

import argparse
import json
import logging
import smtplib
import sqlite3
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

# ── Logging ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("risk_alertas.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

CONFIG_PATH   = Path(__file__).parent / "config.json"
SNAPSHOT_PATH = Path(__file__).parent / "risk_snapshot.csv"
DB_PATH       = Path(__file__).parent / "cobranza.db"


# ══════════════════════════════════════════════════════════════════
# BANDAS DE RIESGO basadas en tramo de mora
# ══════════════════════════════════════════════════════════════════
BANDA_POR_TRAMO = {
    "0-30 días":   "Bajo",
    "31-60 días":  "Medio",
    "61-90 días":  "Medio",
    "91-180 días": "Alto",
    ">180 días":   "Crítico",
}

ORDEN_BANDA = {"Bajo": 1, "Medio": 2, "Alto": 3, "Crítico": 4}

COLORES_BANDA = {
    "Bajo":    "#16a34a",
    "Medio":   "#d97706",
    "Alto":    "#ea580c",
    "Crítico": "#dc2626",
}

ICONOS_BANDA = {
    "Bajo":    "🟢",
    "Medio":   "🟡",
    "Alto":    "🟠",
    "Crítico": "🔴",
}


# ══════════════════════════════════════════════════════════════════
# 1. CARGA DE DATOS
# ══════════════════════════════════════════════════════════════════
def cargar_datos() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT
            folio, cliente_id, cliente_nombre, ejecutivo,
            tramo_mora, dias_mora, risk_score,
            saldo_deudor, estado, segmento, industria
        FROM dim_cartera
        WHERE estado NOT IN ('Vigente', 'Pagada')
          AND saldo_deudor > 0
    """, conn)
    conn.close()

    # Calcular banda de riesgo según tramo
    df["banda_riesgo"] = df["tramo_mora"].map(BANDA_POR_TRAMO).fillna("Medio")
    df["orden_banda"]  = df["banda_riesgo"].map(ORDEN_BANDA)
    log.info(f"  ✔ Cartera cargada: {len(df):,} documentos")
    return df


# ══════════════════════════════════════════════════════════════════
# 2. GESTIÓN DEL SNAPSHOT
# ══════════════════════════════════════════════════════════════════
def cargar_snapshot() -> pd.DataFrame:
    """Carga el snapshot anterior o retorna vacío si es la primera vez."""
    if not SNAPSHOT_PATH.exists():
        log.info("  ℹ️  Sin snapshot previo — primera ejecución.")
        return pd.DataFrame()

    df = pd.read_csv(SNAPSHOT_PATH)
    log.info(f"  ✔ Snapshot cargado: {len(df):,} registros previos")
    return df


def guardar_snapshot(df: pd.DataFrame):
    """Guarda el estado actual como snapshot para la próxima comparación."""
    cols = ["folio", "cliente_nombre", "ejecutivo",
            "tramo_mora", "dias_mora", "risk_score", "banda_riesgo",
            "saldo_deudor"]
    df[cols].to_csv(SNAPSHOT_PATH, index=False)
    log.info(f"  ✔ Snapshot actualizado: {len(df):,} registros")


# ══════════════════════════════════════════════════════════════════
# 3. DETECCIÓN DE CAMBIOS DE RIESGO
# ══════════════════════════════════════════════════════════════════
def detectar_cambios(df_actual: pd.DataFrame, df_prev: pd.DataFrame) -> dict:
    """
    Compara estado actual vs snapshot anterior.
    Retorna dict con tres categorías de alertas.
    """
    if df_prev.empty:
        # Primera ejecución — guardamos snapshot y no alertamos
        guardar_snapshot(df_actual)
        return {"banda_subio": pd.DataFrame(), "score_subio": pd.DataFrame(), "nuevo_critico": pd.DataFrame()}

    # Join por folio
    merged = df_actual.merge(
        df_prev[["folio", "banda_riesgo", "risk_score"]].rename(columns={
            "banda_riesgo": "banda_anterior",
            "risk_score":   "score_anterior",
        }),
        on="folio",
        how="inner"
    )

    merged["orden_anterior"] = merged["banda_anterior"].map(ORDEN_BANDA).fillna(1)
    merged["delta_score"]    = merged["risk_score"] - merged["score_anterior"].astype(float)

    # ── Alerta 1: Banda subió ──────────────────────────────────────
    banda_subio = merged[
        merged["orden_banda"] > merged["orden_anterior"]
    ].copy()
    banda_subio["cambio_desc"] = (
        banda_subio["banda_anterior"] + " → " + banda_subio["banda_riesgo"]
    )

    # ── Alerta 2: Score subió +15 puntos ──────────────────────────
    score_subio = merged[
        (merged["delta_score"] >= 15) &
        (merged["orden_banda"] <= merged["orden_anterior"])  # evitar duplicar con alerta 1
    ].copy()
    score_subio["cambio_desc"] = score_subio["delta_score"].apply(
        lambda d: f"+{d:.0f} pts esta semana"
    )

    # ── Alerta 3: Nuevo en banda Crítica ──────────────────────────
    nuevo_critico = merged[
        (merged["banda_riesgo"] == "Crítico") &
        (merged["banda_anterior"] != "Crítico")
    ].copy()
    nuevo_critico["cambio_desc"] = "Ingresó por primera vez a banda Crítica"

    # Actualizar snapshot con estado actual
    guardar_snapshot(df_actual)

    log.info(f"  ✔ Bandas subidas:    {len(banda_subio)}")
    log.info(f"  ✔ Score +15pts:      {len(score_subio)}")
    log.info(f"  ✔ Nuevos críticos:   {len(nuevo_critico)}")

    return {
        "banda_subio":   banda_subio,
        "score_subio":   score_subio,
        "nuevo_critico": nuevo_critico,
    }


def hay_alertas(cambios: dict) -> bool:
    return any(len(v) > 0 for v in cambios.values())


# ══════════════════════════════════════════════════════════════════
# 4. CONSTRUCCIÓN DEL EMAIL HTML
# ══════════════════════════════════════════════════════════════════
def _fmt_clp(valor: float) -> str:
    if valor >= 1_000_000_000:
        return f"${valor/1_000_000_000:.2f}B"
    return f"${valor/1_000_000:.1f}M"


def _tabla_alertas(df: pd.DataFrame, tipo: str) -> str:
    """Genera una tabla HTML para una categoría de alerta."""
    if df.empty:
        return ""

    iconos = {
        "banda_subio":   "⬆️",
        "score_subio":   "📈",
        "nuevo_critico": "🚨",
    }
    titulos = {
        "banda_subio":   "Clientes que subieron de banda de riesgo",
        "score_subio":   "Clientes con alza de Risk Score ≥15 puntos",
        "nuevo_critico": "Clientes que ingresaron por primera vez a banda Crítica",
    }
    colores_header = {
        "banda_subio":   "#d97706",
        "score_subio":   "#ea580c",
        "nuevo_critico": "#dc2626",
    }

    filas = ""
    for _, row in df.head(10).iterrows():
        banda_color = COLORES_BANDA.get(row["banda_riesgo"], "#64748b")
        icono_banda = ICONOS_BANDA.get(row["banda_riesgo"], "⚪")
        filas += f"""
        <tr>
          <td style="padding:9px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;font-weight:500">{row['cliente_nombre']}</td>
          <td style="padding:9px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;color:#64748b">{row['ejecutivo']}</td>
          <td style="padding:9px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace;font-weight:600">{_fmt_clp(row['saldo_deudor'])}</td>
          <td style="padding:9px 12px;border-bottom:1px solid #f1f5f9;text-align:center">
            <span style="background:{banda_color};color:white;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700">{icono_banda} {row['banda_riesgo']}</span>
          </td>
          <td style="padding:9px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;color:#64748b;text-align:center">{row['cambio_desc']}</td>
        </tr>"""

    n = len(df)
    nota_extra = f'<div style="padding:8px 14px;font-size:11px;color:#94a3b8;text-align:right">{"y " + str(n-10) + " más..." if n > 10 else ""}</div>'

    return f"""
    <div style="background:white;border-radius:10px;border:1px solid #e2e8f0;margin-bottom:16px;overflow:hidden">
      <div style="background:{colores_header[tipo]};padding:13px 18px;display:flex;align-items:center;justify-content:space-between">
        <span style="color:white;font-weight:700;font-size:14px">{iconos[tipo]} {titulos[tipo]}</span>
        <span style="background:rgba(255,255,255,.25);color:white;padding:3px 12px;border-radius:100px;font-size:11px;font-weight:700">{n} cliente{'s' if n>1 else ''}</span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        <thead>
          <tr style="background:#f8fafc">
            <th style="padding:8px 12px;text-align:left;font-size:10px;color:#94a3b8;font-weight:700;text-transform:uppercase">Cliente</th>
            <th style="padding:8px 12px;text-align:left;font-size:10px;color:#94a3b8;font-weight:700;text-transform:uppercase">Ejecutivo</th>
            <th style="padding:8px 12px;text-align:right;font-size:10px;color:#94a3b8;font-weight:700;text-transform:uppercase">Saldo</th>
            <th style="padding:8px 12px;text-align:center;font-size:10px;color:#94a3b8;font-weight:700;text-transform:uppercase">Banda Actual</th>
            <th style="padding:8px 12px;text-align:center;font-size:10px;color:#94a3b8;font-weight:700;text-transform:uppercase">Cambio</th>
          </tr>
        </thead>
        <tbody>{filas}</tbody>
      </table>
      {nota_extra}
    </div>"""


def construir_email_riesgo(cambios: dict, df_actual: pd.DataFrame) -> str:
    """Genera el email HTML de alerta de riesgo Nivel 2."""

    total_alertas = sum(len(v) for v in cambios.values())
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M")

    # Resumen ejecutivo de bandas actuales
    resumen_bandas = df_actual.groupby("banda_riesgo").agg(
        clientes=("cliente_nombre", "nunique"),
        saldo=("saldo_deudor", "sum")
    ).reindex(["Bajo", "Medio", "Alto", "Crítico"]).fillna(0)

    kpis_bandas = ""
    for banda, row in resumen_bandas.iterrows():
        color = COLORES_BANDA.get(banda, "#64748b")
        icono = ICONOS_BANDA.get(banda, "⚪")
        kpis_bandas += f"""
        <div style="background:white;border-radius:10px;padding:16px;border:1px solid #e2e8f0;text-align:center;border-top:3px solid {color}">
          <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin-bottom:6px">{icono} {banda}</div>
          <div style="font-size:24px;font-weight:800;color:#0f172a;font-family:monospace">{int(row['clientes'])}</div>
          <div style="font-size:11px;color:#94a3b8;margin-top:4px">{_fmt_clp(row['saldo'])}</div>
        </div>"""

    # Secciones de alertas
    seccion_bandas  = _tabla_alertas(cambios["banda_subio"],   "banda_subio")
    seccion_score   = _tabla_alertas(cambios["score_subio"],   "score_subio")
    seccion_critico = _tabla_alertas(cambios["nuevo_critico"], "nuevo_critico")

    html = f"""
<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f2f7;font-family:'Segoe UI',Arial,sans-serif">
<div style="max-width:680px;margin:0 auto;padding:24px 16px">

  <!-- HEADER URGENTE -->
  <div style="background:linear-gradient(135deg,#7f1d1d 0%,#dc2626 100%);border-radius:12px;padding:26px 32px;margin-bottom:20px">
    <div style="font-size:10px;letter-spacing:.2em;text-transform:uppercase;color:#fca5a5;font-family:monospace;margin-bottom:8px">⚠️ ALERTA DE RIESGO · NIVEL 2</div>
    <div style="font-size:22px;font-weight:800;color:white;margin-bottom:4px">Cambios Críticos en Cartera</div>
    <div style="font-size:13px;color:#fca5a5">{fecha} · {total_alertas} cliente{'s' if total_alertas>1 else ''} requiere{'n' if total_alertas>1 else ''} atención inmediata</div>
  </div>

  <!-- AVISO -->
  <div style="background:#fef3c7;border:1px solid #fcd34d;border-radius:10px;padding:14px 18px;margin-bottom:20px;font-size:13px;color:#92400e">
    <strong>⚡ Este email se envía SOLO cuando hay cambios relevantes.</strong>
    Los clientes listados empeoraron su perfil de riesgo desde la última evaluación.
  </div>

  <!-- KPIs BANDAS -->
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px">
    {kpis_bandas}
  </div>

  <!-- ALERTAS -->
  {seccion_critico}
  {seccion_bandas}
  {seccion_score}

  <!-- FOOTER -->
  <div style="text-align:center;padding:16px;font-size:11px;color:#94a3b8">
    Cobranza Analytics · Alertas Nivel 2 · github.com/fernandorios/cobranza-analytics<br>
    Generado el {fecha} · Este email no se envía si no hay cambios
  </div>

</div>
</body>
</html>"""
    return html


# ══════════════════════════════════════════════════════════════════
# 5. ENVÍO DE EMAIL
# ══════════════════════════════════════════════════════════════════
def enviar_email(html: str, config: dict, n_alertas: int, dry_run: bool = False):
    asunto = f"⚠️ ALERTA RIESGO — {n_alertas} cliente{'s' if n_alertas>1 else ''} con cambio crítico · {datetime.now().strftime('%d/%m/%Y')}"

    if dry_run:
        preview = Path(__file__).parent / "preview_riesgo.html"
        preview.write_text(html, encoding="utf-8")
        log.info(f"  🔍 DRY RUN — Preview guardado en: {preview}")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = config["gmail_user"]
    msg["To"]      = ", ".join(config["destinatarios"])
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config["gmail_user"], config["gmail_app_password"])
            server.sendmail(config["gmail_user"], config["destinatarios"], msg.as_string())
        log.info(f"  ✅ Alerta enviada a: {', '.join(config['destinatarios'])}")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Error de autenticación Gmail.")
    except Exception as e:
        log.error(f"  ❌ Error al enviar: {e}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Motor de Alertas Nivel 2 — Riesgo")
    parser.add_argument("--dry-run", action="store_true", help="Preview sin enviar email")
    parser.add_argument("--forzar",  action="store_true", help="Forzar envío aunque no haya cambios (para testing)")
    args = parser.parse_args()

    log.info("═" * 50)
    log.info("  COBRANZA ANALYTICS — ALERTAS NIVEL 2")
    log.info(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info("═" * 50)

    if not CONFIG_PATH.exists():
        log.error(f"No se encontró config.json en {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    # Pipeline
    df_actual = cargar_datos()
    df_prev   = cargar_snapshot()
    cambios   = detectar_cambios(df_actual, df_prev)

    total = sum(len(v) for v in cambios.values())

    if not hay_alertas(cambios) and not args.forzar:
        log.info("  ✅ Sin cambios relevantes — no se envía email.")
        log.info("  (Usa --forzar para enviar de todas formas)")
        log.info("═" * 50)
        return

    if args.forzar and not hay_alertas(cambios):
        log.info("  ⚡ Modo --forzar activado — generando email de prueba con datos actuales")
        # Para testing: simular algunos cambios ficticios
        muestra = df_actual.head(5).copy()
        muestra["banda_anterior"] = "Bajo"
        muestra["cambio_desc"]    = "Bajo → " + muestra["banda_riesgo"]
        cambios["banda_subio"] = muestra
        total = len(muestra)

    html = construir_email_riesgo(cambios, df_actual)
    log.info(f"  ✔ Email generado — {total} alertas")

    enviar_email(html, config, total, dry_run=args.dry_run)

    log.info("═" * 50)
    log.info("  ALERTAS NIVEL 2 FINALIZADAS ✅")
    log.info("═" * 50)


if __name__ == "__main__":
    main()

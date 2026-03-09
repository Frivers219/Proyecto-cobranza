"""
alert_engine.py
───────────────────────────────────────────────────────────────────
Motor de alertas para Cobranza Analytics.
Detecta cambios de tramo, genera resumen diario y envía emails HTML.

Uso:
    python alert_engine.py               # ejecuta con config.json
    python alert_engine.py --dry-run     # simula sin enviar correos

Autor: Fernando Ríos — github.com/fernandorios
"""

import argparse
import json
import logging
import smtplib
import sqlite3
import sys
from datetime import datetime, timedelta
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
        logging.FileHandler("alertas.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

BASE = Path(__file__).parent.parent
DB_PATH = Path(__file__).parent / "cobranza.db"
CONFIG_PATH = Path(__file__).parent / "config.json"
SNAPSHOT_PATH = Path(__file__).parent / "ultimo_snapshot.csv"


# ══════════════════════════════════════════════════════════════════
# 1. CARGA DE DATOS
# ══════════════════════════════════════════════════════════════════
def cargar_datos() -> pd.DataFrame:
    """Carga la cartera desde SQLite (o SQL Server si se configura)."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT
            folio, cliente_id, cliente_nombre, ejecutivo,
            fecha_vencimiento, dias_mora, tramo_mora,
            monto_original, saldo_deudor, estado, risk_score,
            industria, segmento, region
        FROM dim_cartera
        WHERE estado NOT IN ('Vigente', 'Pagada')
          AND saldo_deudor > 0
    """, conn)
    conn.close()
    log.info(f"  ✔ Cartera cargada: {len(df):,} documentos activos")
    return df


# ══════════════════════════════════════════════════════════════════
# 2. DETECCIÓN DE CAMBIOS DE TRAMO
# ══════════════════════════════════════════════════════════════════
ORDEN_TRAMO = {
    "0-30 días":   1,
    "31-60 días":  2,
    "61-90 días":  3,
    "91-180 días": 4,
    ">180 días":   5,
}

def detectar_cambios_tramo(df_actual: pd.DataFrame) -> pd.DataFrame:
    """
    Compara la cartera actual con el snapshot anterior.
    Retorna documentos que subieron de tramo esta semana.
    """
    if not SNAPSHOT_PATH.exists():
        log.info("  ℹ️  Sin snapshot previo — se crea uno nuevo hoy.")
        df_actual[["folio", "tramo_mora", "dias_mora"]].to_csv(SNAPSHOT_PATH, index=False)
        return pd.DataFrame()  # primera ejecución, sin cambios aún

    df_prev = pd.read_csv(SNAPSHOT_PATH)
    df_prev = df_prev.rename(columns={"tramo_mora": "tramo_anterior", "dias_mora": "dias_mora_anterior"})

    merged = df_actual.merge(df_prev[["folio", "tramo_anterior"]], on="folio", how="inner")
    merged["orden_actual"]   = merged["tramo_mora"].map(ORDEN_TRAMO)
    merged["orden_anterior"] = merged["tramo_anterior"].map(ORDEN_TRAMO)

    cambios = merged[merged["orden_actual"] > merged["orden_anterior"]].copy()
    cambios["cambio_desc"] = cambios["tramo_anterior"] + " → " + cambios["tramo_mora"]

    # Actualizar snapshot
    df_actual[["folio", "tramo_mora", "dias_mora"]].to_csv(SNAPSHOT_PATH, index=False)
    log.info(f"  ✔ Cambios de tramo detectados: {len(cambios)}")
    return cambios[["folio", "cliente_nombre", "ejecutivo", "saldo_deudor",
                    "tramo_anterior", "tramo_mora", "cambio_desc", "risk_score"]]


# ══════════════════════════════════════════════════════════════════
# 3. RESUMEN DIARIO
# ══════════════════════════════════════════════════════════════════
def generar_resumen(df: pd.DataFrame) -> dict:
    """Calcula KPIs del resumen diario del portafolio."""
    total_docs    = len(df)
    saldo_total   = df["saldo_deudor"].sum()
    criticos      = df[df["risk_score"] >= 75]
    por_estado    = df.groupby("estado")["saldo_deudor"].sum().sort_values(ascending=False)
    por_tramo     = df.groupby("tramo_mora")["saldo_deudor"].sum()
    por_ejecutivo = df.groupby("ejecutivo").agg(
        docs=("folio","count"),
        saldo=("saldo_deudor","sum"),
        risk_prom=("risk_score","mean")
    ).round(1).sort_values("saldo", ascending=False)

    top_clientes = (
        df.groupby("cliente_nombre")["saldo_deudor"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
    )

    return {
        "fecha":          datetime.now().strftime("%d/%m/%Y %H:%M"),
        "total_docs":     total_docs,
        "saldo_total":    saldo_total,
        "n_criticos":     len(criticos),
        "saldo_criticos": criticos["saldo_deudor"].sum(),
        "por_estado":     por_estado,
        "por_tramo":      por_tramo,
        "por_ejecutivo":  por_ejecutivo,
        "top_clientes":   top_clientes,
    }


# ══════════════════════════════════════════════════════════════════
# 4. CONSTRUCCIÓN DEL EMAIL HTML
# ══════════════════════════════════════════════════════════════════
def _fmt_clp(valor: float) -> str:
    """Formatea valor en millones CLP."""
    if valor >= 1_000_000_000:
        return f"${valor/1_000_000_000:.2f}B"
    return f"${valor/1_000_000:.1f}M"

def _risk_color(score: float) -> str:
    if score >= 75: return "#dc2626"
    if score >= 50: return "#d97706"
    if score >= 25: return "#2563eb"
    return "#16a34a"

def _tramo_color(tramo: str) -> str:
    colors = {
        "0-30 días":   "#16a34a",
        "31-60 días":  "#2563eb",
        "61-90 días":  "#d97706",
        "91-180 días": "#ea580c",
        ">180 días":   "#dc2626",
    }
    return colors.get(tramo, "#64748b")

def construir_email_html(resumen: dict, cambios: pd.DataFrame) -> str:
    """Genera el cuerpo HTML del email de alerta."""

    # ── Tabla estados ──
    filas_estado = ""
    for estado, saldo in resumen["por_estado"].items():
        filas_estado += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{estado}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace;font-weight:600">{_fmt_clp(saldo)}</td>
        </tr>"""

    # ── Tabla tramos ──
    filas_tramo = ""
    orden = ["0-30 días","31-60 días","61-90 días","91-180 días",">180 días"]
    for tramo in orden:
        if tramo in resumen["por_tramo"]:
            saldo = resumen["por_tramo"][tramo]
            color = _tramo_color(tramo)
            pct   = saldo / resumen["saldo_total"] * 100
            filas_tramo += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};margin-right:6px"></span>
            {tramo}
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace;font-weight:600">{_fmt_clp(saldo)}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;color:#64748b;text-align:right">{pct:.1f}%</td>
        </tr>"""

    # ── Tabla ejecutivos ──
    filas_exec = ""
    for ejecutivo, row in resumen["por_ejecutivo"].iterrows():
        color = _risk_color(row["risk_prom"])
        filas_exec += f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{ejecutivo}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:center">{int(row['docs'])}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace;font-weight:600">{_fmt_clp(row['saldo'])}</td>
          <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center">
            <span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">{row['risk_prom']:.1f}</span>
          </td>
        </tr>"""

    # ── Tabla cambios de tramo ──
    seccion_cambios = ""
    if len(cambios) > 0:
        filas_cambios = ""
        for _, row in cambios.head(10).iterrows():
            color = _tramo_color(row["tramo_mora"])
            filas_cambios += f"""
            <tr>
              <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{row['cliente_nombre']}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:12px;color:#64748b">{row['ejecutivo']}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace">{_fmt_clp(row['saldo_deudor'])}</td>
              <td style="padding:8px 12px;border-bottom:1px solid #f1f5f9;text-align:center">
                <span style="background:{color};color:white;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700">{row['cambio_desc']}</span>
              </td>
            </tr>"""

        n = len(cambios)
        seccion_cambios = f"""
        <!-- CAMBIOS DE TRAMO -->
        <div style="background:#fff;border-radius:10px;border:1px solid #e2e8f0;margin-bottom:20px;overflow:hidden">
          <div style="background:#7c3aed;padding:14px 20px;display:flex;align-items:center;justify-content:space-between">
            <span style="color:white;font-weight:700;font-size:15px">⚠️ Documentos que subieron de tramo</span>
            <span style="background:rgba(255,255,255,.2);color:white;padding:3px 12px;border-radius:100px;font-size:12px;font-weight:700">{n} documentos</span>
          </div>
          <table style="width:100%;border-collapse:collapse">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:9px 12px;text-align:left;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.05em">Cliente</th>
                <th style="padding:9px 12px;text-align:left;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.05em">Ejecutivo</th>
                <th style="padding:9px 12px;text-align:right;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.05em">Saldo</th>
                <th style="padding:9px 12px;text-align:center;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase;letter-spacing:.05em">Cambio</th>
              </tr>
            </thead>
            <tbody>{filas_cambios}</tbody>
          </table>
        </div>"""
    else:
        seccion_cambios = """
        <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:16px 20px;margin-bottom:20px;color:#166534;font-size:13px">
          ✅ <strong>Sin cambios de tramo esta semana</strong> — ningún documento empeoró su antigüedad.
        </div>"""

    # ── Top clientes ──
    filas_top = ""
    for cliente, saldo in resumen["top_clientes"].items():
        filas_top += f"""
        <tr>
          <td style="padding:7px 12px;border-bottom:1px solid #f1f5f9;font-size:13px">{cliente}</td>
          <td style="padding:7px 12px;border-bottom:1px solid #f1f5f9;font-size:13px;text-align:right;font-family:monospace;font-weight:600;color:#dc2626">{_fmt_clp(saldo)}</td>
        </tr>"""

    html = f"""
<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f2f7;font-family:'Segoe UI',Arial,sans-serif">

<div style="max-width:680px;margin:0 auto;padding:24px 16px">

  <!-- HEADER -->
  <div style="background:linear-gradient(135deg,#0f1c3f 0%,#1d4ed8 100%);border-radius:12px;padding:28px 32px;margin-bottom:20px">
    <div style="font-size:11px;letter-spacing:.15em;text-transform:uppercase;color:#93c5fd;font-family:monospace;margin-bottom:8px">COBRANZA ANALYTICS</div>
    <div style="font-size:24px;font-weight:800;color:white;margin-bottom:4px">Reporte Diario de Cartera</div>
    <div style="font-size:13px;color:#93c5fd">{resumen['fecha']} · Sistema automatizado</div>
  </div>

  <!-- KPIs -->
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:20px">
    <div style="background:white;border-radius:10px;padding:18px;border:1px solid #e2e8f0;text-align:center">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#94a3b8;margin-bottom:8px">Documentos Activos</div>
      <div style="font-size:28px;font-weight:800;color:#0f172a;font-family:monospace">{resumen['total_docs']:,}</div>
    </div>
    <div style="background:white;border-radius:10px;padding:18px;border:1px solid #e2e8f0;text-align:center">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#94a3b8;margin-bottom:8px">Saldo Total</div>
      <div style="font-size:28px;font-weight:800;color:#dc2626;font-family:monospace">{_fmt_clp(resumen['saldo_total'])}</div>
    </div>
    <div style="background:white;border-radius:10px;padding:18px;border:1px solid #e2e8f0;text-align:center">
      <div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#94a3b8;margin-bottom:8px">Riesgo Crítico</div>
      <div style="font-size:28px;font-weight:800;color:#dc2626;font-family:monospace">{resumen['n_criticos']}</div>
      <div style="font-size:11px;color:#94a3b8">{_fmt_clp(resumen['saldo_criticos'])}</div>
    </div>
  </div>

  {seccion_cambios}

  <!-- ESTADOS + TRAMOS -->
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px">
    <div style="background:white;border-radius:10px;border:1px solid #e2e8f0;overflow:hidden">
      <div style="background:#0f1c3f;padding:12px 16px">
        <span style="color:white;font-weight:700;font-size:13px">Por Estado</span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        <tbody>{filas_estado}</tbody>
      </table>
    </div>
    <div style="background:white;border-radius:10px;border:1px solid #e2e8f0;overflow:hidden">
      <div style="background:#0f1c3f;padding:12px 16px">
        <span style="color:white;font-weight:700;font-size:13px">Por Tramo de Mora</span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        <tbody>{filas_tramo}</tbody>
      </table>
    </div>
  </div>

  <!-- EJECUTIVOS -->
  <div style="background:white;border-radius:10px;border:1px solid #e2e8f0;margin-bottom:20px;overflow:hidden">
    <div style="background:#0f1c3f;padding:14px 20px">
      <span style="color:white;font-weight:700;font-size:15px">👤 Performance Ejecutivos</span>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#f8fafc">
          <th style="padding:9px 12px;text-align:left;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase">Ejecutivo</th>
          <th style="padding:9px 12px;text-align:center;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase">Docs</th>
          <th style="padding:9px 12px;text-align:right;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase">Saldo</th>
          <th style="padding:9px 12px;text-align:center;font-size:11px;color:#94a3b8;font-weight:700;text-transform:uppercase">Risk Score</th>
        </tr>
      </thead>
      <tbody>{filas_exec}</tbody>
    </table>
  </div>

  <!-- TOP CLIENTES -->
  <div style="background:white;border-radius:10px;border:1px solid #e2e8f0;margin-bottom:20px;overflow:hidden">
    <div style="background:#dc2626;padding:14px 20px">
      <span style="color:white;font-weight:700;font-size:15px">🔴 Top 5 Clientes Mayor Exposición</span>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <tbody>{filas_top}</tbody>
    </table>
  </div>

  <!-- FOOTER -->
  <div style="text-align:center;padding:16px;font-size:11px;color:#94a3b8">
    Cobranza Analytics · Sistema automatizado de alertas · github.com/fernandorios/cobranza-analytics<br>
    Generado el {resumen['fecha']}
  </div>

</div>
</body>
</html>"""
    return html


# ══════════════════════════════════════════════════════════════════
# 5. ENVÍO DE EMAIL
# ══════════════════════════════════════════════════════════════════
def enviar_email(html: str, config: dict, dry_run: bool = False):
    """Envía el email HTML por Gmail SMTP."""
    asunto = f"📊 Cobranza Analytics — Reporte {datetime.now().strftime('%d/%m/%Y')}"

    if dry_run:
        preview_path = Path(__file__).parent / "preview_email.html"
        preview_path.write_text(html, encoding="utf-8")
        log.info(f"  🔍 DRY RUN — Email guardado en: {preview_path}")
        log.info(f"  → Abre el archivo en tu navegador para ver el preview.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = asunto
    msg["From"]    = config["gmail_user"]
    msg["To"]      = ", ".join(config["destinatarios"])
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(config["gmail_user"], config["gmail_app_password"])
            server.sendmail(
                config["gmail_user"],
                config["destinatarios"],
                msg.as_string()
            )
        log.info(f"  ✅ Email enviado a: {', '.join(config['destinatarios'])}")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Error de autenticación Gmail. Verifica gmail_app_password en config.json")
        log.error("     → Ve a: myaccount.google.com/apppasswords")
    except Exception as e:
        log.error(f"  ❌ Error al enviar: {e}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Motor de Alertas — Cobranza Analytics")
    parser.add_argument("--dry-run", action="store_true",
                        help="Genera preview HTML sin enviar email")
    args = parser.parse_args()

    log.info("═" * 50)
    log.info("  COBRANZA ANALYTICS — MOTOR DE ALERTAS")
    log.info(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    log.info("═" * 50)

    # Cargar config
    if not CONFIG_PATH.exists():
        log.error(f"No se encontró config.json en {CONFIG_PATH}")
        log.error("Copia config.example.json → config.json y completa tus datos.")
        sys.exit(1)

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    # Pipeline
    df       = cargar_datos()
    cambios  = detectar_cambios_tramo(df)
    resumen  = generar_resumen(df)
    html     = construir_email_html(resumen, cambios)

    log.info(f"  ✔ KPIs: {resumen['total_docs']} docs | Saldo {_fmt_clp(resumen['saldo_total'])} | Críticos: {resumen['n_criticos']}")
    log.info(f"  ✔ Cambios de tramo: {len(cambios)}")

    enviar_email(html, config, dry_run=args.dry_run)

    log.info("═" * 50)
    log.info("  ALERTAS FINALIZADAS ✅")
    log.info("═" * 50)


if __name__ == "__main__":
    main()

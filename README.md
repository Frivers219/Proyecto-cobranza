# 📊 Cobranza Analytics

> Pipeline de análisis de cartera de cobranza con alertas automáticas por email.
> Stack: Python · SQLite / SQL Server · Chart.js · Gmail SMTP

[![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python)](https://python.org)
[![SQLite](https://img.shields.io/badge/SQLite-3-lightgrey?logo=sqlite)](https://sqlite.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## 🗂️ Estructura del Proyecto

```
cobranza-analytics/
├── data/
│   ├── cartera_cobranza.xlsx       ← Documentos de cobranza
│   ├── gestiones_cobranza.xlsx     ← Historial de gestiones
│   ├── pagos_cobranza.xlsx         ← Pagos registrados
│   └── cobranza.db                 ← Base de datos SQLite (generada por ETL)
├── etl/
│   ├── generate_data.py            ← Generador de datos ficticios
│   └── etl_pipeline.py             ← ETL: Extract → Transform → Load
├── alerts/
│   ├── alert_engine.py             ← Motor de alertas y envío de email
│   ├── setup_tarea_windows.py      ← Configuración Task Scheduler
│   ├── config.example.json         ← Plantilla de configuración
│   └── .gitignore                  ← Protege credenciales
├── sql/
│   └── queries_analiticos.sql      ← Vistas y queries para Power BI
├── portal/
│   └── index.html                  ← Dashboard HTML interactivo
└── README.md
```

---

## 🔄 Flujo del Sistema

```
┌─────────────────────────────────────────────────────────────────┐
│                        FUENTES DE DATOS                         │
│   cartera.xlsx   gestiones.xlsx   pagos.xlsx   (o SQL Server)   │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      ETL PIPELINE                               │
│                                                                 │
│  1. EXTRACT   → Lee Excel/CSV, detecta duplicados              │
│  2. TRANSFORM → Calcula risk_score, tramos de mora, KPIs       │
│  3. LOAD      → Upsert inteligente en SQLite / SQL Server      │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BASE DE DATOS                                │
│                                                                 │
│   dim_cartera       fact_gestiones                             │
│   fact_pagos        resumen_clientes                           │
└──────────┬──────────────────────────┬──────────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────┐       ┌──────────────────────────────────────┐
│  PORTAL HTML     │       │         MOTOR DE ALERTAS             │
│                  │       │                                      │
│  Dashboard con   │       │  · Detecta cambios de tramo         │
│  4 vistas:       │       │  · Calcula KPIs diarios             │
│  · Resumen       │       │  · Genera email HTML                │
│  · Cartera       │       │  · Envía por Gmail SMTP             │
│  · Gestiones     │       │  · Evita duplicados (snapshot)      │
│  · Ejecutivos    │       └──────────────┬───────────────────────┘
└──────────────────┘                      │
                                          ▼
                             ┌────────────────────────┐
                             │   EMAIL AUTOMÁTICO     │
                             │   Todos los días 8 AM  │
                             │   (Task Scheduler)     │
                             └────────────────────────┘
```

---

## 🚀 Instalación y Configuración

### 1. Requisitos previos

```bash
# Python 3.13+
python --version

# Instalar dependencias
pip install pandas numpy openpyxl
```

### 2. Generar datos y crear la base de datos

```bash
# Generar datos ficticios (solo la primera vez)
python etl/generate_data.py

# Ejecutar ETL — crea cobranza.db en data/
python etl/etl_pipeline.py --mode sqlite

# Para SQL Server en producción:
python etl/etl_pipeline.py --mode sqlserver --server MISERVIDOR --db CobranzaDB
```

### 3. Configurar alertas por email

```bash
# Copiar plantilla de configuración
copy alerts/config.example.json alerts/config.json
```

Editar `alerts/config.json`:
```json
{
  "gmail_user": "tucorreo@gmail.com",
  "gmail_app_password": "abcd efgh ijkl mnop",
  "destinatarios": ["tucorreo@gmail.com"]
}
```

> **Cómo obtener gmail_app_password:**
> 1. Ve a [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords)
> 2. Crea una nueva contraseña de aplicación
> 3. Copia los 16 caracteres generados

### 4. Probar el sistema

```bash
# Preview del email sin enviar
python alerts/alert_engine.py --dry-run

# Envío real
python alerts/alert_engine.py
```

### 5. Automatizar (una sola vez, como Administrador)

```bash
# Ejecutar todos los días a las 8:00 AM
python alerts/setup_tarea_windows.py --hora 08:00

# Verificar que quedó configurado
python alerts/setup_tarea_windows.py --verificar

# Eliminar la tarea si es necesario
python alerts/setup_tarea_windows.py --eliminar
```

---

## 🧠 Lógica Anti-Duplicados

El ETL usa **upsert inteligente** — si un documento ya existe en la DB, lo actualiza en vez de duplicarlo:

```
Primera ejecución:   600 docs → INSERT 600 nuevos
Segunda ejecución:   600 docs → UPDATE 580 existentes + INSERT 20 nuevos
```

El motor de alertas usa un **snapshot diario** para detectar solo cambios reales:

```
Lunes:   Cliente A en tramo "31-60 días"  → sin alerta (estado base)
Martes:  Cliente A en tramo "61-90 días"  → ALERTA: subió de tramo
```

---

## 📊 KPIs del Dashboard

| KPI | Descripción |
|-----|-------------|
| **Risk Score** | 0–100 calculado por días de mora, monto y estado |
| **Tramo de Mora** | 0-30 / 31-60 / 61-90 / 91-180 / +180 días |
| **% Recuperado** | Pagos recibidos / Monto original |
| **Efectividad Gestión** | Gestiones con resultado positivo / Total |

---

## 🔧 Variables de Entorno (Producción)

Para producción se recomienda usar variables de entorno en vez de `config.json`:

```bash
# Windows
set GMAIL_USER=tucorreo@gmail.com
set GMAIL_APP_PASSWORD=abcdefghijklmnop

# Linux/Mac
export GMAIL_USER=tucorreo@gmail.com
export GMAIL_APP_PASSWORD=abcdefghijklmnop
```

---

## 📫 Autor

**Fernando Ríos Figueroa** · Ingeniero Civil Industrial
- 💼 [LinkedIn](https://linkedin.com/in/fernandorios)
- 🐙 [GitHub](https://github.com/fernandorios)
- 📍 Santiago, Chile

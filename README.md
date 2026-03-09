# 📊 Cobranza Analytics

> **Sistema ETL + Portal de Reportes para Gestión de Cobranza Empresarial**
> Stack: Python · SQL Server · HTML/JS · Power BI ready

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://python.org)
[![SQL Server](https://img.shields.io/badge/SQL_Server-2019+-CC2927?logo=microsoftsqlserver&logoColor=white)](https://microsoft.com/sql-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🎯 Descripción

Pipeline completo de análisis de cartera de cobranza: desde la ingesta de datos en Excel/CSV hasta un portal HTML interactivo de reportes y consultas analíticas listas para conectar a Power BI.

Diseñado para equipos de cobranza que necesitan **visibilidad en tiempo real** sobre mora, riesgo, performance de ejecutivos y efectividad de gestiones.

---

## 🏗️ Arquitectura

```
data/
  cartera_cobranza.xlsx       ← Fuente: exportación sistema legacy
  gestiones_cobranza.xlsx     ← Fuente: CRM de cobranza
  pagos_cobranza.xlsx         ← Fuente: sistema de pagos
        │
        ▼
etl/
  generate_data.py            ← Generador de datos ficticios realistas
  etl_pipeline.py             ← ETL: Extract → Transform → Load
        │
        ▼
data/cobranza.db              ← SQLite (demo) / SQL Server (producción)
        │
   ┌────┴────┐
   ▼         ▼
portal/    sql/
index.html queries_analiticos.sql
(Self-contained HTML)   (Vistas + queries para Power BI)
```

---

## 📦 Tablas generadas

| Tabla | Descripción | Filas aprox. |
|-------|-------------|-------------|
| `dim_cartera` | Documentos de cobranza con estado, mora y riesgo | 600 |
| `fact_gestiones` | Historial de contactos y acciones por ejecutivo | 2.460 |
| `fact_pagos` | Pagos y cuotas registradas | 790 |
| `resumen_clientes` | Agregado de exposición y recuperación por cliente | 298 |

---

## 🚀 Instalación y uso

### 1. Clonar e instalar dependencias

```bash
git clone https://github.com/fernandorios/cobranza-analytics
cd cobranza-analytics
pip install -r requirements.txt
```

### 2. Generar datos ficticios

```bash
python etl/generate_data.py
```

### 3. Ejecutar ETL

**Modo demo (SQLite — no requiere SQL Server):**
```bash
python etl/etl_pipeline.py --mode sqlite
```

**Modo producción (SQL Server):**
```bash
python etl/etl_pipeline.py \
  --mode sqlserver \
  --server MISERVIDOR\SQLEXPRESS \
  --db CobranzaDB \
  --trusted
```

### 4. Abrir portal de reportes

```bash
# Simplemente abre en el navegador:
open portal/index.html
```

### 5. Conectar Power BI

1. Abrir Power BI Desktop
2. `Obtener datos` → `SQL Server` → ingresar servidor y base de datos
3. Seleccionar tablas: `dim_cartera`, `fact_pagos`, `resumen_clientes`
4. Los queries en `sql/queries_analiticos.sql` incluyen vistas listas para importar

---

## 📊 Vistas del Portal

| Vista | Descripción |
|-------|-------------|
| **Resumen Ejecutivo** | KPIs globales, tendencia de recaudación, distribución por estado |
| **Análisis de Cartera** | Top clientes, tramos de mora, concentración regional |
| **Gestiones** | Efectividad por canal, resultados de contacto |
| **Ejecutivos** | Performance individual, cartera asignada, risk score |

---

## 🔍 Métricas calculadas

- **Risk Score (0–100):** Índice compuesto basado en días de mora, segmento y estado
- **Tasa de Recuperación:** `monto_pagado / monto_original * 100`
- **Tasa de Contacto Exitoso:** Gestiones con resultado positivo / total gestiones
- **Clasificación de Riesgo:** CRÍTICO / ALTO / MEDIO / BAJO por cliente

---

## 🛠️ Stack Tecnológico

| Capa | Tecnología |
|------|-----------|
| Generación datos | Python, Faker, NumPy, Pandas |
| ETL | Python, Pandas, SQLAlchemy, pyodbc |
| Base de datos | SQL Server 2019 / SQLite (demo) |
| Portal reportes | HTML5, CSS3, Chart.js, JavaScript vanilla |
| BI | Power BI Desktop (queries incluidos) |
| IA Asistida | GitHub Copilot, Claude AI |

---

## 📁 Estructura del repositorio

```
cobranza-analytics/
├── data/
│   ├── cartera_cobranza.xlsx
│   ├── gestiones_cobranza.xlsx
│   ├── pagos_cobranza.xlsx
│   └── cobranza.db              (generado por ETL)
├── etl/
│   ├── generate_data.py
│   └── etl_pipeline.py
├── portal/
│   └── index.html
├── sql/
│   └── queries_analiticos.sql
├── docs/
│   └── diagrama_er.png          (pendiente)
├── requirements.txt
└── README.md
```

---

## 📋 requirements.txt

```
pandas>=2.0
numpy>=1.24
openpyxl>=3.1
sqlalchemy>=2.0
pyodbc>=4.0          # solo para SQL Server
```

---

## 👤 Autor

**Fernando Ríos Figueroa** — Ingeniero Civil Industrial
- Jefe de Proyectos TI | Analista de Datos
- [LinkedIn](https://linkedin.com/in/fernandorios) · [GitHub](https://github.com/fernandorios)

---

## 📄 Licencia

MIT — libre para uso educativo y portafolio profesional.

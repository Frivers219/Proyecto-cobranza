-- ═══════════════════════════════════════════════════════════════
-- QUERIES ANALÍTICOS — Cobranza Analytics
-- Compatibles: SQL Server 2019+ / Azure SQL / SQLite (modo demo)
-- Autor: Fernando Ríos — github.com/fernandorios
-- ═══════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────
-- 1. KPIs GLOBALES (vista para Power BI)
-- ─────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS vw_kpis_globales AS
SELECT
    COUNT(*)                                            AS total_documentos,
    SUM(monto_original)                                 AS monto_total,
    SUM(saldo_deudor)                                   AS saldo_total,
    SUM(monto_pagado)                                   AS total_recaudado,
    ROUND(SUM(monto_pagado) * 100.0 / SUM(monto_original), 2) AS pct_recuperacion,
    ROUND(AVG(dias_mora), 1)                            AS mora_promedio_dias,
    ROUND(AVG(risk_score), 1)                           AS risk_score_promedio,
    COUNT(CASE WHEN estado = 'Castigada'        THEN 1 END) AS docs_castigados,
    COUNT(CASE WHEN estado = 'En Gestión'       THEN 1 END) AS docs_en_gestion,
    COUNT(CASE WHEN estado = 'Acuerdo de Pago'  THEN 1 END) AS docs_acuerdo,
    COUNT(CASE WHEN estado = 'Vigente'          THEN 1 END) AS docs_vigentes
FROM dim_cartera;


-- ─────────────────────────────────────────────
-- 2. RESUMEN POR TRAMO DE MORA
-- ─────────────────────────────────────────────
SELECT
    tramo_mora,
    COUNT(*)                        AS documentos,
    SUM(saldo_deudor)               AS saldo_total,
    ROUND(AVG(risk_score), 1)       AS risk_promedio,
    ROUND(SUM(saldo_deudor) * 100.0 /
          (SELECT SUM(saldo_deudor) FROM dim_cartera), 2) AS pct_del_total
FROM dim_cartera
GROUP BY tramo_mora
ORDER BY
    CASE tramo_mora
        WHEN '0-30 días'    THEN 1
        WHEN '31-60 días'   THEN 2
        WHEN '61-90 días'   THEN 3
        WHEN '91-180 días'  THEN 4
        WHEN '>180 días'    THEN 5
    END;


-- ─────────────────────────────────────────────
-- 3. PERFORMANCE POR EJECUTIVO (con benchmarks)
-- ─────────────────────────────────────────────
WITH totales AS (
    SELECT
        ejecutivo,
        COUNT(*)                            AS total_docs,
        SUM(monto_original)                 AS cartera_asignada,
        SUM(saldo_deudor)                   AS saldo_vigente,
        SUM(monto_pagado)                   AS recaudado,
        ROUND(AVG(dias_mora), 1)            AS mora_promedio,
        ROUND(AVG(risk_score), 1)           AS risk_promedio
    FROM dim_cartera
    GROUP BY ejecutivo
),
gestiones_exec AS (
    SELECT
        ejecutivo,
        COUNT(*)                            AS total_gestiones,
        SUM(es_contacto_exitoso)            AS contactos_exitosos,
        SUM(es_promesa)                     AS promesas
    FROM fact_gestiones
    GROUP BY ejecutivo
)
SELECT
    t.ejecutivo,
    t.total_docs,
    t.cartera_asignada,
    t.saldo_vigente,
    t.recaudado,
    ROUND(t.recaudado * 100.0 / NULLIF(t.cartera_asignada, 0), 2) AS pct_recuperacion,
    t.mora_promedio,
    t.risk_promedio,
    g.total_gestiones,
    g.contactos_exitosos,
    g.promesas,
    ROUND(g.contactos_exitosos * 100.0 / NULLIF(g.total_gestiones, 0), 2) AS tasa_contacto_pct
FROM totales t
LEFT JOIN gestiones_exec g ON t.ejecutivo = g.ejecutivo
ORDER BY t.saldo_vigente DESC;


-- ─────────────────────────────────────────────
-- 4. TENDENCIA MENSUAL DE RECAUDACIÓN
-- ─────────────────────────────────────────────
SELECT
    mes_pago                        AS periodo,
    COUNT(*)                        AS num_pagos,
    SUM(monto)                      AS monto_recaudado,
    ROUND(AVG(monto), 0)            AS ticket_promedio,
    COUNT(DISTINCT cliente_id)      AS clientes_distintos
FROM fact_pagos
WHERE mes_pago NOT IN ('NaT', 'None', '')
GROUP BY mes_pago
ORDER BY mes_pago;


-- ─────────────────────────────────────────────
-- 5. ANÁLISIS DE RIESGO POR CLIENTE
--    (para scoring y priorización)
-- ─────────────────────────────────────────────
SELECT
    c.cliente_id,
    c.cliente_nombre,
    c.industria,
    c.segmento,
    c.region,
    c.ejecutivo,
    COUNT(d.folio)                  AS total_documentos,
    SUM(d.monto_original)           AS exposicion_total,
    SUM(d.saldo_deudor)             AS saldo_total,
    ROUND(AVG(d.dias_mora), 0)      AS mora_promedio,
    ROUND(AVG(d.risk_score), 1)     AS risk_score,
    ROUND(SUM(d.monto_pagado) * 100.0 / NULLIF(SUM(d.monto_original),0), 2) AS pct_recuperado,
    COUNT(CASE WHEN d.estado = 'Castigada' THEN 1 END) AS docs_castigados,
    -- Clasificación de riesgo
    CASE
        WHEN AVG(d.risk_score) >= 75 THEN 'CRÍTICO'
        WHEN AVG(d.risk_score) >= 50 THEN 'ALTO'
        WHEN AVG(d.risk_score) >= 25 THEN 'MEDIO'
        ELSE 'BAJO'
    END AS clasificacion_riesgo
FROM resumen_clientes c
JOIN dim_cartera d ON c.cliente_id = d.cliente_id
GROUP BY c.cliente_id, c.cliente_nombre, c.industria, c.segmento, c.region, c.ejecutivo
ORDER BY saldo_total DESC;


-- ─────────────────────────────────────────────
-- 6. EFECTIVIDAD DE GESTIONES
-- ─────────────────────────────────────────────
SELECT
    tipo                            AS canal,
    resultado,
    COUNT(*)                        AS total_gestiones,
    SUM(es_contacto_exitoso)        AS exitosas,
    SUM(es_promesa)                 AS promesas,
    ROUND(SUM(es_contacto_exitoso) * 100.0 / COUNT(*), 2) AS tasa_exito_pct
FROM fact_gestiones
GROUP BY tipo, resultado
ORDER BY tipo, total_gestiones DESC;


-- ─────────────────────────────────────────────
-- 7. DOCUMENTOS DE ALTO RIESGO (alarmas)
--    Para dashboard operativo diario
-- ─────────────────────────────────────────────
SELECT
    folio,
    cliente_nombre,
    ejecutivo,
    fecha_vencimiento,
    dias_mora,
    tramo_mora,
    saldo_deudor,
    estado,
    risk_score,
    CASE
        WHEN risk_score >= 75 AND dias_mora > 180 THEN '🔴 URGENTE — Evaluar castigo'
        WHEN risk_score >= 50 AND dias_mora > 90  THEN '🟠 PRIORITARIO — Gestionar esta semana'
        WHEN risk_score >= 25                     THEN '🟡 SEGUIMIENTO — Monitorear'
        ELSE                                           '🟢 NORMAL'
    END AS accion_recomendada
FROM dim_cartera
WHERE estado NOT IN ('Vigente', 'Pagada')
  AND saldo_deudor > 0
ORDER BY risk_score DESC, saldo_deudor DESC
LIMIT 50;

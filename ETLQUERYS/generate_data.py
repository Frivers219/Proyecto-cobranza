"""
generate_data.py
Genera datos ficticios realistas para el sistema de Cobranza.
Produce archivos Excel/CSV que simulan exportaciones de un sistema legacy.
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

# ── Catálogos ──────────────────────────────────────────────────────────────
CLIENTES = [
    ("C001","Constructora Los Andes Ltda.","Construcción","Grande"),
    ("C002","Supermercados Del Sur S.A.","Retail","Grande"),
    ("C003","Clínica Santa María SpA","Salud","Mediana"),
    ("C004","Transportes Rápido Ltda.","Logística","Pequeña"),
    ("C005","Inmobiliaria Horizonte S.A.","Inmobiliaria","Mediana"),
    ("C006","Farmacia Cruz Verde S.A.","Retail","Grande"),
    ("C007","Restaurante El Huerto SpA","Gastronomía","Pequeña"),
    ("C008","Corporación Educacional Norte","Educación","Mediana"),
    ("C009","Minera Altiplano S.A.","Minería","Grande"),
    ("C010","Tecnologías del Pacífico Ltda.","TI","Pequeña"),
    ("C011","Agrícola Valle Verde SpA","Agricultura","Mediana"),
    ("C012","Ferretería Industrial Ltda.","Comercio","Pequeña"),
    ("C013","Seguros Continental S.A.","Seguros","Grande"),
    ("C014","Hotel Costanera SpA","Turismo","Mediana"),
    ("C015","Servicios Generales del Sur","Servicios","Pequeña"),
]

EJECUTIVOS = ["María González","Carlos Rojas","Paola Muñoz","Andrés Torres","Valentina Soto"]

ESTADOS = ["Vigente","Vencida","En Gestión","Acuerdo de Pago","Castigada","Pagada"]
TRAMOS = ["0-30 días","31-60 días","61-90 días","91-180 días",">180 días"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end-start).days))

def generar_cartera():
    rows = []
    base = datetime(2023, 1, 1)
    hoy  = datetime(2025, 3, 1)
    folio = 10000

    for _ in range(600):
        cliente = random.choice(CLIENTES)
        emision = random_date(base, hoy - timedelta(days=10))
        dias_plazo = random.choice([30, 45, 60, 90])
        vencimiento = emision + timedelta(days=dias_plazo)
        dias_mora = max(0, (hoy - vencimiento).days)

        monto_base = {
            "Grande":  np.random.uniform(5_000_000, 80_000_000),
            "Mediana": np.random.uniform(1_000_000, 15_000_000),
            "Pequeña": np.random.uniform(100_000,  3_000_000),
        }[cliente[3]]

        monto = round(monto_base, -3)

        if dias_mora == 0:
            estado = "Vigente"
        elif dias_mora <= 30:
            estado = random.choices(["Vencida","En Gestión"], weights=[0.6,0.4])[0]
        elif dias_mora <= 90:
            estado = random.choices(["En Gestión","Acuerdo de Pago","Vencida"], weights=[0.4,0.35,0.25])[0]
        elif dias_mora <= 180:
            estado = random.choices(["Acuerdo de Pago","Castigada","En Gestión"], weights=[0.4,0.3,0.3])[0]
        else:
            estado = random.choices(["Castigada","Acuerdo de Pago"], weights=[0.65,0.35])[0]

        if estado == "Pagada":
            monto_pagado = monto
        elif estado == "Acuerdo de Pago":
            monto_pagado = round(monto * np.random.uniform(0.1, 0.5), -3)
        elif estado in ["Castigada"]:
            monto_pagado = round(monto * np.random.uniform(0, 0.1), -3)
        else:
            monto_pagado = 0

        saldo = monto - monto_pagado

        if dias_mora == 0:
            tramo = "0-30 días"
        elif dias_mora <= 30:
            tramo = "0-30 días"
        elif dias_mora <= 60:
            tramo = "31-60 días"
        elif dias_mora <= 90:
            tramo = "61-90 días"
        elif dias_mora <= 180:
            tramo = "91-180 días"
        else:
            tramo = ">180 días"

        folio += 1
        rows.append({
            "folio":          folio,
            "cliente_id":     cliente[0],
            "cliente_nombre": cliente[1],
            "industria":      cliente[2],
            "segmento":       cliente[3],
            "ejecutivo":      random.choice(EJECUTIVOS),
            "fecha_emision":  emision.strftime("%Y-%m-%d"),
            "fecha_vencimiento": vencimiento.strftime("%Y-%m-%d"),
            "dias_mora":      dias_mora,
            "tramo_mora":     tramo,
            "monto_original": monto,
            "monto_pagado":   monto_pagado,
            "saldo_deudor":   saldo,
            "estado":         estado,
            "region":         random.choice(["Metropolitana","Valparaíso","Biobío","Antofagasta","Araucanía"]),
        })

    return pd.DataFrame(rows)

def generar_gestiones(cartera):
    rows = []
    tipos = ["Llamada","Email","Carta","Visita","WhatsApp"]
    resultados = ["Contactado","No contesta","Promesa de pago","Acuerdo firmado","Sin respuesta","Número equivocado"]
    gestion_id = 1

    for _, doc in cartera[cartera["estado"].isin(["En Gestión","Acuerdo de Pago","Castigada"])].iterrows():
        n_gestiones = random.randint(1, 8)
        fecha_base = datetime.strptime(doc["fecha_vencimiento"], "%Y-%m-%d")
        for i in range(n_gestiones):
            fecha_g = fecha_base + timedelta(days=random.randint(1, doc["dias_mora"] or 1))
            rows.append({
                "gestion_id":  gestion_id,
                "folio":       doc["folio"],
                "cliente_id":  doc["cliente_id"],
                "ejecutivo":   doc["ejecutivo"],
                "fecha":       fecha_g.strftime("%Y-%m-%d"),
                "tipo":        random.choice(tipos),
                "resultado":   random.choice(resultados),
                "observacion": random.choice([
                    "Cliente indica que pagará fin de mes",
                    "No se pudo contactar",
                    "Solicita plazo adicional de 30 días",
                    "Disputa factura, enviada a revisión",
                    "Acuerdo de cuotas establecido",
                    "Cliente informó quiebra inminente",
                    "Promete pago al 15 del mes",
                    ""
                ])
            })
            gestion_id += 1

    return pd.DataFrame(rows)

def generar_pagos(cartera):
    rows = []
    pago_id = 1
    metodos = ["Transferencia","Cheque","Depósito","PAC"]

    for _, doc in cartera[cartera["monto_pagado"] > 0].iterrows():
        if doc["estado"] == "Acuerdo de Pago":
            n_pagos = random.randint(1, 4)
            saldo_restante = doc["monto_pagado"]
            fecha_base = datetime.strptime(doc["fecha_vencimiento"], "%Y-%m-%d")
            for i in range(n_pagos):
                if saldo_restante <= 0:
                    break
                monto_cuota = round(min(saldo_restante, saldo_restante / (n_pagos - i) * np.random.uniform(0.8, 1.2)), -3)
                fecha_p = fecha_base + timedelta(days=30 * (i + 1) + random.randint(-5, 5))
                rows.append({
                    "pago_id":    pago_id,
                    "folio":      doc["folio"],
                    "cliente_id": doc["cliente_id"],
                    "fecha_pago": fecha_p.strftime("%Y-%m-%d"),
                    "monto":      monto_cuota,
                    "metodo":     random.choice(metodos),
                })
                saldo_restante -= monto_cuota
                pago_id += 1
        else:
            fecha_p = datetime.strptime(doc["fecha_vencimiento"], "%Y-%m-%d") + timedelta(days=random.randint(0, 10))
            rows.append({
                "pago_id":    pago_id,
                "folio":      doc["folio"],
                "cliente_id": doc["cliente_id"],
                "fecha_pago": fecha_p.strftime("%Y-%m-%d"),
                "monto":      doc["monto_pagado"],
                "metodo":     random.choice(metodos),
            })
            pago_id += 1

    return pd.DataFrame(rows)

if __name__ == "__main__":
    out = os.path.dirname(__file__)

    print("Generando cartera...")
    cartera = generar_cartera()
    cartera.to_excel(f"{out}/../data/cartera_cobranza.xlsx", index=False)
    cartera.to_csv(f"{out}/../data/cartera_cobranza.csv", index=False)
    print(f"  → {len(cartera)} documentos generados")

    print("Generando gestiones...")
    gestiones = generar_gestiones(cartera)
    gestiones.to_excel(f"{out}/../data/gestiones_cobranza.xlsx", index=False)
    gestiones.to_csv(f"{out}/../data/gestiones_cobranza.csv", index=False)
    print(f"  → {len(gestiones)} gestiones generadas")

    print("Generando pagos...")
    pagos = generar_pagos(cartera)
    pagos.to_excel(f"{out}/../data/pagos_cobranza.xlsx", index=False)
    pagos.to_csv(f"{out}/../data/pagos_cobranza.csv", index=False)
    print(f"  → {len(pagos)} pagos generados")

    print("\n✅ Datos generados exitosamente en /data/")

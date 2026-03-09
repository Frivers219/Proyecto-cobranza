"""
setup_tarea_windows.py
────────────────────────────────────────────────────────────────────
Crea automáticamente la tarea programada en Windows Task Scheduler
para ejecutar el motor de alertas cada día a la hora que elijas.

Uso (ejecutar como Administrador):
    python setup_tarea_windows.py
    python setup_tarea_windows.py --hora 08:00
    python setup_tarea_windows.py --hora 07:30 --eliminar

Autor: Fernando Ríos — github.com/fernandorios
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

NOMBRE_TAREA = "CobranzaAnalytics_Alertas"


def obtener_rutas():
    """Detecta Python y el script automáticamente."""
    python_exe = sys.executable
    script_dir = Path(__file__).parent
    engine_path = script_dir / "alert_engine.py"
    return python_exe, engine_path, script_dir


def crear_tarea(hora: str):
    python_exe, engine_path, script_dir = obtener_rutas()

    if not engine_path.exists():
        print(f"❌ No se encontró alert_engine.py en {engine_path}")
        sys.exit(1)

    # Comando que ejecutará la tarea
    comando = f'"{python_exe}" "{engine_path}"'

    # XML de configuración de la tarea
    xml_tarea = f"""<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Cobranza Analytics — Reporte diario automatizado de cartera y alertas por email.</Description>
    <Author>Fernando Rios</Author>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2025-01-01T{hora}:00</StartBoundary>
      <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{python_exe}</Command>
      <Arguments>"{engine_path}"</Arguments>
      <WorkingDirectory>{script_dir}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>"""

    # Guardar XML temporal
    xml_path = script_dir / "tarea_temp.xml"
    xml_path.write_text(xml_tarea, encoding="utf-16")

    try:
        # Eliminar si ya existe
        subprocess.run(
            ["schtasks", "/delete", "/tn", NOMBRE_TAREA, "/f"],
            capture_output=True
        )

        # Crear tarea desde XML
        result = subprocess.run(
            ["schtasks", "/create", "/tn", NOMBRE_TAREA, "/xml", str(xml_path)],
            capture_output=True, text=True
        )

        if result.returncode == 0:
            print(f"\n✅ Tarea '{NOMBRE_TAREA}' creada exitosamente.")
            print(f"   → Se ejecutará todos los días a las {hora}")
            print(f"   → Script: {engine_path}")
            print(f"   → Python:  {python_exe}")
            print(f"\n💡 Para verificar: Abre 'Programador de tareas' en Windows")
            print(f"   y busca '{NOMBRE_TAREA}' en la biblioteca.")
        else:
            print(f"❌ Error al crear la tarea:")
            print(result.stderr)
            print("\n💡 Intenta ejecutar este script como Administrador.")

    finally:
        # Limpiar XML temporal
        if xml_path.exists():
            xml_path.unlink()


def eliminar_tarea():
    result = subprocess.run(
        ["schtasks", "/delete", "/tn", NOMBRE_TAREA, "/f"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"✅ Tarea '{NOMBRE_TAREA}' eliminada.")
    else:
        print(f"⚠️  No se encontró la tarea o no se pudo eliminar.")


def verificar_tarea():
    result = subprocess.run(
        ["schtasks", "/query", "/tn", NOMBRE_TAREA, "/fo", "LIST"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(result.stdout)
    else:
        print(f"⚠️  La tarea '{NOMBRE_TAREA}' no está registrada.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Configura Task Scheduler para Cobranza Analytics")
    parser.add_argument("--hora",     default="08:00", help="Hora de ejecución diaria (HH:MM). Default: 08:00")
    parser.add_argument("--eliminar", action="store_true", help="Elimina la tarea programada")
    parser.add_argument("--verificar",action="store_true", help="Muestra el estado de la tarea")
    args = parser.parse_args()

    if args.eliminar:
        eliminar_tarea()
    elif args.verificar:
        verificar_tarea()
    else:
        print("═" * 50)
        print("  COBRANZA ANALYTICS — TASK SCHEDULER")
        print("═" * 50)
        crear_tarea(args.hora)

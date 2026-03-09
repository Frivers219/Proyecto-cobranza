import sqlite3
ruta = "C:\Users\fernando.rios\Downloads\Cobranza\Alertas\cobranza.db"
conn = sqlite3.connect(ruta)
tablas = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("Tablas encontradas:", tablas)
print("Total tablas:", len(tablas))
conn.close()
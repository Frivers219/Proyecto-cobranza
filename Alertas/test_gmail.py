print("Iniciando test...")
import smtplib
import json

print("Leyendo config...")
with open('config.json', encoding='utf-8') as f:
    config = json.load(f)

print("Usuario:", config['gmail_user'])
print("Password length:", len(config['gmail_app_password']))
print("Intentando conectar...")

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(config['gmail_user'], config['gmail_app_password'])
        print("CONEXION EXITOSA")
except smtplib.SMTPAuthenticationError as e:
    print("Error autenticacion:", e)
except Exception as e:
    print("Otro error:", e)
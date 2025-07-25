
import streamlit as st
import openai
import mysql.connector
import pandas as pd
import csv

# Configura la clave de OpenAI
openai.api_key = st.secrets["OPENAI_API_KEY"]

# Datos de conexión a MySQL
db_config = {
    "host": "localhost",
    "user": "domolabs_admin",
    "password": "Pa$$w0rd_123",
    "database": "domolabs_Chatbot_SQL_DB",
    "port": 3306
}

# UI
st.image("assets/logo_neurovia.png", width=180)
st.title("🤖 Asistente Inteligente de NeurovIA")
st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

query = st.text_input("🧠 Pregunta en lenguaje natural")

# Obtener esquema de la base
def obtener_esquema():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tablas = cursor.fetchall()
        esquema = ""
        for (tabla,) in tablas:
            cursor.execute(f"DESCRIBE {tabla};")
            columnas = cursor.fetchall()
            esquema += f"Tabla: {tabla}\n"
            for col in columnas:
                esquema += f"- {col[0]} ({col[1]})\n"
        cursor.close()
        conn.close()
        return esquema
    except Exception as e:
        return f"Error al obtener esquema: {e}"

# Convertir pregunta a SQL
def generar_sql(pregunta, schema_info):
    prompt = f"""Eres un experto en SQL. Con el siguiente esquema:

{schema_info}

Convierte esta pregunta del usuario en SQL válida:
Pregunta: "{pregunta}"
SQL:
"""
    respuesta = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return respuesta.choices[0].message.content.strip()

# Ejecutar SQL
def ejecutar_sql(sql):
    try:
        conn = mysql.connector.connect(**db_config)
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception as e:
        return f"❌ Error al ejecutar la consulta: {e}"

# Log
def log(query, sql):
    with open("chat_logs.csv", "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([query, sql])

# Procesar consulta
if query:
    with st.spinner("Generando consulta SQL..."):
        esquema = obtener_esquema()
        sql = generar_sql(query, esquema)
    st.code(sql, language="sql")
    resultado = ejecutar_sql(sql)
    if isinstance(resultado, pd.DataFrame):
        st.dataframe(resultado)
        log(query, sql)
    else:
        st.error(resultado)

# Descargar logs
if Path("chat_logs.csv").exists():
    with open("chat_logs.csv", "r", encoding="utf-8") as f:
        st.download_button("⬇️ Descargar logs", f, "chat_logs.csv", "text/csv")

    
import os
import openai
import streamlit as st
import mysql.connector
import pandas as pd
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from pathlib import Path
import csv

# CONFIGURACIÓN INICIAL
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title("🧠 Asistente Inteligente de NeuroVIA")
st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

# API OPENAI
openai.api_key = st.secrets["OPENAI_API_KEY"]
llm = ChatOpenAI(temperature=0)

# FUNCIÓN PARA CONECTAR A MySQL
def connect_db():
    return mysql.connector.connect(
        host="s1355.use1.mysecurecloudhost.com",
        port=3306,
        user="domolabs_admin",
        password="Pa$$w0rd_123",
        database="domolabs_Chatbot_SQL_DB"
    )

# PROMPT PERSONALIZADO
sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template="""
    Eres un asistente experto en SQL para una base de datos MySQL.
    Devuelve únicamente el código SQL sin explicaciones.

    Pregunta: {pregunta}
    SQL:
    """
)

# FUNCIONES DE LOG
if not Path("chat_logs.csv").exists():
    with open("chat_logs.csv", "w", encoding="utf-8") as f:
        f.write("Pregunta,SQL,Resultado\n")

def log_interaction(pregunta, sql, resultado):
    with open("chat_logs.csv", "a", newline="", encoding="utf-8") as logfile:
        writer = csv.writer(logfile)
        writer.writerow([pregunta, sql, resultado])

# ENTRADA DE USUARIO
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    # GENERAR SQL
    prompt = sql_prompt.format(pregunta=pregunta)
    sql_query = llm.predict(prompt).strip().strip("```sql").strip("```")

    st.markdown("🔍 **Consulta SQL Generada:**")
    st.code(sql_query, language="sql")

    # CONECTAR Y EJECUTAR
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # SI LA CONSULTA ES SELECT, MOSTRAR RESULTADOS
        if sql_query.lower().startswith("select"):
            columns = [col[0] for col in cursor.description]
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=columns)
            st.dataframe(df)
            resultado_str = f"{len(df)} filas"
        else:
            conn.commit()
            resultado_str = f"Consulta ejecutada correctamente."

        cursor.close()
        conn.close()
        log_interaction(pregunta, sql_query, resultado_str)

    except Exception as e:
        st.error(f"❌ Error al ejecutar la consulta: {e}")
        log_interaction(pregunta, sql_query, f"Error: {e}")

# BOTÓN DESCARGAR LOGS
if Path("chat_logs.csv").exists():
    with open("chat_logs.csv", "r", encoding="utf-8") as f:
        st.download_button(
            label="📥 Descargar logs",
            data=f,
            file_name="chat_logs.csv",
            mime="text/csv"
        )

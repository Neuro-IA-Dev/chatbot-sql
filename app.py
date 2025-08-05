import os
import json
import numpy as np
import datetime
import requests
import streamlit as st
import mysql.connector
import pandas as pd
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from openai import OpenAI
from pathlib import Path
import csv

# CONFIGURACIÓN INICIAL
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title("🧠 Asistente Inteligente de Intanis/NeuroVIA")

if st.button("🪝 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

if "historial" not in st.session_state:
    st.session_state["historial"] = []

if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []

for entrada in st.session_state["conversacion"]:
    st.markdown(f"**🧠 Pregunta:** {entrada['pregunta']}")
    st.markdown(f"**💬 Respuesta:** {entrada['respuesta']}")
    st.markdown("---")

llm = ChatOpenAI(temperature=0)

# CONEXIÓN A MySQL
def connect_db():
    return mysql.connector.connect(
        host="s1355.use1.mysecurecloudhost.com",
        port=3306,
        user="domolabs_RedTabBot_USER",
        password="Pa$$w0rd_123",
        database="domolabs_RedTabBot_DB"
    )

def es_consulta_segura(sql):
    sql = sql.lower()
    comandos_peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(comando in sql for comando in comandos_peligrosos)

# PROMPT
# (...se mantiene igual para el PromptTemplate y esquema)

# LOG DE INTERACCIONES EN BASE DE DATOS
def log_interaction(pregunta, sql, resultado):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO chat_logs (pregunta, sql_generado, resultado)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (pregunta, sql, resultado))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar el log en la base de datos: {e}")

# SEMANTIC CACHE
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

def obtener_embedding(texto):
    if not texto or not texto.strip():
        st.warning("❌ El texto para obtener embedding está vacío.")
        return None
    try:
        response = client.embeddings.create(input=[texto], model="text-embedding-3-small")
        return response.data[0].embedding
    except Exception as e:
        st.warning(f"Error al obtener embedding: {e}")
        return None

def guardar_en_cache(pregunta, sql_generado, embedding):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = "INSERT INTO semantic_cache (pregunta, embedding, sql_generado) VALUES (%s, %s, %s)"
        cursor.execute(query, (pregunta, json.dumps(embedding), sql_generado))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"No se pudo guardar en el semantic cache: {e}")

def buscar_sql_en_cache(pregunta_nueva, umbral_similitud=0.90):
    try:
        embedding_nuevo = obtener_embedding(pregunta_nueva)
        if embedding_nuevo is None:
            return None
        embedding_nuevo = np.array(embedding_nuevo)
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT pregunta, embedding, sql_generado FROM semantic_cache")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        for row in rows:
            try:
                embedding_guardado = json.loads(row["embedding"])
                if embedding_guardado is None:
                    continue
                embedding_guardado = np.array(embedding_guardado)
                similitud = np.dot(embedding_nuevo, embedding_guardado) / (
                    np.linalg.norm(embedding_nuevo) * np.linalg.norm(embedding_guardado)
                )
                if similitud >= umbral_similitud:
                    return row["sql_generado"]
            except Exception as e:
                st.warning(f"Error comparando embeddings: {e}")
        return None
    except Exception as e:
        st.warning(f"Error al buscar en el semantic cache: {e}")
        return None

# ENTRADA PRINCIPAL
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    st.markdown(f"**📝 Pregunta:** {pregunta}")
    sql_query = buscar_sql_en_cache(pregunta)

    if sql_query:
        st.info("🔁 Se reutilizó una consulta SQL previamente generada por similitud semántica.")
    else:
        prompt = sql_prompt.format_prompt(pregunta=pregunta).to_string()
        sql_query = llm.predict(prompt).strip().strip("```sql").strip("```")
        embedding = obtener_embedding(pregunta)
        if embedding:
            guardar_en_cache(pregunta, sql_query, embedding)

    st.session_state["historial"].append((pregunta, sql_query))
    st.markdown("🔍 **Consulta SQL Generada:**")
    st.code(sql_query, language="sql")

    try:
        if not es_consulta_segura(sql_query):
            st.error("❌ La consulta generada contiene comandos peligrosos y no será ejecutada.")
            log_interaction(pregunta, sql_query, "Consulta bloqueada por seguridad")
        else:
            conn = connect_db()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            if sql_query.lower().startswith("select"):
                columns = [col[0] for col in cursor.description]
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=columns)
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]) or "fecha" in col.lower() or "dia" in col.lower():
                        try:
                            df[col] = pd.to_datetime(df[col])
                            if len(df) == 1:
                                df[col] = df[col].dt.strftime('%A %d de %B de %Y')
                            else:
                                df[col] = df[col].dt.strftime('%d/%m/%Y')
                        except Exception:
                            pass
                st.dataframe(df)
                resultado_str = f"{len(df)} filas"
            else:
                conn.commit()
                resultado_str = "Consulta ejecutada correctamente."
            cursor.close()
            conn.close()
            st.markdown(f"**💬 Respuesta:** {resultado_str}")
            log_interaction(pregunta, sql_query, resultado_str)
            st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": resultado_str})
    except Exception as e:
        st.error(f"❌ Error al ejecutar la consulta: {e}")
        log_interaction(pregunta, sql_query, f"Error: {e}")
        st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": str(e)})

# 🔄 VER HISTORIAL DE PREGUNTAS
st.markdown("---")
st.subheader("📚 Historial de consultas anteriores")

if st.toggle("📋 Mostrar historial de preguntas", key="toggle_historial"):
    try:
        conn = connect_db()
        df_logs = pd.read_sql("SELECT id, fecha, pregunta, sql_generado, resultado FROM chat_logs ORDER BY fecha DESC", conn)
        conn.close()

        st.dataframe(df_logs, use_container_width=True)

        csv_logs = df_logs.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Descargar historial como CSV",
            data=csv_logs,
            file_name="historial_chat_logs.csv",
            mime="text/csv"
        )
    except Exception as e:
        st.error(f"❌ Error al cargar logs desde la base de datos: {e}")

# DASHBOARD
st.markdown("---")
st.subheader("📈 Estadísticas de uso del asistente")

if st.toggle("📊 Mostrar dashboard de uso", key="toggle_dashboard"):
    try:
        conn = connect_db()
        df_stats = pd.read_sql("SELECT * FROM chat_logs", conn)
        conn.close()

        total_preguntas = len(df_stats)
        errores = df_stats["resultado"].str.contains("error", case=False, na=False).sum()
        ultima_fecha = df_stats["fecha"].max()
        tipos = df_stats["sql_generado"].str.extract(r'^\s*(\w+)', expand=False).value_counts()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total de preguntas", total_preguntas)
        col2.metric("Errores detectados", errores)
        col3.metric("Último uso", ultima_fecha.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(ultima_fecha) else "N/A")

        st.markdown("#### 🔍 Distribución por tipo de consulta SQL")
        st.bar_chart(tipos)

    except Exception as e:
        st.error(f"❌ No se pudieron cargar las métricas: {e}")

# MONITOREO DE COSTOS OPENAI
def obtener_consumo_openai(api_key):
    try:
        hoy = datetime.date.today()
        inicio_mes = hoy.replace(day=1)
        url = f"https://api.openai.com/v1/dashboard/billing/usage?start_date={inicio_mes}&end_date={hoy}"

        headers = {
            "Authorization": f"Bearer {api_key}"
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            usd = data.get("total_usage", 0) / 100
            return round(usd, 2)
        elif response.status_code == 401:
            return "❌ Error 401: API Key inválida o sin permisos de uso"
        else:
            return f"❌ Error {response.status_code}: {response.text}"

    except Exception as e:
        return f"❌ Excepción: {e}"

if st.toggle("💰 Ver costo acumulado en OpenAI", key="toggle_costos_openai"):
    with st.spinner("Consultando consumo..."):
        consumo = obtener_consumo_openai(st.secrets["OPENAI_API_KEY"])
        st.metric("Consumo actual OpenAI (mes)", f"${consumo}")



# Revisar IP
import requests

try:
    ip = requests.get("https://api64.ipify.org").text
    st.markdown(f"🌐 **IP pública del servidor (Streamlit):** `{ip}`")
except Exception as e:
    st.warning(f"No se pudo obtener la IP pública: {e}")

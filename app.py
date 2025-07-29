# app.py

import os
import datetime
import requests
import openai
import streamlit as st
import mysql.connector
import pandas as pd
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from pathlib import Path
import csv
import numpy as np
import json

# CONFIGURACIÓN INICIAL
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title("🧠 Asistente Inteligente de Intanis/NeuroVIA")

if st.button("🧹 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

# Inicializar historial en la sesión
if "historial" not in st.session_state:
    st.session_state["historial"] = []

if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []

# Mostrar historial conversacional
for entrada in st.session_state["conversacion"]:
    st.markdown(f"**🧠 Pregunta:** {entrada['pregunta']}")
    st.markdown(f"**💬 Respuesta:** {entrada['respuesta']}")
    st.markdown("---")

# API OPENAI
openai.api_key = st.secrets["OPENAI_API_KEY"]
llm = ChatOpenAI(temperature=0)

# CONEXIÓN A MySQL
def connect_db():
    return mysql.connector.connect(
        host="s1355.use1.mysecurecloudhost.com",
        port=3306,
        user="domolabs_admin",
        password="Pa$$w0rd_123",
        database="domolabs_Chatbot_SQL_DB"
    )

# VALIDACIÓN DE CONSULTAS SQL
def es_consulta_segura(sql):
    sql = sql.lower()
    comandos_peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(comando in sql for comando in comandos_peligrosos)

# EMBEDDINGS Y CACHE SEMÁNTICO
def obtener_embedding(texto):
    try:
        response = openai.Embedding.create(
            input=[texto],
            model="text-embedding-3-small"
        )
        return response["data"][0]["embedding"]
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
        embedding_nuevo = np.array(obtener_embedding(pregunta_nueva))
        if embedding_nuevo is None:
            return None

        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT pregunta, embedding, sql_generado FROM semantic_cache")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        for row in rows:
            embedding_guardado = np.array(json.loads(row["embedding"]))
            similitud = np.dot(embedding_nuevo, embedding_guardado) / (
                np.linalg.norm(embedding_nuevo) * np.linalg.norm(embedding_guardado)
            )
            if similitud >= umbral_similitud:
                return row["sql_generado"]

        return None
    except Exception as e:
        st.warning(f"Error al buscar en el semantic cache: {e}")
        return None



# Revisar IP
#import requests

#try:
 #   ip = requests.get("https://api64.ipify.org").text
  #  st.markdown(f"🌐 **IP pública del servidor (Streamlit):** `{ip}`")
#except Exception as e:
 #   st.warning(f"No se pudo obtener la IP pública: {e}")

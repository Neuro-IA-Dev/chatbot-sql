# chatbot-sql/app.py

import streamlit as st
import mysql.connector
from pathlib import Path
import pandas as pd
import datetime
import base64
from sqlalchemy import create_engine
from langchain.chat_models import ChatOpenAI
from langchain.agents.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.base import SQLDatabaseToolkit
from langchain_community.utilities.sql_database import SQLDatabase

# ---------------- CONFIGURACIÓN ----------------
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", layout="wide")
st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem;
        }
        .stChatMessage { background-color: #000 !important; }
    </style>
""", unsafe_allow_html=True)

# Logo y título
col1, col2 = st.columns([0.2, 0.8])
with col1:
    st.image("assets/logo_neurovia.png", width=180)
with col2:
    st.markdown("""
        <h1>🧠 Asistente Inteligente de <span style='color:#ffffff'>NeuroVIA</span></h1>
        <p style="font-size:1.2rem; color:#ccc;">Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.</p>
    """, unsafe_allow_html=True)

# ---------------- ESQUEMA DE BASE DE DATOS ----------------
db_schema = """
Base de datos: domolabs_Chatbot_SQL_DB

Tablas y relaciones:

1. **articulos**
   - cod_articulo (PK)
   - desc_articulo
   - desc_generico
   - desc_temporada
   - desc_grado_moda

2. **ventas**
   - numero_documento (PK)
   - cod_articulo (FK → articulos.cod_articulo)
   - ingresos
   - costos
   - tipo_documento
   - cod_tienda (FK → tiendas.cod_tienda)
   - fecha_venta

3. **tiendas**
   - cod_tienda (PK)
   - desc_tienda
   - cod_canal (FK → canal.cod_canal)
   - cod_marca (FK → marca.cod_marca)

4. **marca**
   - cod_marca (PK)
   - desc_marca

5. **canal**
   - cod_canal (PK)
   - desc_canal

Relaciones clave:
- ventas.cod_articulo → articulos.cod_articulo
- ventas.cod_tienda → tiendas.cod_tienda
- tiendas.cod_marca → marca.cod_marca
- tiendas.cod_canal → canal.cod_canal

Todas las consultas deben hacerse considerando este esquema y relaciones.
"""

# ---------------- CONEXIÓN A MySQL ----------------
engine = create_engine("mysql+mysqlconnector://domolabs_admin:Pa$$w0rd_123@localhost:3306/domolabs_Chatbot_SQL_DB")
db = SQLDatabase(engine=engine, include_tables=["articulos", "ventas", "tiendas", "marca", "canal"])

# ---------------- AGENTE DE LENGUAJE ----------------
llm = ChatOpenAI(temperature=0, openai_api_key=st.secrets["OPENAI_API_KEY"])
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
chain = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

# ---------------- INTERFAZ ----------------
st.markdown("""<br><b>💬 Consulta en lenguaje natural</b>""", unsafe_allow_html=True)
user_input = st.chat_input("Pregunta en lenguaje natural")

if user_input:
    with st.spinner("Procesando..."):
        try:
            result = chain.run(user_input)
            st.markdown("""<br>🔍 <b>Consulta SQL Generada:</b>""", unsafe_allow_html=True)
            st.success(result)
        except Exception as e:
            st.error(f"Error al ejecutar la consulta: {str(e)}")



# app.py

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
st.title("🧠 Asistente Inteligente de Intanis/NeuroVIA")
st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

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

# ESQUEMA DE LA BASE DE DATOS PARA EL PROMPT
db_schema = """
Base de datos: domolabs_Chatbot_SQL_DB

Tablas y relaciones:

1. articulos
   - cod_articulo (PK)
   - desc_articulo
   - desc_generico
   - desc_temporada
   - desc_grado_moda

2. ventas
   - numero_documento (PK)
   - cod_articulo (FK → articulos.cod_articulo)
   - ingresos
   - costos
   - tipo_documento
   - cod_tienda (FK → tiendas.cod_tienda)
   - fecha_venta

3. tiendas
   - cod_tienda (PK)
   - desc_tienda
   - cod_canal (FK → canal.cod_canal)
   - cod_marca (FK → marca.cod_marca)

4. marca
   - cod_marca (PK)
   - desc_marca

5. canal
   - cod_canal (PK)
   - desc_canal

Relaciones clave:
- ventas.cod_articulo → articulos.cod_articulo
- ventas.cod_tienda → tiendas.cod_tienda
- tiendas.cod_marca → marca.cod_marca
- tiendas.cod_canal → canal.cod_canal
"""

# PROMPT PERSONALIZADO CON ESQUEMA
sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template=f"""
Eres un asistente experto en SQL para una base de datos MySQL.
Este es el esquema de la base de datos:

{db_schema}

Genera únicamente el código SQL correcto basado en el esquema anterior.
No des explicaciones.

Pregunta: {{pregunta}}

SQL:
"""
)



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

# ENTRADA
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    st.markdown(f"**📝 Pregunta:** {pregunta}")

    # GENERAR CONSULTA SQL
    prompt = sql_prompt.format(pregunta=pregunta)
    sql_query = llm.predict(prompt).strip().strip("```sql").strip("```")

    st.markdown("🔍 **Consulta SQL Generada:**")
    st.code(sql_query, language="sql")

    # CONECTAR Y EJECUTAR
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(sql_query)

        if sql_query.lower().startswith("select"):
            columns = [col[0] for col in cursor.description]
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=columns)
            st.dataframe(df)
            resultado_str = f"{len(df)} filas"
        else:
            conn.commit()
            resultado_str = "Consulta ejecutada correctamente."

        cursor.close()
        conn.close()
        log_interaction(pregunta, sql_query, resultado_str)

    except Exception as e:
        st.error(f"❌ Error al ejecutar la consulta: {e}")
        log_interaction(pregunta, sql_query, f"Error: {e}")



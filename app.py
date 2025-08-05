# app.py

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

# CONFIGURACIÓN STREAMLIT
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title("🧠 Asistente Inteligente de Intanis/NeuroVIA")

# Botón para limpiar historial
if st.button("🧹 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

# Inicializar estados
if "historial" not in st.session_state:
    st.session_state["historial"] = []
if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []

# Mostrar conversación previa
for entrada in st.session_state["conversacion"]:
    st.markdown(f"**🧠 Pregunta:** {entrada['pregunta']}")
    st.markdown(f"**💬 Respuesta:** {entrada['respuesta']}")
    st.markdown("---")

# OPENAI API
llm = ChatOpenAI(temperature=0)

# CONEXIÓN A BASE DE DATOS
def connect_db():
    return mysql.connector.connect(
        host="s1355.use1.mysecurecloudhost.com",
        port=3306,
        user="domolabs_RedTabBot_USER",
        password="Pa$$w0rd_123",
        database="domolabs_RedTabBot_DB"
    )

# VERIFICACIÓN DE CONSULTAS SQL
def es_consulta_segura(sql):
    sql = sql.lower()
    comandos_peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(comando in sql for comando in comandos_peligrosos)

# PROMPT SQL
db_schema = """
Base de datos: domolabs_RedTabBot_DB
Tabla: VENTAS (COD_TIENDA, DESC_TIENDA, COD_CANAL, DESC_CANAL, INGRESOS, COSTOS, UNIDADES, MONEDA, etc.)
"""

sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template="""
Eres un asistente experto en análisis de datos para una empresa de retail. Tu tarea es interpretar preguntas en lenguaje natural y generar la consulta SQL correcta para obtener la información desde una única tabla llamada `VENTAS`.

La tabla `VENTAS` contiene información histórica de ventas, productos, tiendas, marcas, canales, clientes y artículos. Todos los datos están contenidos en esa misma tabla, por lo que no necesitas hacer JOINs.

🔁 Usa las siguientes reglas de mapeo inteligente:

1. Si el usuario menciona términos como "tienda", "cliente", "marca", "canal", "producto", "temporada", etc., asume que se refiere a su campo descriptivo (`DESC_...`) y **no al código (`COD_...`)**, excepto que el usuario especifique explícitamente “código de...”.
   - Ejemplo: "tienda" → `DESC_TIENDA`
   - Ejemplo: "código de tienda" → `COD_TIENDA`

2. Si el usuario pide:
   - "¿Cuántas tiendas?" o "total de tiendas": usa `COUNT(DISTINCT DESC_TIENDA)`
   - "¿Cuántos canales?" → `COUNT(DISTINCT DESC_CANAL)`
   - "¿Cuántos clientes?" → `COUNT(DISTINCT NOMBRE_CLIENTE)`
   - Aplica la lógica `COUNT(DISTINCT ...)` para cualquier atributo que tenga múltiples registros.

3. Siempre que se mencione:
   - "ventas", "ingresos": usar la columna `INGRESOS`
   - "costos": usar `COSTOS`
   - "unidades vendidas": usar `UNIDADES`
   - "producto", "artículo", "sku": puedes usar `DESC_ARTICULO` o `DESC_SKU` dependiendo del contexto.

4. No asumas que hay relaciones externas: toda la información está embebida en el tablon `VENTAS`.

5. Cuando pregunten por montos como ingresos o ventas, consulta si la información requerida debe ser en CLP o USD. Esta información está disponible en la columna `MONEDA`.

6. Cuando pregunten algo como "muestrame el codigo y descripcion de todas las tiendas que hay" debes hacer un distinct.

7. "Despacho a domicilio" es un ARTICULO

8. Fecha de venta es FECHA_DOCUMENTO.

🔐 Recuerda usar `WHERE`, `GROUP BY` o `ORDER BY` cuando el usuario pregunte por filtros, agrupaciones o rankings.

🖍️ Cuando generes la consulta SQL, no expliques la respuesta —solo entrega el SQL limpio y optimizado para MySQL.


"""
)

# LOG DE PREGUNTAS
def log_interaction(pregunta, sql, resultado):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = "INSERT INTO chat_logs (pregunta, sql_generado, resultado) VALUES (%s, %s, %s)"
        cursor.execute(query, (pregunta, sql, resultado))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar el log: {e}")

# SEMANTIC CACHE
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

def obtener_embedding(texto):
    if not texto.strip():
        return None
    try:
        response = client.embeddings.create(
            input=[texto],
            model="text-embedding-3-small"
        )
        return response.data[0].embedding
    except Exception as e:
        st.warning(f"❌ Error embedding: {e}")
        return None

def guardar_en_cache(pregunta, sql_generado, embedding):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO semantic_cache (pregunta, embedding, sql_generado) VALUES (%s, %s, %s)",
            (pregunta, json.dumps(embedding), sql_generado)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"❌ No se guardó en semantic_cache: {e}")

def buscar_sql_en_cache(pregunta_nueva, umbral_similitud=0.90):
    embedding_nuevo = obtener_embedding(pregunta_nueva)
    if embedding_nuevo is None:
        return None

    try:
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT pregunta, embedding, sql_generado FROM semantic_cache")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        vec_nuevo = np.array(embedding_nuevo)
        for row in rows:
            vec_guardado = np.array(json.loads(row["embedding"]))
            similitud = np.dot(vec_nuevo, vec_guardado) / (np.linalg.norm(vec_nuevo) * np.linalg.norm(vec_guardado))
            if similitud >= umbral_similitud:
                return row["sql_generado"]
    except Exception as e:
        st.warning(f"❌ Error buscando en cache: {e}")

    return None

# INTERFAZ PRINCIPAL
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    st.markdown(f"**📝 Pregunta:** {pregunta}")
    sql_query = buscar_sql_en_cache(pregunta)

    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        prompt = sql_prompt.format(pregunta=pregunta)
        sql_query = llm.predict(prompt).strip().strip("```sql").strip("```")
        embedding = obtener_embedding(pregunta)
        if embedding:
            guardar_en_cache(pregunta, sql_query, embedding)

    st.session_state["historial"].append((pregunta, sql_query))
    st.markdown("🔍 **Consulta SQL Generada:**")
    st.code(sql_query, language="sql")

    try:
        if not es_consulta_segura(sql_query):
            st.error("❌ Consulta peligrosa bloqueada.")
            log_interaction(pregunta, sql_query, "Consulta bloqueada")
        else:
            conn = connect_db()
            cursor = conn.cursor()
            cursor.execute(sql_query)

            if sql_query.lower().startswith("select"):
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=columns)
                st.dataframe(df)
                resultado = f"{len(df)} filas"
            else:
                conn.commit()
                resultado = "Consulta ejecutada."

            cursor.close()
            conn.close()
            st.markdown(f"**💬 Respuesta:** {resultado}")
            log_interaction(pregunta, sql_query, resultado)
            st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": resultado})
    except Exception as e:
        st.error(f"❌ Error ejecutando SQL: {e}")
        log_interaction(pregunta, sql_query, str(e))
        st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": str(e)})

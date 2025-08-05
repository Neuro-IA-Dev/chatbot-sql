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

# CONFIG STREAMLIT
st.set_page_config(page_title="Asistente Inteligente de NeuroVIA", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title(":brain: Asistente Inteligente de Intanis/NeuroVIA")

if st.button("\U0001f9f9 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

if "historial" not in st.session_state:
    st.session_state["historial"] = []
if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []

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

def es_consulta_segura(sql):
    sql = sql.lower()
    comandos_peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(comando in sql for comando in comandos_peligrosos)

# PROMPT
sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template="""..."""  # contenido del prompt omitido por brevedad
)

# LOGGING Y FEEDBACK
def log_interaction(pregunta, sql, resultado, feedback=None):
    try:
        conn = connect_db()
        cursor = conn.cursor()
        query = """
        INSERT INTO chat_logs (pregunta, sql_generado, resultado, feedback) 
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (pregunta, sql, resultado, feedback))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar el log: {e}")

client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

def obtener_embedding(texto):
    try:
        response = client.embeddings.create(input=[texto], model="text-embedding-3-small")
        return response.data[0].embedding
    except:
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

pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    with st.chat_message("user"):
        st.markdown(pregunta)

    sql_query = buscar_sql_en_cache(pregunta)

    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        prompt_text = sql_prompt.format(pregunta=pregunta)
        sql_query = llm.predict(prompt_text).strip().strip("```sql").strip("```")
        embedding = obtener_embedding(pregunta)
        if embedding:
            guardar_en_cache(pregunta, sql_query, embedding)

    st.session_state["historial"].append((pregunta, sql_query))

    with st.chat_message("assistant"):
        st.markdown("**🔍 Consulta SQL Generada:**")
        st.code(sql_query, language="sql")

        try:
            if not es_consulta_segura(sql_query):
                st.error("❌ Consulta peligrosa bloqueada.")
                log_interaction(pregunta, sql_query, "Consulta bloqueada")
            else:
                conn = connect_db()
                cursor = conn.cursor()
                cursor.execute(sql_query)

                # FIX: Evitar error "Unread result"
                while cursor.nextset():
                    pass

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

                # Evaluación con botones
                col1, col2 = st.columns(2)
                feedback = None
                with col1:
                    if st.button("✅ Fue acertada", key=f"ok_{pregunta}"):
                        feedback = "acertada"
                        st.success("Gracias por tu feedback.")
                with col2:
                    if st.button("❌ No fue correcta", key=f"fail_{pregunta}"):
                        feedback = "incorrecta"
                        st.error("Gracias, mejoraremos esta consulta.")

                log_interaction(pregunta, sql_query, resultado, feedback)
                st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": resultado})

        except Exception as e:
            st.error(f"❌ Error ejecutando SQL: {e}")
            log_interaction(pregunta, sql_query, str(e))
            st.session_state["conversacion"].append({"pregunta": pregunta, "respuesta": str(e)})

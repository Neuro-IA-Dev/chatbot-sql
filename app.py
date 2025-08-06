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

if "historial" not in st.session_state:
    st.session_state["historial"] = []
if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []

if st.button("🧹 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

llm = ChatOpenAI(temperature=0)

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

sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template="""
1. Si el usuario menciona términos como "tienda", "cliente", "marca", "canal", "producto", "temporada", "calidad", etc., asume que se refiere a su campo descriptivo (DESC_...) y **no al código (COD_...)**, excepto que el usuario especifique explícitamente “código de...”.

   - Ejemplo: "tienda" → DESC_TIENDA
   - Ejemplo: "marca" → DESC_MARCA
   - Ejemplo: "calidad" → DESC_CALIDAD
   - Ejemplo: "temporada" → DESC_TEMPORADA
   - Ejemplo: "producto" → DESC_ARTICULO
   - Ejemplo: "código de tienda" → COD_TIENDA

   Cuando el usuario mencione palabras que parecen referirse a nombres de marcas o productos (por ejemplo: "Levis", "Nike", "Adidas", etc.), **búscalas en DESC_MARCA**.

   Cuando el usuario mencione nombres de ciudades, centros comerciales u otros lugares (por ejemplo: "Costanera", "Talca", "Plaza Vespucio"), **búscalos en DESC_TIENDA**.

   Cuando filtres por estos campos descriptivos (DESC_...), usa SIEMPRE la cláusula LIKE '%valor%' en lugar de =, para permitir coincidencias parciales o mayúsculas/minúsculas.

2. Si el usuario pide:
   - "¿Cuántas tiendas?" o "total de tiendas": usa COUNT(DISTINCT DESC_TIENDA)
   - "¿Cuántos canales?" → COUNT(DISTINCT DESC_CANAL)
   - "¿Cuántos clientes?" → COUNT(DISTINCT NOMBRE_CLIENTE)

3. Siempre que se mencione:
   - "ventas", "ingresos": usar la columna INGRESOS
   - "costos": usar COSTOS
   - "unidades vendidas": usar UNIDADES
   - "producto", "artículo", "sku": puedes usar DESC_ARTICULO o DESC_SKU dependiendo del contexto.

4. No asumas que hay relaciones externas: toda la información está embebida en el tablon VENTAS.

5. Cuando pregunten por montos como ingresos o ventas, consulta si la información requerida debe ser en CLP o USD. Esta información está disponible en la columna MONEDA.

6. Cuando pregunten algo como "muestrame el codigo y descripcion de todas las tiendas que hay" debes hacer un distinct.

7. "Despacho a domicilio" es un ARTICULO

8. Fecha de venta es FECHA_DOCUMENTO.

9.- Si se menciona “para mujer”, “de mujer”, “femenino” o “de dama”, filtra con DESC_GENERO LIKE '%woman%'.
- Si se menciona “para hombre”, “masculino”, “de varón” o “de caballero”, filtra con DESC_GENERO LIKE '%men%'.
- Si se menciona “unisex”, usa DESC_GENERO LIKE '%unisex%'.

🔐 Recuerda usar WHERE, GROUP BY o ORDER BY cuando el usuario pregunte por filtros, agrupaciones o rankings.

🖍️ Cuando generes la consulta SQL, no expliques la respuesta —solo entrega el SQL limpio y optimizado para MySQL.

Pregunta: {pregunta}
"""
)
# CONTEXTO AUTOMÁTICO
referencias = {
    "esa tienda": "DESC_TIENDA",
    "ese canal": "DESC_CANAL",
    "esa marca": "DESC_MARCA",
    "ese producto": "DESC_ARTICULO",
    "ese artículo": "DESC_ARTICULO",
    "esa categoría": "DESC_CATEGORIA",
    "ese cliente": "NOMBRE_CLIENTE"
}

def aplicar_contexto(pregunta):
    pregunta_modificada = pregunta.lower()
    for ref, campo in referencias.items():
        if ref in pregunta_modificada and campo in st.session_state["contexto"]:
            pregunta_modificada = pregunta_modificada.replace(ref, st.session_state["contexto"][campo].lower())
    return pregunta_modificada

campos_contexto = ["DESC_TIENDA", "DESC_CANAL", "DESC_MARCA", "DESC_ARTICULO", "DESC_GENERO", "NOMBRE_CLIENTE"]

def actualizar_contexto(df):
    for campo in campos_contexto:
        if campo in df.columns and not df[campo].isnull().all():
            st.session_state["contexto"][campo] = str(df[campo].iloc[0])

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

# ENTRADA DEL USUARIO
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    with st.chat_message("user"):
        st.markdown(pregunta)

    sql_query = buscar_sql_en_cache(pregunta)
    guardar_en_cache_pending = None

    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        prompt_text = sql_prompt.format(pregunta=pregunta)
        sql_query = llm.predict(prompt_text).replace("```sql", "").replace("```", "").strip()
        embedding = obtener_embedding(pregunta)
        guardar_en_cache_pending = embedding if embedding else None

    resultado = ""

    try:
        if not es_consulta_segura(sql_query):
            st.error("❌ Consulta peligrosa bloqueada.")
            resultado = "Consulta bloqueada"
        else:
            conn = connect_db()
            cursor = conn.cursor()
            cursor.execute(sql_query)

            if sql_query.lower().startswith("select"):
                rows = cursor.fetchall()
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    if "FECHA_DOCUMENTO" in df.columns:
                        df["FECHA_DOCUMENTO"] = pd.to_datetime(df["FECHA_DOCUMENTO"].astype(str), format="%Y%m%d").dt.strftime("%d/%m/%Y")

                    st.dataframe(df)
                    resultado = f"{len(df)} filas"
                else:
                    resultado = "La consulta no devolvió resultados."
            else:
                conn.commit()
                resultado = "Consulta ejecutada."

            cursor.close()
            conn.close()

    except Exception as e:
        resultado = f"❌ Error ejecutando SQL: {e}"

    st.session_state["conversacion"].append({
        "pregunta": pregunta,
        "respuesta": resultado,
        "sql": sql_query,
        "cache": guardar_en_cache_pending
    })

# MOSTRAR TODAS LAS INTERACCIONES COMO CHAT
for i, item in enumerate(st.session_state["conversacion"]):
    with st.chat_message("user"):
        st.markdown(item["pregunta"])

    with st.chat_message("assistant"):
        if "sql" in item:
            st.markdown("**🔍 Consulta SQL Generada:**")
            st.code(item["sql"], language="sql")
            st.markdown(f"**💬 Respuesta:** {item['respuesta']}")
        else:
            st.warning("⚠️ No se generó una consulta SQL válida para esta pregunta.")
            st.markdown(f"**💬 Respuesta:** {item['respuesta']}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Fue acertada", key=f"ok_{i}"):
                st.success("Gracias por tu feedback. 👍")
                if item.get("cache"):
                    guardar_en_cache(item["pregunta"], item["sql"], item["cache"])
                log_interaction(item["pregunta"], item["sql"], item["respuesta"], "acertada")
        with col2:
            if st.button("❌ No fue correcta", key=f"fail_{i}"):
                st.warning("Gracias por reportarlo. Mejoraremos esta consulta. 🛠️")
                log_interaction(item["pregunta"], item["sql"], item["respuesta"], "incorrecta")

        st.markdown("---")

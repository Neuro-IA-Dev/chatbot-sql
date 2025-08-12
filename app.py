
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
import re

# CONFIG STREAMLIT
st.set_page_config(page_title="Asistente Inteligente de Ventas Retail", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title(":brain: Asistente Inteligente de Intanis Ventas Retail")
import requests

def obtener_ip_publica():
    try:
        ip = requests.get("https://api.ipify.org").text
        print(f"Tu IP pública es: {ip}")
        return ip
    except Exception as e:
        print(f"Error al obtener la IP pública: {e}")
        return None

# Ejecutar
ip_actual = obtener_ip_publica()

if "historial" not in st.session_state:
    st.session_state["historial"] = []
if "conversacion" not in st.session_state:
    st.session_state["conversacion"] = []
if "contexto" not in st.session_state:
    st.session_state["contexto"] = {}

if st.button("🧹 Borrar historial de preguntas", key="btn_borrar_historial"):
    st.session_state["historial"] = []
    st.session_state["conversacion"] = []
    st.success("Historial de conversación borrado.")
    
if st.button("🔁 Reiniciar contexto", key="btn_reset_contexto"):
    st.session_state["contexto"] = {}
    st.info("Contexto reiniciado (tienda, canal, marca, artículo, género, cliente).")

st.markdown("Haz una pregunta y el sistema generará y ejecutará una consulta SQL automáticamente.")

llm = ChatOpenAI(model_name="gpt-4o", temperature=0)

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

   Cuando DESC_MARCA sea igual a "Centro de Distribución LEVI" No se considera como una tienda, si no como "Centro de distribución" y no se contabiliza como tienda para ningun calculo.

   Cuando DESC_ARTICULO in ("Bolsa mediana LEVI'S®","Bolsa chica LEVI'S®","Bolsa grande LEVI'S®") no se considera un articulo, si no una Bolsa. Si se pregunta cuantas bolsas usa DESC_ARTICULO in ("Bolsa mediana LEVI'S®","Bolsa chica LEVI'S®","Bolsa grande LEVI'S®") y si se pregunta
   por Bolsas medianas usa DESC_ARTICULO = ("Bolsa mediana LEVI'S®") , bolsa chica usa  DESC_ARTICULO = ("Bolsa chica LEVI'S®"), y bolsa grande usa  DESC_ARTICULO = ("Bolsa grande LEVI'S®")

2. Si el usuario pide:
   - "¿Cuántas tiendas?" o "total de tiendas": usa COUNT(DISTINCT DESC_TIENDA) where DESC_TIENDA <> "Centro de Distribución LEVI"
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

10. Siempre que se pregunte "¿de qué canal es esa tienda?", "¿qué canal pertenece?" o algo similar, usa `SELECT DISTINCT DESC_CANAL ...` para evitar resultados duplicados.

11. Si se pregunta por promociones, se refiere al campo D_PROMO como descripcion y el PROMO como codigo. Un articulo se vendio con promocion cuando estos campos no son null.

12. Cuando TIPO_DOC es BO quiere decir que es BOLETA

13. Unidades negativas son devoluciones

14. EL DESC_ARTICULO = "DESPACHO A DOMICILIO" no se considera articulo si no un servicio. 

15. COD_MODELO,	COD_COLOR.	TALLA y	LARGO son campos que no tienen descripcion solo mostrarlos asi

16. Cuando se hable de un articulo, usar DESC_ARTICULO para mostrarlo a menos que se pida solo el Codigo. ejemplo "Jeans mas vendido de mujer por modelo, talla, largo y color"  DESC_ARTICULO, COD_MODELO, etc.

Cuando se reemplace un valor como “ese artículo”, “esa tienda”, etc., asegúrate de utilizar siempre `LIKE '%valor%'` en lugar de `=` para evitar errores por coincidencias exactas.

🔐 Recuerda usar WHERE, GROUP BY o ORDER BY cuando el usuario pregunte por filtros, agrupaciones o rankings.

🖍️ Cuando generes la consulta SQL, no expliques la respuesta —solo entrega el SQL limpio y optimizado para MySQL.

Pregunta: {pregunta}
"""
)

referencias = {
    "esa tienda": "DESC_TIENDA",
    "ese canal": "DESC_CANAL",
    "esa marca": "DESC_MARCA",
    "ese producto": "DESC_ARTICULO",
    "ese artículo": "DESC_ARTICULO",
    "ese articulo": "DESC_ARTICULO",
    "esa categoría": "DESC_CATEGORIA",
    "esa categoria": "DESC_CATEGORIA",
    "ese cliente": "NOMBRE_CLIENTE",
    "ese género": "DESC_GENERO",
    "ese genero": "DESC_GENERO",
    "ese sexo": "DESC_GENERO",
    "ese público": "DESC_GENERO",
    "esa categoría de género": "DESC_GENERO",
    "esa categoria de genero": "DESC_GENERO"
}

def aplicar_contexto(pregunta):
    pregunta_modificada = pregunta
    for ref, campo in referencias.items():
        if ref.lower() in pregunta.lower() and campo in st.session_state["contexto"]:
            # Escapar cualquier carácter especial del valor recordado (comillas, +, ?, etc.)
            valor_contexto = re.escape(st.session_state["contexto"][campo])
            # Reemplazo case-insensitive SOLO del texto de referencia (esa tienda, ese canal, etc.)
            pregunta_modificada = re.sub(ref, valor_contexto, pregunta_modificada, flags=re.IGNORECASE)
    return pregunta_modificada

campos_contexto = ["DESC_TIENDA", "DESC_CANAL", "DESC_MARCA", "DESC_ARTICULO", "DESC_GENERO", "NOMBRE_CLIENTE"]


def actualizar_contexto(df):
    # Mapa de posibles alias -> campo canónico
    alias = {
        "DESC_TIENDA": ["DESC_TIENDA", "TIENDA", "Tienda"],
        "DESC_CANAL": ["DESC_CANAL", "CANAL", "Canal"],
        "DESC_MARCA": ["DESC_MARCA", "MARCA", "Marca"],
        "DESC_ARTICULO": ["DESC_ARTICULO", "ARTICULO", "Artículo", "Articulo"],
        "DESC_GENERO": ["DESC_GENERO", "GENERO", "Género", "Genero"],
        "NOMBRE_CLIENTE": ["NOMBRE_CLIENTE", "CLIENTE", "Cliente"]
    }

    for canonico, posibles in alias.items():
        for col in posibles:
            if col in df.columns and not df[col].isnull().all():
                # Guarda el primer valor no nulo visible
                valor = str(df[col].dropna().iloc[0])
                if valor.strip():
                    st.session_state["contexto"][canonico] = valor
                break
def forzar_distinct_canal_si_corresponde(pregunta, sql_generado):
    """
    Si la pregunta pide el canal de una tienda (ej: '¿de qué canal es esa tienda?'),
    envuelve el SQL en un SELECT DISTINCT para evitar filas duplicadas.
    """
    if re.search(r'\bcanal(es)?\b', pregunta, flags=re.IGNORECASE) and \
       re.search(r'\btienda\b|esa tienda', pregunta, flags=re.IGNORECASE):
        # Evitar doble DISTINCT si ya viene correcto
        if not re.search(r'\bselect\s+distinct\b', sql_generado, flags=re.IGNORECASE):
            return f"SELECT DISTINCT DESC_CANAL FROM ({sql_generado}) AS t"
    return sql_generado

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
# ENTRADA DEL USUARIO
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

# Inicializar para evitar NameError si aún no hay pregunta
sql_query = None
resultado = ""
guardar_en_cache_pending = None

if pregunta:
    with st.chat_message("user"):
        st.markdown(pregunta)

    # 1) Intentar reutilizar desde la cache semántica
    sql_query = buscar_sql_en_cache(pregunta)

    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        # 2) Derivar género desde la pregunta (mejora de contexto)
        if re.search(r'\b(mujer|femenin[oa])\b', pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Woman"
        elif re.search(r'\b(hombre|masculin[oa]|varón|varon|caballero)\b', pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Men"
        elif re.search(r'\bunisex\b', pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Unisex"

        # 3) Aplicar contexto y generar SQL con el LLM
        pregunta_con_contexto = aplicar_contexto(pregunta)
        prompt_text = sql_prompt.format(pregunta=pregunta_con_contexto)
        sql_query = llm.predict(prompt_text).replace("```sql", "").replace("```", "").strip()

        # 4) Forzar DISTINCT si corresponde
        sql_query = forzar_distinct_canal_si_corresponde(pregunta_con_contexto, sql_query)

        # 5) Preparar guardado en cache
        embedding = obtener_embedding(pregunta)
        guardar_en_cache_pending = embedding if embedding else None

    # 6) Ejecutar SQL
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
                        df["FECHA_DOCUMENTO"] = pd.to_datetime(
                            df["FECHA_DOCUMENTO"].astype(str), format="%Y%m%d"
                        ).dt.strftime("%d/%m/%Y")
                    st.dataframe(df)
                    resultado = f"{len(df)} filas"
                    actualizar_contexto(df)
                else:
                    resultado = "La consulta no devolvió resultados."
            else:
                conn.commit()
                resultado = "Consulta ejecutada."

            cursor.close()
            conn.close()
    except Exception as e:
        resultado = f"❌ Error ejecutando SQL: {e}"

    # 7) Guardar conversación
    st.session_state["conversacion"].append({
        "pregunta": pregunta,
        "respuesta": resultado,
        "sql": sql_query,
        "cache": guardar_en_cache_pending
    })
# MOSTRAR TODAS LAS INTERACCIONES COMO CHAT
# UI MEJORADA EN STREAMLIT
# (Esta parte va justo al final del archivo app.py, reemplazando el bloque de visualización actual de interacciones)

if pregunta and sql_query is not None:
    with st.chat_message("user"):
        st.markdown(f"### 🤖 Pregunta actual:")
        st.markdown(f"> {pregunta}")

    with st.chat_message("assistant"):
        st.markdown("### 🔍 Consulta SQL Generada:")
        st.code(sql_query, language="sql")
        st.markdown("### 💬 Respuesta:")
        st.markdown(resultado)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Fue acertada", key=f"ok_last"):
                st.success("Gracias por tu feedback. 👍")
                if guardar_en_cache_pending:
                    guardar_en_cache(pregunta, sql_query, guardar_en_cache_pending)
                log_interaction(pregunta, sql_query, resultado, "acertada")
        with col2:
            if st.button("❌ No fue correcta", key=f"fail_last"):
                st.warning("Gracias por reportarlo. Mejoraremos esta consulta. 🚲")
                log_interaction(pregunta, sql_query, resultado, "incorrecta")

    st.markdown("---")

# MOSTRAR HISTORIAL PREVIO (EXCLUYENDO LA Última PREGUNTA)
if st.session_state["conversacion"]:
    st.markdown("## ⌛ Historial de preguntas anteriores")
    for i, item in enumerate(reversed(st.session_state["conversacion"][:-1])):
        with st.expander(f"💬 {item['pregunta']}", expanded=False):
            st.markdown("**Consulta SQL Generada:**")
            st.code(item["sql"], language="sql")

            st.markdown("**📊 Resultado:**")
            try:
                # Intentar volver a ejecutar la consulta para mostrar los resultados
                if es_consulta_segura(item["sql"]):
                    conn = connect_db()
                    cursor = conn.cursor()
                    cursor.execute(item["sql"])
                    rows = cursor.fetchall()
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        df_hist = pd.DataFrame(rows, columns=columns)
                        if "FECHA_DOCUMENTO" in df_hist.columns:
                            df_hist["FECHA_DOCUMENTO"] = pd.to_datetime(df_hist["FECHA_DOCUMENTO"].astype(str), errors="coerce", format="%Y%m%d").dt.strftime("%d/%m/%Y")
                        st.dataframe(df_hist, hide_index=True)
                    else:
                        st.markdown("*Sin resultados para esta consulta.*")
                    cursor.close()
                    conn.close()
                else:
                    st.warning("⚠️ Consulta peligrosa. No se vuelve a ejecutar por seguridad.")
            except Exception as e:
                st.error(f"❌ Error al mostrar resultado anterior: {e}")

            col1, col2 = st.columns(2)
            with col1:
                if st.button("✅ Fue acertada", key=f"ok_{i}"):
                    st.success("Gracias por tu feedback. 👍")
                    if item.get("cache"):
                        guardar_en_cache(item["pregunta"], item["sql"], item["cache"])
                    log_interaction(item["pregunta"], item["sql"], "respuesta recreada", "acertada")
            with col2:
                if st.button("❌ No fue correcta", key=f"fail_{i}"):
                    st.warning("Gracias por reportarlo. Mejoraremos esta consulta. 🚲")
                    log_interaction(item["pregunta"], item["sql"], "respuesta recreada", "incorrecta")

        st.markdown("---")

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

if st.button("🧹 Borrar historial de preguntas"):
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

# PROMPT PERSONALIZADO CON EJEMPLOS
ejemplos = """
Ejemplo 1:
Pregunta: ¿Cuál es la tienda que más ha vendido?
SQL: SELECT t.desc_tienda, SUM(v.ingresos) AS total_ventas
FROM ventas v
JOIN tiendas t ON v.cod_tienda = t.cod_tienda
GROUP BY t.desc_tienda
ORDER BY total_ventas DESC
LIMIT 1;

Ejemplo 2:
Pregunta: ¿Cuáles son los artículos más vendidos?
SQL: SELECT a.desc_articulo, COUNT(*) AS cantidad
FROM ventas v
JOIN articulos a ON v.cod_articulo = a.cod_articulo
GROUP BY a.desc_articulo
ORDER BY cantidad DESC;

Ejemplo 3:
Pregunta: ¿Qué canal tiene más ingresos?
SQL: SELECT c.desc_canal, SUM(v.ingresos) AS total
FROM ventas v
JOIN tiendas t ON v.cod_tienda = t.cod_tienda
JOIN canal c ON t.cod_canal = c.cod_canal
GROUP BY c.desc_canal
ORDER BY total DESC
LIMIT 1;
"""

sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template=f"""
Eres un asistente experto en SQL para una base de datos MySQL.
Este es el esquema de la base de datos:

{db_schema}

A continuación algunos ejemplos para que aprendas cómo responder:

{ejemplos}

Ahora responde esta nueva pregunta:
Pregunta: {{pregunta}}

SQL:
"""
)

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

# ENTRADA
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")

if pregunta:
    st.markdown(f"**📝 Pregunta:** {pregunta}")

    # Construcción del contexto con historial
    contexto = ""
    for i, (preg, sql) in enumerate(st.session_state["historial"][-5:]):
        contexto += f"Pregunta anterior: {preg}\nSQL generado: {sql}\n"

    prompt_completo = f"""
{contexto}
Nueva pregunta: {pregunta}
"""

    prompt = sql_prompt.format(pregunta=prompt_completo)
    sql_query = llm.predict(prompt).strip().strip("```sql").strip("```")

    st.session_state["historial"].append((pregunta, sql_query))

    st.markdown("🔍 **Consulta SQL Generada:**")
    st.code(sql_query, language="sql")

    # CONECTAR Y EJECUTAR
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

# DASHBOARD
st.markdown("---")
st.subheader("📈 Estadísticas de uso del asistente")

if st.toggle("📊 Mostrar dashboard de uso"):
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




# Revisar IP
#import requests

#try:
 #   ip = requests.get("https://api64.ipify.org").text
  #  st.markdown(f"🌐 **IP pública del servidor (Streamlit):** `{ip}`")
#except Exception as e:
 #   st.warning(f"No se pudo obtener la IP pública: {e}")

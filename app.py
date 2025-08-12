
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
# Patrón país usado por los detectores (debe declararse antes de usarse)
_COUNTRY_REGEX = r"\b(chile|per[uú]|bolivia|pa[ií]s(?:es)?)\b"

# CONFIG STREAMLIT
st.set_page_config(page_title="Asistente Inteligente de Ventas Retail", page_icon="🧠")
st.image("assets/logo_neurovia.png", width=180)
st.title(":brain: Asistente Inteligente de Intanis Ventas Retail")
import requests
import io

def make_excel_download_bytes(df: pd.DataFrame, sheet_name="Datos"):
    """Devuelve bytes de un .xlsx con el dataframe."""
    bio = io.BytesIO()
    # Usa xlsxwriter si está disponible; pandas cae a openpyxl si no.
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.getvalue()
def obtener_ip_publica():
    try:
        # Evita que se quede pegado si el servicio no responde
        return requests.get("https://api.ipify.org", timeout=2).text
    except Exception:
        return None
def _fmt_money(v: float) -> str:
    if pd.isna(v):
        return ""
    s = f"{float(v):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def aplicar_formato_monetario(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    # columnas candidatos
    money_cols = [c for c in df.columns if c.upper() in (
        "INGRESOS","COSTOS","PRECIO","PRECIO_PROMEDIO","MARGEN","GM","VALOR","VALORES","TOTAL"
    )]
    if not money_cols:
        return df

    df2 = df.copy()
    if "MONEDA" in df2.columns:
        for c in money_cols:
            df2[c] = df2.apply(lambda r: f"{_fmt_money(r[c])} {r['MONEDA']}" if pd.notnull(r[c]) else r[c], axis=1)
    else:
        # usa la última(s) moneda(s) confirmada(s) por el usuario si hay solo una
        last = st.session_state.get("clarif_moneda_last")
        suf = None
        if isinstance(last, list) and len(last) == 1:
            suf = last[0]
        elif isinstance(last, str):
            suf = last
        if suf:
            for c in money_cols:
                df2[c] = df2[c].map(lambda x: f"{_fmt_money(x)} {suf}" if pd.notnull(x) else x)
        else:
            # sin info de moneda → solo formato numérico europeo
            for c in money_cols:
                df2[c] = df2[c].map(lambda x: _fmt_money(x) if pd.notnull(x) else x)
    return df2

def _to_yyyymmdd(v) -> str:
    """Acepta date, datetime o string dd/mm/yyyy y devuelve 'YYYYMMDD'."""
    if isinstance(v, _dt.date):
        return v.strftime("%Y%m%d")
    if isinstance(v, str):
        v = v.strip()
        # dd/mm/yyyy
        try:
            d = _dt.datetime.strptime(v, "%d/%m/%Y").date()
            return d.strftime("%Y%m%d")
        except Exception:
            pass
        # yyyy-mm-dd (por si llega así)
        try:
            d = _dt.datetime.strptime(v, "%Y-%m-%d").date()
            return d.strftime("%Y%m%d")
        except Exception:
            pass
    # si no se pudo parsear, devuelve tal cual
    return str(v)
# --- País <-> moneda -------------------------------------------
_LOCAL_CURRENCY_BY_SOC = {"1000": "CLP", "2000": "PEN", "3000": "BOB"}
_SOC_BY_NAME = {"chile": "1000", "perú": "2000", "peru": "2000", "bolivia": "3000"}

def _extraer_paises(texto: str) -> set[str]:
    """Set de SOCIEDAD_CO presentes explícitamente en el texto (por nombre o código)."""
    codes = set()
    for k, v in _SOC_BY_NAME.items():
        if re.search(rf"\b{k}\b", texto, re.I):
            codes.add(v)
    for m in re.findall(r"\b(1000|2000|3000)\b", texto):
        codes.add(m)
    return codes

def _sugerir_monedas(paises: set[str], es_agrupado_por_pais: bool) -> list[str]:
    # Multi-país o ranking/comparación por país -> USD
    if es_agrupado_por_pais or len(paises) != 1:
        return ["USD"]
    # Un solo país -> USD + local
    unico = next(iter(paises))
    return ["USD", _LOCAL_CURRENCY_BY_SOC.get(unico, "USD")]

# --- Moneda: detectar en el texto (agrega PEN/BOB)
def _tiene_moneda(texto: str) -> bool:
    return bool(re.search(r"\b(usd|clp|pen|bob|d[oó]lar(?:es)?|pesos?)\b", texto, re.I))

# Ejecutar y mostrar IP saliente (útil para Remote MySQL en cPanel)
ip_actual = obtener_ip_publica()
if ip_actual:
    st.caption(f"IP saliente detectada: {ip_actual} — agrégala en cPanel → Remote MySQL (Add Access Host).")
else:
    st.caption("No se pudo detectar la IP saliente (timeout/red).")

def split_queries(sql_text: str) -> list[str]:
    """Divide el SQL por ';' y limpia vacíos. Suficiente para la mayoría de casos."""
    return [q.strip() for q in sql_text.strip().split(";") if q.strip()]

def ejecutar_select(conn, query: str) -> pd.DataFrame | None:
    q = query.strip()
    if not q.lower().startswith("select"):
        cur = conn.cursor(buffered=True)  # evita unread result también aquí
        cur.execute(q)
        conn.commit()
        cur.close()
        return None

    # SELECT: pandas consume todo → sin Unread result found
    df = pd.read_sql_query(q, conn)
    if "FECHA_DOCUMENTO" in df.columns:
        df["FECHA_DOCUMENTO"] = pd.to_datetime(
            df["FECHA_DOCUMENTO"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.strftime("%d/%m/%Y")
    return df

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

llm = ChatOpenAI(
    model_name="gpt-4o",
    temperature=0,
    openai_api_key=st.secrets["OPENAI_API_KEY"],
    max_retries=1,
    request_timeout=30,
)

def connect_db():
    try:
        return mysql.connector.connect(
            host="s1355.use1.mysecurecloudhost.com",
            port=3306,
            user="domolabs_RedTabBot_USER",
            password="Pa$$w0rd_123",
            database="domolabs_RedTabBot_DB",
            connection_timeout=8,   # ← evita cuelgues largos
        )
    except mysql.connector.Error as e:
        st.error(
            "❌ No se pudo conectar a MySQL.\n\n"
            "Posibles causas: servidor caído, tu IP no está autorizada en cPanel → Remote MySQL, "
            "o límite de conexiones.\n\n"
            f"Detalle técnico: {e}"
            + (f"\n\nIP detectada: {ip_actual}" if ip_actual else "")
        )
        return None


def es_consulta_segura(sql):
    if not sql or not isinstance(sql, str):
        return False
    sql_l = sql.lower()
    comandos_peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(c in sql_l for c in comandos_peligrosos)

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

    La columna SOCIEDAD_CO representa al pais 1000 = "Chile", 2000 = "Perú" y 3000 = "Bolivia" siempre que se mencione pais, usa esta regla.
    
   Cuando el usuario mencione palabras que parecen referirse a nombres de marcas o productos (por ejemplo: "Levis", "Nike", "Adidas", etc.), **búscalas en DESC_MARCA**.

   Cuando el usuario mencione nombres de ciudades, centros comerciales u otros lugares (por ejemplo: "Costanera", "Talca", "Plaza Vespucio"), **búscalos en DESC_TIENDA**.

   Cuando filtres por estos campos descriptivos (DESC_...), usa SIEMPRE la cláusula LIKE '%valor%' en lugar de =, para permitir coincidencias parciales o mayúsculas/minúsculas.

   Cuando DESC_TIENDA sea igual a "Centro de Distribución LEVI" No se considera como una tienda, si no como "Centro de distribución" y no se contabiliza como tienda para ningun calculo.

   Cuando DESC_ARTICULO in ("Bolsa mediana LEVI'S®","Bolsa chica LEVI'S®","Bolsa grande LEVI'S®") no se considera un articulo, si no una Bolsa. Si se pregunta cuantas bolsas usa DESC_ARTICULO in ("Bolsa mediana LEVI'S®","Bolsa chica LEVI'S®","Bolsa grande LEVI'S®") y si se pregunta
   por Bolsas medianas usa DESC_ARTICULO = ("Bolsa mediana LEVI'S®") , bolsa chica usa  DESC_ARTICULO = ("Bolsa chica LEVI'S®"), y bolsa grande usa  DESC_ARTICULO = ("Bolsa grande LEVI'S®")

2. Si el usuario pide:
   - "¿Cuántas tiendas?" o "total de tiendas": usa COUNT(DISTINCT DESC_TIENDA) where DESC_TIENDA <> "Centro de Distribución LEVI"
   - "¿Cuántos canales?" → COUNT(DISTINCT DESC_CANAL)
   - "¿Cuántos clientes?" → COUNT(DISTINCT NOMBRE_CLIENTE)

3. Siempre que se mencione:
   - "ventas", "ingresos","precios": usar la columna INGRESOS
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

13. Unidades negativas son devoluciones, si se pregunta por precios bajos o baratos, solo considerar unidades mayores a 0

14. EL DESC_ARTICULO = "DESPACHO A DOMICILIO" no se considera articulo si no un servicio. 

15. COD_MODELO,	COD_COLOR.	TALLA y	LARGO son campos que no tienen descripcion solo mostrarlos asi

16. Cuando se hable de un articulo, usar DESC_ARTICULO para mostrarlo a menos que se pida solo el Codigo. ejemplo "Jeans mas vendido de mujer por modelo, talla, largo y color"  DESC_ARTICULO, COD_MODELO, etc.

17. Cuando filtres por FECHA_DOCUMENTO, usa SIEMPRE formato 'YYYYMMDD' **sin guiones**. Ejemplo:
    WHERE FECHA_DOCUMENTO BETWEEN '20250101' AND '20250131'
    (La columna es numérica/texto sin guiones; NO uses '2025-01-01'.)

18. Si la consulta es por país (ranking, “más vende”, “por país”, etc.):
    - Agrupa por SOCIEDAD_CO y decodifica el nombre con:
      CASE SOCIEDAD_CO WHEN '1000' THEN 'Chile' WHEN '2000' THEN 'Perú' WHEN '3000' THEN 'Bolivia' END AS PAIS

Cuando se reemplace un valor como “ese artículo”, “esa tienda”, etc., asegúrate de utilizar siempre `LIKE '%valor%'` en lugar de `=` para evitar errores por coincidencias exactas.

🔐 Recuerda usar WHERE, GROUP BY o ORDER BY cuando el usuario pregunte por filtros, agrupaciones o rankings.

🖍️ Cuando generes la consulta SQL, no expliques la respuesta —solo entrega el SQL limpio y optimizado para MySQL.

Pregunta: {pregunta}
"""
)

referencias = {
    "esa tienda": "DESC_TIENDA",
    "esta tienda": "DESC_TIENDA",
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
    "esa categoria de genero": "DESC_GENERO",
    "ese pais": "SOCIEDAD_CO",
    "ese país": "SOCIEDAD_CO"
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

campos_contexto = ["DESC_TIENDA", "DESC_CANAL", "DESC_MARCA", "DESC_ARTICULO", "DESC_GENERO", "NOMBRE_CLIENTE","SOCIEDAD_CO"]


def actualizar_contexto(df):
    # Mapa de posibles alias -> campo canónico
    alias = {
        "DESC_TIENDA": ["DESC_TIENDA", "TIENDA", "Tienda"],
        "DESC_CANAL": ["DESC_CANAL", "CANAL", "Canal"],
        "DESC_MARCA": ["DESC_MARCA", "MARCA", "Marca"],
        "DESC_ARTICULO": ["DESC_ARTICULO", "ARTICULO", "Artículo", "Articulo"],
        "DESC_GENERO": ["DESC_GENERO", "GENERO", "Género", "Genero"],
        "NOMBRE_CLIENTE": ["NOMBRE_CLIENTE", "CLIENTE", "Cliente"],
        "SOCIEDAD_CO": ["PAIS", "PAISES", "Pais","Paises","Países","País"]
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
    Si la pregunta pide el pais de una tienda (ej: '¿de qué pais es esa tienda?'),
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
        if conn is None:
            return None  # Sin conexión → no hay cache

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

# ==== DESAMBIGUACIÓN: detectores y UI ========================================

import datetime as _dt
from typing import Optional, Tuple

# Palabras que delatan montos:
_MONEY_KEYS = (
    r"(venta|vende|ventas|ingreso|ingresos|margen|utilidad|gm|revenue|sales|facturaci[oó]n|"
    r"precio|precios|car[oa]s?|barat[oa]s?|cost[eo]s?|ticket\s*promedio|valor(?:es)?)"
)
    # Palabras que delatan pais:
# --- País: detectores ----------------------------------------
 

def _habla_de_pais(texto: str) -> bool:
    # ¿se menciona la noción de país en general?
    return bool(re.search(_COUNTRY_REGEX, texto, re.I))

def _tiene_pais(texto: str) -> bool:
    # ¿viene un país explícito (por nombre o código SOCIEDAD_CO)?
    return bool(re.search(r"\b(1000|2000|3000|chile|per[uú]|bolivia)\b", texto, re.I))

def _agregacion_por_pais(texto: str) -> bool:
    # intenciones de ranking/agrupación/comparación por país
    patrones = (
        r"(por\s+pa[ií]s|seg[uú]n\s+pa[ií]s|ranking\s+de\s+pa[ií]ses|"
        r"top\s+\d+\s+pa[ií]ses|comparaci[oó]n\s+por\s+pa[ií]s|"
        r"cu[aá]l(?:es)?\s+es\s+el\s+pa[ií]s\s+que\s+(?:m[aá]s|menos))"
    )
    return bool(re.search(patrones, texto, re.I))
# Palabras que delatan fechas explícitas:
_DATE_KEYS = r"(hoy|ayer|semana|mes|año|anio|últim|ultimo|desde|hasta|entre|rango|202\d|20\d\d|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"



# Mapa utilitario para SOCIEDAD_CO
_PAIS_MAP = {"chile": "1000", "peru": "2000", "perú": "2000", "bolivia": "3000"}

def _extraer_pais(texto: str):
    """Devuelve (codigo, etiqueta) si aparece un país en el texto, si no (None, None)."""
    m = re.search(r"\b(chile|per[uú]|bolivia)\b", texto, re.I)
    if not m:
        return None, None
    p = m.group(1).lower()
    if p.startswith("chil"): return "1000", "Chile"
    if p.startswith("per"):  return "2000", "Perú"
    if p.startswith("bol"):  return "3000", "Bolivia"
    return None, None
def _pide_montos(texto: str) -> bool:
    return bool(re.search(_MONEY_KEYS, texto, re.I))

def _tiene_fecha(texto: str) -> bool:
    return bool(re.search(_DATE_KEYS, texto, re.I))

def _habla_de_tienda(texto: str) -> bool:
    return bool(re.search(r"\btienda(s)?\b", texto, re.I))


def _menciona_cd(texto: str) -> bool:
    # si el usuario ya dijo explícitamente CD o ese nombre, no preguntamos
    return bool(re.search(r"centro\s+de\s+distribuci[oó]n", texto, re.I) or re.search(r"\bCD\b", texto, re.I))
# --- País: detectores (definir una sola vez) -----------------
_COUNTRY_REGEX = r"\b(chile|per[uú]|bolivia|pa[ií]s(?:es)?)\b"

def _habla_de_pais(texto: str) -> bool:
    # ¿se menciona la noción de país en general?
    return bool(re.search(_COUNTRY_REGEX, texto, re.I))

def _tiene_pais(texto: str) -> bool:
    # ¿hay un país explícito por nombre o por código SOCIEDAD_CO?
    return bool(re.search(r"\b(1000|2000|3000|chile|per[uú]|bolivia)\b", texto, re.I))

def _agregacion_por_pais(texto: str) -> bool:
    # intenciones de ranking/agrupación/comparación por país
    patrones = (
        r"(por\s+pa[ií]s|seg[uú]n\s+pa[ií]s|ranking\s+de\s+pa[ií]ses|"
        r"top\s+\d+\s+pa[ií]ses|comparaci[oó]n\s+por\s+pa[ií]s|"
        r"cu[aá]l(?:es)?\s+es\s+el\s+pa[ií]s\s+que\s+(?:m[aá]s|menos))"
    )
    return bool(re.search(patrones, texto, re.I))

def _extraer_pais(texto: str):
    """Devuelve (codigo, etiqueta) si aparece un país en el texto; si no, (None, None)."""
    m = re.search(r"\b(chile|per[uú]|bolivia)\b", texto, re.I)
    if not m:
        return None, None
    p = m.group(1).lower()
    if p.startswith("chil"): return "1000", "Chile"
    if p.startswith("per"):  return "2000", "Perú"
    if p.startswith("bol"):  return "3000", "Bolivia"
    return None, None

def _necesita_aclaracion(texto: str) -> dict:
    habla_pais  = _habla_de_pais(texto)
    tiene_pais  = _tiene_pais(texto)
    agrega_pais = _agregacion_por_pais(texto)

    return {
        "moneda": (_pide_montos(texto) and not _tiene_moneda(texto)),
        # Solo preguntamos "País" si se habla de país, NO hay uno explícito
        # y NO es una intención de ranking/agrupación por país.
        "pais":   (habla_pais and not tiene_pais and not agrega_pais),
        "fecha":  (not _tiene_fecha(texto)),
        "tienda_vs_cd": (_habla_de_tienda(texto) and not _menciona_cd(texto)),
    }

def _defaults_fecha() -> Tuple[str, str, str]:
    """Rango por defecto: últimos 30 días, en formato dd/mm/yyyy + yyyyMMdd."""
    hoy = _dt.date.today()
    desde = hoy - _dt.timedelta(days=30)
    # Para mostrar:
    desde_str = desde.strftime("%d/%m/%Y")
    hasta_str = hoy.strftime("%d/%m/%Y")
    # Para SQL (si luego quisieras inyectar literal):
    desde_sql = desde.strftime("%Y%m%d")
    hasta_sql = hoy.strftime("%Y%m%d")
    return desde_str, hasta_str, f"{desde_sql}-{hasta_sql}"

def _inyectar_aclaraciones_en_pregunta(pregunta: str, moneda, rango, excluir_cd):
    partes = [pregunta.strip()]

    if moneda:
        partes.append(f" en moneda {moneda}")

    if rango:
        d, h = rango
        d_norm = _to_yyyymmdd(d)
        h_norm = _to_yyyymmdd(h)
        # Díselo explícito al modelo
        partes.append(
            f" usando FECHA_DOCUMENTO entre {d_norm} y {h_norm} (formato YYYYMMDD sin guiones)"
        )

    if excluir_cd is not None:
        partes.append(
            " excluyendo el Centro de Distribución" if excluir_cd
            else " incluyendo el Centro de Distribución"
        )
    return " ".join(partes).strip()


# ===== Monedas por país =====
_LOCAL_CURRENCY_BY_SOC = {"1000": "CLP", "2000": "PEN", "3000": "BOB"}
_SOC_BY_NAME = {"chile": "1000", "perú": "2000", "peru": "2000", "bolivia": "3000"}

def _extraer_paises(texto: str) -> set[str]:
    codes = set()
    for k, v in _SOC_BY_NAME.items():
        if re.search(rf"\b{k}\b", texto, re.I):
            codes.add(v)
    for m in re.findall(r"\b(1000|2000|3000)\b", texto):
        codes.add(m)
    return codes

def _sugerir_monedas(paises: set[str], es_agrupado_por_pais: bool) -> list[str]:
    if es_agrupado_por_pais or len(paises) != 1:
        return ["USD"]
    unico = next(iter(paises))
    return ["USD", _LOCAL_CURRENCY_BY_SOC.get(unico, "USD")]

def _tiene_moneda(texto: str) -> bool:
    # Detecta USD/CLP/PEN/BOB
    return bool(re.search(r"\b(usd|clp|pen|bob|d[oó]lar(?:es)?|pesos?)\b", texto, re.I))


def manejar_aclaracion(pregunta: str) -> Optional[str]:
    flags = _necesita_aclaracion(pregunta)
    if not any(flags.values()):
        return None

    st.info("Antes de ejecutar, aclaremos algunos detalles para evitar resultados ambiguos 👇")

    # Estado inicial
    st.session_state.setdefault("clarif_moneda", None)
    st.session_state.setdefault("clarif_fecha_desde", None)
    st.session_state.setdefault("clarif_fecha_hasta", None)
    st.session_state.setdefault("clarif_excluir_cd", True)

    # Países detectados y sugerencia de monedas
    paises_texto = _extraer_paises(pregunta)
    es_agrupado = _agregacion_por_pais(pregunta)
    sugeridas = _sugerir_monedas(paises_texto, es_agrupado)

    # Monedas permitidas según regla
    if es_agrupado or len(paises_texto) != 1:
        monedas_permitidas = ["USD"]
    elif len(paises_texto) == 1:
        local = _LOCAL_CURRENCY_BY_SOC[list(paises_texto)[0]]
        monedas_permitidas = ["USD", local]
    else:
        monedas_permitidas = ["USD", "CLP", "PEN", "BOB"]

    # Moneda
    if flags["moneda"]:
        st.subheader("Moneda")
        st.session_state["clarif_moneda"] = st.multiselect(
            "¿En qué moneda(s) quieres ver los montos?",
            options=monedas_permitidas,
            default=sugeridas,
            key="k_moneda_multi",
            help="Si comparas varios países o pides ranking por país, sólo USD."
        )
    else:
        if st.session_state.get("clarif_moneda") is None:
            st.session_state["clarif_moneda"] = sugeridas

    # Rango de fechas
    if flags["fecha"]:
        st.subheader("Rango de fechas")
        hoy = _dt.date.today()
        desde_def = hoy - _dt.timedelta(days=30)
        val = st.date_input("Selecciona el rango", value=(desde_def, hoy), key="k_rango_fechas")
        if isinstance(val, tuple) and len(val) == 2:
            d, h = val
        else:
            d, h = val, None
        st.session_state["clarif_fecha_desde"] = d
        st.session_state["clarif_fecha_hasta"] = h
        if h is None:
            st.caption("Elige también la fecha de término para continuar.")
            st.stop()

    # País (si no viene claro en el texto y no es ranking por país)
    pais_code, pais_label = _extraer_pais(pregunta)
    if flags.get("pais"):
        st.subheader("País")
        if not pais_code:
            pais_label = st.radio(
                "¿Para qué país?",
                options=["Chile", "Perú", "Bolivia"],
                horizontal=True,
                key="k_pais_radio",
            )
            pais_code = {"Chile": "1000", "Perú": "2000", "Bolivia": "3000"}[pais_label]
        st.session_state["clarif_pais_code"] = pais_code
        st.session_state["clarif_pais_label"] = pais_label

    # Tienda vs CD
    if flags["tienda_vs_cd"]:
        st.subheader("Tipo de ubicación")
        st.session_state["clarif_excluir_cd"] = st.checkbox(
            "Excluir Centros de Distribución (CD)", value=True, key="k_excluir_cd",
        )

    # Confirmar
    if st.button("✅ Continuar con estas opciones", type="primary", key="btn_continuar_opciones"):
        moneda_sel = st.session_state.get("clarif_moneda")
        d = st.session_state.get("clarif_fecha_desde") if flags["fecha"] else None
        h = st.session_state.get("clarif_fecha_hasta") if flags["fecha"] else None
        if flags["fecha"] and (d is None or h is None):
            st.warning("Falta completar el rango de fechas.")
            st.stop()
    # -------------------- País (tu lógica existente de país explícito)
    pais_code, pais_label = _extraer_pais(pregunta)
    if flags.get("pais"):
        st.subheader("País")
        if not pais_code:
            pais_label = st.radio(
                "¿Para qué país?",
                options=["Chile", "Perú", "Bolivia"],
                horizontal=True,
                key="k_pais_radio",
            )
            pais_code = {"Chile": "1000", "Perú": "2000", "Bolivia": "3000"}[pais_label]
        st.session_state["clarif_pais_code"] = pais_code
        st.session_state["clarif_pais_label"] = pais_label

    # -------------------- Tienda vs CD
    if flags["tienda_vs_cd"]:
        st.subheader("Tipo de ubicación")
        st.session_state["clarif_excluir_cd"] = st.checkbox(
            "Excluir Centros de Distribución (CD)",
            value=True,
            key="k_excluir_cd",
        )

    # -------------------- Confirmar
    if st.button("✅ Continuar con estas opciones", type="primary", key="btn_continuar_opciones"):
        moneda_sel = st.session_state.get("clarif_moneda")   # lista o None
        d = st.session_state.get("clarif_fecha_desde") if flags["fecha"] else None
        h = st.session_state.get("clarif_fecha_hasta") if flags["fecha"] else None
        if flags["fecha"] and (d is None or h is None):
            st.warning("Falta completar el rango de fechas.")
            st.stop()

        rango = (d, h) if flags["fecha"] else None
        excluir_cd = st.session_state.get("clarif_excluir_cd") if flags["tienda_vs_cd"] else None

        # País elegido en UI (si aplica)
        pais_code_ui = st.session_state.get("clarif_pais_code") if flags.get("pais") else None
        pais_label_ui = st.session_state.get("clarif_pais_label") if flags.get("pais") else None

        # Construir la pregunta enriquecida (tu función actual acepta un valor; convertimos lista->texto)
        moneda_txt = ", ".join(moneda_sel) if isinstance(moneda_sel, (list, tuple, set)) else moneda_sel
        pregunta_enriquecida = _inyectar_aclaraciones_en_pregunta(pregunta, moneda_txt, rango, excluir_cd)

        # Si hay país, explícitalo para que el modelo aplique SOCIEDAD_CO
        if pais_code_ui and pais_label_ui:
            pregunta_enriquecida += f" para {pais_label_ui} (SOCIEDAD_CO={pais_code_ui})"

        # Limpiar estados
        for k in ["clarif_moneda","clarif_fecha_desde","clarif_fecha_hasta",
                  "clarif_excluir_cd","clarif_pais_code","clarif_pais_label"]:
            st.session_state.pop(k, None)

        return pregunta_enriquecida

    # Si aún no confirma, detenemos el flujo principal y la app se re-renderiza
    st.stop()

        rango = (d, h) if flags["fecha"] else None
        excluir_cd = st.session_state.get("clarif_excluir_cd") if flags["tienda_vs_cd"] else None

        # ← NUEVO: país
        pais_code = st.session_state.get("clarif_pais_code") if flags["pais"] else None
        pais_label = st.session_state.get("clarif_pais_label") if flags["pais"] else None

        pregunta_enriquecida = _inyectar_aclaraciones_en_pregunta(pregunta, moneda, rango, excluir_cd)

        # Si hay país, explícitalo para que el modelo aplique la regla de SOCIEDAD_CO
        if pais_code and pais_label:
            pregunta_enriquecida += f" para {pais_label} (SOCIEDAD_CO={pais_code})"

        # Limpia estados
        for k in ["clarif_moneda","clarif_fecha_desde","clarif_fecha_hasta","clarif_excluir_cd","clarif_pais_code","clarif_pais_label"]:
            st.session_state.pop(k, None)

        return pregunta_enriquecida

    # Si aún no confirma, detenemos el flujo principal y la app se re-renderiza
    st.stop()


# ENTRADA DEL USUARIO
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")
# --- NUEVO: si el usuario ya escribió algo antes y estamos en el rerun del botón,
# recupera la pregunta que guardamos en session_state
if not pregunta and st.session_state.get("pending_question"):
    pregunta = st.session_state["pending_question"]
# Inicializar para evitar NameError si aún no hay pregunta
sql_query = None
resultado = ""
guardar_en_cache_pending = None

if pregunta:
    # Guarda siempre la última pregunta mientras dure la desambiguación
    st.session_state["pending_question"] = pregunta

    # ⬇️ NUEVO: pedir aclaraciones si hace falta
    pregunta_clara = manejar_aclaracion(pregunta)
    if pregunta_clara:
        # Reemplaza y limpia
        pregunta = pregunta_clara
        st.session_state["pending_question"] = pregunta  # opcional: mantén enriquecida
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

# 6) Ejecutar SQL (soporta múltiples SELECT separados por ';') — SOLO si hay SQL
if pregunta and isinstance(sql_query, str) and sql_query.strip():
    try:
        if not es_consulta_segura(sql_query):
            st.error("❌ Consulta peligrosa bloqueada.")
            resultado = "Consulta bloqueada"
        else:
            conn = connect_db()
            if conn is None:
                st.info("🔌 Sin conexión a MySQL: se muestra solo la consulta generada.")
                st.code(sql_query, language="sql")
                resultado = "Sin conexión a MySQL"
            else:
                queries = split_queries(sql_query)
                dfs_mostrados = 0

                for idx, q in enumerate(queries, start=1):
                    if not es_consulta_segura(q):
                        st.warning(f"⚠️ Subconsulta {idx} bloqueada por seguridad.")
                        continue

                    df_sub = ejecutar_select(conn, q)
                    if df_sub is not None:
                        df_sub = aplicar_formato_monetario(df_sub)  # ⬅️ AÑADIR ESTA LÍNEA
                        dfs_mostrados += 1
                        st.subheader(f"Resultado {idx}")
                        st.dataframe(df_sub, use_container_width=True)
                        try:
                            xlsx_bytes = make_excel_download_bytes(df_sub, sheet_name=f"Resultado_{idx}")
                            st.download_button(
                                label="⬇️ Descargar en Excel",
                                data=xlsx_bytes,
                                file_name=f"resultado_{idx}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"dl_{idx}",
                            )
                        except Exception as e:
                            st.warning(f"No se pudo generar la descarga del Resultado {idx}: {e}")

                        if dfs_mostrados == 1:
                            actualizar_contexto(df_sub)

                conn.close()
                resultado = ("Consulta ejecutada sin resultados tabulares."
                             if dfs_mostrados == 0 else
                             f"Se mostraron {dfs_mostrados} resultado(s).")
    except Exception as e:
        resultado = f"❌ Error ejecutando SQL: {e}"
else:
    # Al cargar sin pregunta, no muestres nada
    resultado = ""


# ✅ 7) Guardar conversación SOLO si hay datos válidos
if sql_query:
    st.session_state["conversacion"].append({
        "pregunta": pregunta,
        "respuesta": resultado,
        "sql": sql_query,
        "cache": guardar_en_cache_pending
    })
    # Limpia la pregunta pendiente si ya no la necesitas
    st.session_state.pop("pending_question", None)




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

# MOSTRAR HISTORIAL PREVIO (EXCLUYENDO LA ÚLTIMA PREGUNTA)
if st.session_state["conversacion"]:
    st.markdown("## ⌛ Historial de preguntas anteriores")

    # Limpia entradas viejas que hayan quedado sin pregunta o sin sql
    st.session_state["conversacion"] = [
        it for it in st.session_state["conversacion"]
        if it and it.get("pregunta") and it.get("sql")
    ]

    for i, item in enumerate(reversed(st.session_state["conversacion"][:-1])):
        pregunta_hist = item.get("pregunta", "—")
        sql_hist = item.get("sql")

        if not sql_hist:
            # Si por algún motivo sigue sin SQL, sáltalo
            continue

        with st.expander(f"💬 {pregunta_hist}", expanded=False):
            st.markdown("**Consulta SQL Generada:**")
            st.code(sql_hist, language="sql")

            st.markdown("**📊 Resultado:**")
            try:
                if es_consulta_segura(sql_hist):
                    conn = connect_db()
                    if conn is None:
                        st.warning("Sin conexión a MySQL para recrear el resultado.")
                    else:
                        # Soporta múltiples SELECT separados por ';'
                        for idx, q in enumerate(split_queries(sql_hist), start=1):
                            if not es_consulta_segura(q):
                                st.warning(f"⚠️ Subconsulta {idx} bloqueada por seguridad.")
                                continue

                            df_hist = ejecutar_select(conn, q)
                            if df_hist is not None:
                                df_hist = aplicar_formato_monetario(df_hist)  # ⬅️ AÑADIR
                                st.subheader(f"Resultado {idx}")
                                st.dataframe(df_hist, hide_index=True, use_container_width=True)

                                # Descarga a Excel por resultado
                                try:
                                    xlsx_hist = make_excel_download_bytes(df_hist, sheet_name=f"Historial_{idx}")
                                    st.download_button(
                                        label="⬇️ Descargar en Excel",
                                        data=xlsx_hist,
                                        file_name=f"resultado_hist_{i}_{idx}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key=f"dl_hist_{i}_{idx}"
                                    )
                                except Exception as e:
                                    st.warning(f"No se pudo generar el Excel: {e}")
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

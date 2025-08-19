# -*- coding: utf-8 -*-
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

# ==== Estilos adicionales seguros (solo CSS/HTML) ====
st.markdown(
    """
<style>
/* Contenedor más angosto y centrado */
.block-container { max-width: 1100px; padding-top: .75rem; }
/* Fuente */
html, body, [class*="css"] { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
/* Chips/píldoras */
.pill {
  display:inline-block; padding:4px 10px; border-radius:999px;
  border:1px solid #2b3340; background:#0e1116; color:#aab3c5; font-size:12px; margin-right:6px;
}
.pill b { color:#e2e8f0; }
/* Bloque de código */
.stCode { border-radius: 12px !important; border: 1px solid #1f2530; }
/* Tablas hover */
.dataframe tbody tr:hover { background: rgba(96,165,250,.08); }
</style>
    """,
    unsafe_allow_html=True,
)

st.image("assets/logo_neurovia.png", width=180)
st.title(":brain: Asistente Inteligente de Intanis Ventas Retail")
import requests
import io
# --- AYUDA: "Qué puedo preguntarte" ------------------------------------------
_HELP_TRIGGERS_RE = re.compile(
    r"\b(qu[eé]\s+puedo\s+preguntarte|ayuda|qu[eé]\s+sabes\s+hacer|help)\b",
    re.IGNORECASE
)
def _sanear_puntos_y_comas(sql: str) -> str:
    """
    Arregla ';' mal ubicados en una única sentencia SQL:
      - '; AND'  -> ' AND'
      - '; GROUP/ORDER/LIMIT' -> ' GROUP/ORDER/LIMIT'
      - '; WHERE' -> ' WHERE'
      - ';;' -> ';'
      - Elimina ';' intermedios que no estén al final de la sentencia.
    No cambia el contenido lógico del SQL.
    """
    if not sql or not isinstance(sql, str):
        return sql

    # Normaliza espacios
    s = sql

    # 1) Casos típicos: '; AND ...'
    s = re.sub(r";\s+(?=AND\b)", " ", s, flags=re.IGNORECASE)

    # 2) '; GROUP BY / ORDER BY / LIMIT'
    s = re.sub(r";\s+(?=(GROUP\s+BY|ORDER\s+BY|LIMIT)\b)", " ", s, flags=re.IGNORECASE)

    # 3) '; WHERE'
    s = re.sub(r";\s+(?=WHERE\b)", " ", s, flags=re.IGNORECASE)

    # 4) Doble punto y coma ';;' -> ';'
    s = re.sub(r";\s*;+", ";", s)

    # 5) Si quedó algún ';' en medio antes del final de la sentencia,
    #    quítalo (conserva el ';' final si existe)
    #    - separa por salto de línea para no romper formateo
    lines = s.splitlines()
    for i, line in enumerate(lines):
        if i < len(lines) - 1:
            # elimina ';' al final de líneas intermedias
            lines[i] = re.sub(r";\s*$", "", line)
    s = "\n".join(lines)

    # 6) Limpieza de espacios redundantes
    s = re.sub(r"\s+\n", "\n", s).strip()

    return s
def normalizar_importe_sql(sql: str) -> str:
    """Reemplaza referencias a IMPORTE por INGRESOS (la tabla no tiene IMPORTE)."""
    if not sql or not isinstance(sql, str):
        return sql
    s = sql
    # SUM/AVG/MIN/MAX sobre IMPORTE
    s = re.sub(r"(?i)\bSUM\s*\(\s*IMPORTE\s*\)", "SUM(INGRESOS)", s)
    s = re.sub(r"(?i)\bAVG\s*\(\s*IMPORTE\s*\)", "AVG(INGRESOS)", s)
    s = re.sub(r"(?i)\bMIN\s*\(\s*IMPORTE\s*\)", "MIN(INGRESOS)", s)
    s = re.sub(r"(?i)\bMAX\s*\(\s*IMPORTE\s*\)", "MAX(INGRESOS)", s)
    # IMPORTE "a secas"
    s = re.sub(r"(?i)\bIMPORTE\b", "INGRESOS", s)
    return s
def asegurar_exclusion_servicios(sql: str) -> str:
    if not sql or not isinstance(sql, str):
        return sql
    s = sql
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'FLETE%'", 
               "UPPER(DESC_ARTICULO) NOT LIKE 'FLETE%'", s)
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'DESPACHO A DOMICILIO'",
               "UPPER(DESC_ARTICULO) NOT LIKE 'DESPACHO A DOMICILIO'", s)
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'%BOLSA%'", 
               "UPPER(DESC_ARTICULO) NOT LIKE '%BOLSA%'", s)
    return s

def render_help_capacidades():
    st.markdown("## 🤖 ¿Qué puedes preguntarme?")
    st.markdown("""
Puedo entender preguntas de **ventas retail** y generar la **consulta SQL** adecuada sobre el **tablon `VENTAS`**, aplicando automáticamente filtros y reglas del negocio que ya definiste.

---

### 🧭 Tipos de preguntas frecuentes
- **Ventas / Ingresos / Costos**  
  *“Ventas en USD en Chile del último mes”*, *“Costos por canal en 2025”*, *“Ingresos por tienda en Perú”*.
- **Top / Ranking / Mejor vendido**  
  *“Artículo más vendido por unidades en Bolivia”*, *“Top 10 por país en USD”*.  
  ➤ Para “más vendido”, **agrupo por `DESC_ARTICULO`** y uso **`UNIDADES > 0`**.
- **Filtros por atributos descriptivos**  
  *“Ventas de la marca Levi’s en Plaza Vespucio”*, *“Canal de esa tienda”*, *“Productos mujer”*.  
  ➤ Siempre uso **campos `DESC_*`** (no códigos) y **`LIKE '%valor%'`**.
- **País y moneda**  
  *“Comparación por país del trimestre”*, *“Ventas en CLP para Chile”*.  
  ➤ País se mapea con `SOCIEDAD_CO → Chile/Perú/Bolivia`.  
  ➤ Si comparas **varios países**, usa **USD**. Para **un solo país**, **USD** + moneda local.
- **Género**  
  *“Jeans de mujer”*, *“Camisas hombre”*, *“Unisex”*.  
  ➤ `DESC_GENERO LIKE '%woman%' | '%men%' | '%unisex%'`.
- **Promociones**  
  *“Ventas con promoción”*, *“Detalle de la promo”*.  
  ➤ Código: `PROMO` — Descripción: `D_PROMO` (no nulos ⇒ vendió con promo).
- **Tiendas / Canales / Clientes**  
  *“¿Cuántas tiendas hay?”*, *“¿De qué canal es esa tienda?”*, *“Clientes distintos del mes”*.  
  ➤ `COUNT(DISTINCT ...)` y **para “¿de qué canal?”** uso `SELECT DISTINCT DESC_CANAL ...`.
- **Listados y conteos por país**  
  *“Lista de países disponibles”*, *“¿Cuántos países hay?”*.  
  ➤ Entrego:  
    1) `SELECT COUNT(DISTINCT SOCIEDAD_CO) ...`  
    2) `SELECT DISTINCT CASE SOCIEDAD_CO ... END AS PAIS ...`

---

### 🧱 Reglas clave que aplico (del prompt)
# MÁRGENES (cuando no existe columna MARGEN)
- Si piden **margen** en monto (margen bruto / utilidad), calcula:
  (SUM(INGRESOS) - SUM(COSTOS))  AS MARGEN
  -# MONTOS / INGRESOS (no existe columna IMPORTE)
- La tabla NO tiene IMPORTE. Si aparecen términos como: "importe", "monto", "total", "facturación",
  "revenue", "sales", "ventas (monto)", "valor", "ticket", usa SIEMPRE la columna INGRESOS.
  Ejemplos:
  • SUM(INGRESOS) AS TOTAL_MONTO
  • AVG(INGRESOS) AS TICKET_PROMEDIO
- Para GM% (margen %): ((SUM(INGRESOS) - SUM(COSTOS)) / NULLIF(SUM(INGRESOS),0)) * 100 AS GM_PORCENTAJE.
- No uses la columna MARGEN ni IMPORTE (no existen).
- Solo filtra MONEDA cuando la métrica sea monetaria; para conteos/unidades no agregues MONEDA.

- Si piden **GM% / margen porcentual** (señales: "gm%", "%", "porcentaje"):
  ((SUM(INGRESOS) - SUM(COSTOS)) / NULLIF(SUM(INGRESOS),0)) * 100  AS GM_PORCENTAJE
- No uses la columna MARGEN (no existe). Siempre deriva a partir de INGRESOS y COSTOS.
- Agrupa por las dimensiones solicitadas y aplica MONEDA solo si se trata de métricas monetarias (para GM% se puede filtrar MONEDA=’USD’ para normalizar).
-Importe no existe, siempre considerar ingresos. 
- **Campos descriptivos:** “tienda, marca, canal, producto…” ⇒ `DESC_*` (no `COD_*`).  
- **Fechas:** `FECHA_DOCUMENTO` **formato `YYYYMMDD` sin guiones**.  
- **Unidades negativas:** son devoluciones ⇒ si se habla de ventas o “baratos”, **`UNIDADES > 0`**.  
- **Centros de distribución:**  
  - Nombres como *“CENTRO DE DISTRIBUCIÓN LEVI”* y *“CENTRO DISTRIBUCION LEVIS PERU”* **no** cuentan como tienda.  
  - Puedes **excluir/incluir** CD según lo pidas.
- **Artículo vs Servicio:**  
  - `COD_TIPOARTICULO = 'MODE'` ⇒ artículo  
  - `COD_TIPOARTICULO = 'DIEN'` o `DESC_ARTICULO = 'DESPACHO A DOMICILIO'` ⇒ servicio  
- **Precio de venta:** considera **ingreso unitario (cantidad = 1)**.  
- **Tipos (DESC_TIPO):** *Back Patches, Jeans, Sweaters…*  
  - Se usan **como filtro** (`DESC_TIPO LIKE '%valor%'`)  
  - En rankings/listados se **muestra `DESC_ARTICULO`**, salvo que pidas “por tipo”.
- **Líneas (`DESC_LINEA`):** *Accesorios, Bottoms, Tops, Customization, Insumos*.
- **País (SOCIEDAD_CO):**  
  - `1000→Chile`, `2000→Perú`, `3000→Bolivia`.  
  - Para “por país”, agrupo por `SOCIEDAD_CO` y decodifico con `CASE`.

---

### 📝 Ejemplos listos para usar
- *“Ventas en USD por país entre 20250101 y 20250131”*  
- *“¿De qué canal es esa tienda?”* (devuelve **1 fila** con `SELECT DISTINCT DESC_CANAL`)  
- *“Top 5 artículos más vendidos (unidades > 0) en Perú en 2025”*  
- *“Total de tiendas (excluyendo centros de distribución)”*  
- *“Ventas de mujer en Jeans Levi’s en Chile este mes”*  
- *“Lista de países disponibles”* (devuelve el conteo + listado)

---

### 🔍 Recuerda
- Siempre puedo mostrarte la **consulta SQL** que generé.  
- Puedo exportar resultados a **Excel** desde la app.  
- Uso `LIKE '%valor%'` para evitar pérdidas por capitalización/acentos.
""")
# --- Equivalencias español -> inglés para DESC_TIPO (no toca tus listas existentes) ---
EQUIV_DESC_TIPO_ES_EN = {
    # prendas comunes
    r"\bchaqueta(s)?\b": "Jackets",
    r"\bcamisa(s)?\b": "Shirts",
    r"\bpolera(s)?\b": "Shirts",        # si usas "T-shirts" cambia aquí
    r"\bpoler[óo]n(es)?\b": "Sweatshirts",
    r"\bjean(s)?\b": "Jeans",
    r"\bpantal[oó]n(es)?\b": "Pants",
    r"\bsu[eé]ter(es)?\b": "Sweaters",
    r"\bparche(s)?\b": "Patches",
    r"\bbot[oó]n(es)?\b": "Buttons",
    r"\bp[ií]n(es)?\b": "Pines",
    r"\bbolsa(s)?\b": "Packing Bags",   # en tu VENTAS es "Packing Bags"
    # puedes seguir agregando sin romper nada:
    # r"\bgorro(s)?\b": "Knits",
    # r"\btab(s)?\b": "Tabs",
}
def forzar_excluir_centros_distribucion(sql: str) -> str:
    """
    Si el SQL tiene FROM VENTAS y no excluye CDs, agrega el filtro para que
    no aparezcan en resultados de tiendas.
    Evita crear un segundo WHERE; si ya hay WHERE, inserta un AND antes de GROUP/ORDER/LIMIT.
    """
    if not sql:
        return sql

    s_low = sql.lower()
    if "from ventas" not in s_low:
        return sql

    # Ya excluye CDs explícita o implícitamente
    if re.search(r"(?i)desc_tienda\s+not\s+like\s+'%centro%distrib%'", sql):
        return sql

    # Regex para detectar sección que sigue a la cláusula WHERE
    tail_re = re.compile(r"(?i)\b(group\s+by|order\s+by|limit)\b")

    if re.search(r"(?i)\bwhere\b", sql):
        # Ya hay WHERE: insertar "AND ..." antes de GROUP/ORDER/LIMIT (si existen) o al final
        m = tail_re.search(sql)
        if m:
            idx = m.start()
            before = sql[:idx].rstrip()
            after = sql[idx:]
            # si el before ya tiene WHERE, agregamos AND al final del predicado
            return before + " AND DESC_TIENDA NOT LIKE '%CENTRO%DISTRIB%' " + after
        else:
            # no hay GROUP/ORDER/LIMIT; agregamos al final del SQL
            return sql.rstrip() + " AND DESC_TIENDA NOT LIKE '%CENTRO%DISTRIB%'"
    else:
        # No hay WHERE: lo insertamos antes de GROUP/ORDER/LIMIT o al final
        m = tail_re.search(sql)
        if m:
            idx = m.start()
            before = sql[:idx].rstrip()
            after = sql[idx:]
            return before + " WHERE DESC_TIENDA NOT LIKE '%CENTRO%DISTRIB%' " + after
        else:
            return sql.rstrip() + " WHERE DESC_TIENDA NOT LIKE '%CENTRO%DISTRIB%'"
# ==== Helpers de inyección y fixers de marca/artículo ====

# Inserta un predicado respetando si ya hay WHERE o no
def _inyectar_predicado_where(sql: str, predicado: str) -> str:
    if not sql:
        return sql
    tail_re = re.compile(r"(?i)\b(group\s+by|order\s+by|limit)\b")
    if re.search(r"(?i)\bwhere\b", sql):
        m = tail_re.search(sql)
        if m:
            i = m.start()
            return sql[:i].rstrip() + " AND " + predicado + " " + sql[i:]
        return sql.rstrip() + " AND " + predicado
    else:
        m = tail_re.search(sql)
        if m:
            i = m.start()
            return sql[:i].rstrip() + " WHERE " + predicado + " " + sql[i:]
        return sql.rstrip() + " WHERE " + predicado

# --- MARCAS: LEVI'S / DOCKERS deben ir SIEMPRE por DESC_MARCA ---
_LEVIS_RE   = re.compile(r"\b(levis|levi[’'´`]?s|levi|lv)\b", re.I)
_DOCKERS_RE = re.compile(r"\b(dockers|dk)\b", re.I)

def forzar_marca_en_sql_si_corresponde(pregunta: str, sql: str) -> str:
    if not sql or not pregunta:
        return sql
    s_low = sql.lower()

    def _ya_filtra_marca(s: str, marca: str) -> bool:
        return re.search(rf"(?i)desc_marca\s+like\s+'%{re.escape(marca)}%'", s) is not None

    # LEVI'S / LEVIS / LEVI / LV → DESC_MARCA LIKE '%LEVI%'
    if _LEVIS_RE.search(pregunta) and not _ya_filtra_marca(s_low, "LEVI"):
        sql = _inyectar_predicado_where(sql, "DESC_MARCA LIKE '%LEVI%'")

    # DOCKERS / DK → DESC_MARCA LIKE '%DOCKERS%'
    if _DOCKERS_RE.search(pregunta) and not _ya_filtra_marca(s_low, "DOCKERS"):
        sql = _inyectar_predicado_where(sql, "DESC_MARCA LIKE '%DOCKERS%'")

    return sql

# --- ARTÍCULO sin bolsas/servicios ni PACKING BAGS ---
_PRODUCTO_INTENT_RE  = re.compile(r"\b(producto[s]?|art[ií]culo[s]?|sku[s]?)\b", re.I)
_EXPLICT_SERVICE_RE  = re.compile(r"\b(bolsa[s]?|packing\s*bags?|flete[s]?|despacho(?:\s+a)?\s+domicilio|servicio[s]?)\b", re.I)

def _es_intencion_producto(pregunta: str) -> bool:
    if not pregunta:
        return False
    # Solo si NO piden explícitamente bolsas/packing/fletes/despachos
    return bool(_PRODUCTO_INTENT_RE.search(pregunta)) and not bool(_EXPLICT_SERVICE_RE.search(pregunta))

# ==== Helpers de inyección y fixers de marca/artículo ====

# Inserta un predicado respetando si ya hay WHERE o no
def _inyectar_predicado_where(sql: str, predicado: str) -> str:
    if not sql:
        return sql
    tail_re = re.compile(r"(?i)\b(group\s+by|order\s+by|limit)\b")
    if re.search(r"(?i)\bwhere\b", sql):
        m = tail_re.search(sql)
        if m:
            i = m.start()
            return sql[:i].rstrip() + " AND " + predicado + " " + sql[i:]
        return sql.rstrip() + " AND " + predicado
    else:
        m = tail_re.search(sql)
        if m:
            i = m.start()
            return sql[:i].rstrip() + " WHERE " + predicado + " " + sql[i:]
        return sql.rstrip() + " WHERE " + predicado

# --- MARCAS: LEVI'S / DOCKERS deben ir SIEMPRE por DESC_MARCA ---
_LEVIS_RE   = re.compile(r"\b(levis|levi[’'´`]?s|levi|lv)\b", re.I)
_DOCKERS_RE = re.compile(r"\b(dockers|dk)\b", re.I)

def forzar_marca_en_sql_si_corresponde(pregunta: str, sql: str) -> str:
    if not sql or not pregunta:
        return sql
    s_low = sql.lower()

    def _ya_filtra_marca(s: str, marca: str) -> bool:
        return re.search(rf"(?i)desc_marca\s+like\s+'%{re.escape(marca)}%'", s) is not None

    # LEVI'S / LEVIS / LEVI / LV → DESC_MARCA LIKE '%LEVI%'
    if _LEVIS_RE.search(pregunta) and not _ya_filtra_marca(s_low, "LEVI"):
        sql = _inyectar_predicado_where(sql, "DESC_MARCA LIKE '%LEVI%'")

    # DOCKERS / DK → DESC_MARCA LIKE '%DOCKERS%'
    if _DOCKERS_RE.search(pregunta) and not _ya_filtra_marca(s_low, "DOCKERS"):
        sql = _inyectar_predicado_where(sql, "DESC_MARCA LIKE '%DOCKERS%'")

    return sql

# --- ARTÍCULO sin bolsas/servicios ni PACKING BAGS ---
_PRODUCTO_INTENT_RE  = re.compile(r"\b(producto[s]?|art[ií]culo[s]?|sku[s]?)\b", re.I)
_EXPLICT_SERVICE_RE  = re.compile(r"\b(bolsa[s]?|packing\s*bags?|flete[s]?|despacho(?:\s+a)?\s+domicilio|servicio[s]?)\b", re.I)

def _es_intencion_producto(pregunta: str) -> bool:
    if not pregunta:
        return False
    # Solo si NO piden explícitamente bolsas/packing/fletes/despachos
    return bool(_PRODUCTO_INTENT_RE.search(pregunta)) and not bool(_EXPLICT_SERVICE_RE.search(pregunta))

def forzar_articulo_y_excluir_bolsas(pregunta: str, sql: str) -> str:
    """
    Si la intención es producto/artículo:
      - Asegura COD_TIPOARTICULO='MODE'
      - Corrige si el LLM puso DESC_TIPOARTICULO='PACKING BAGS'
      - Excluye por DESC_ARTICULO: %BOLSA%, DESPACHO A DOMICILIO, FLETE%
      - Excluye PACKING BAGS por DESC_TIPO
    """
    if not sql or not _es_intencion_producto(pregunta):
        return sql

    s = sql

    # Corrige errores de TIPOARTICULO mal puesto
    s = re.sub(
        r"(?i)\bdesc_tipoarticulo\s*=\s*'?\s*packing\s*bags\s*'?",
        "DESC_TIPOARTICULO = 'MODE'",
        s,
    )

    # Asegurar artículo
    if _tipoarticulo = 'mode'" not in s.lower():
        s = _inyectar_predicado_where(s, "COD_TIPOARTICULO = 'MODE'")

    # Excluir por descripción (bolsas/servicios)
    excl_desc = [
        "UPPER(DESC_ARTICULO) NOT LIKE '%BOLSA%'",
        "UPPER(DESC_ARTICULO) NOT LIKE 'DESPACHO A DOMICILIO'",
        "UPPER(DESC_ARTICULO) NOT LIKE 'FLETE%'",
    ]
    for pred in excl_desc:
        if pred.lower() not in s.lower():
            s = _inyectar_predicado_where(s, pred)

    # Excluir PACKING BAGS por tipo (es DESC_TIPO)
    tipo_excl = "(DESC_TIPO IS NULL OR UPPER(DESC_TIPO) <> 'PACKING BAGS')"
    if "upper(desc_tipo) <> 'packing bags'" not in s.lower():
        s = _inyectar_predicado_where(s, tipo_excl)

    return s


def mapear_desc_tipo_es_en(texto: str) -> str:
    """
    Reemplaza, de forma segura (con \b límites de palabra), términos españoles por
    su equivalente canónico en inglés para que el filtro DESC_TIPO LIKE funcione.
    No modifica nada más del texto.
    """
    if not isinstance(texto, str) or not texto:
        return texto
    t = texto
    for patron, canonico in EQUIV_DESC_TIPO_ES_EN.items():
        t = re.sub(patron, canonico, t, flags=re.IGNORECASE)
    return t

def make_excel_download_bytes(df: pd.DataFrame, sheet_name="Datos"):
    """Devuelve bytes de un .xlsx con el dataframe."""
    bio = io.BytesIO()
    # Usa xlsxwriter si está disponible; pandas cae a openpyxl si no.
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.getvalue()
# ---- Valores de DESC_TIPO que queremos reconocer en texto ----
_TIPOS_VALIDOS = [
    "Back Patches","Buttons","Jackets","Jeans","Knits","Packing Bags","Pants",
    "Patches","Pines","Shirts","Sin Tipo","Sweaters","Sweatshirts","Tabs","(Vacías)"
]
# mapa en minúsculas para matching case-insensitive
_TIPOS_SET = {t.lower(): t for t in _TIPOS_VALIDOS}

def _detectar_tipo_en_texto(texto: str) -> str | None:
    # compara en lower y permite coincidencias parciales de palabras
    tx = texto.lower()
    for k, original in _TIPOS_SET.items():
        # coincidencia por palabra o subcadena completa segura
        # (Back Patches y Packing Bags tienen espacio; usamos 'in' con cuidado)
        if re.search(rf"\\b{re.escape(k)}\\b", tx) or k in tx:
            return original
    return None

def _anotar_tipo_en_pregunta(pregunta: str) -> str:
    # Si el último reemplazo fue por ARTÍCULO → no forzar TIPO
    if st.session_state.get("__last_ref_replacement__") == "DESC_ARTICULO":
        return pregunta

    # Detecta TIPO solo en la pregunta ORIGINAL del usuario
    original = st.session_state.get("__last_user_question__", pregunta)
    t = _detectar_tipo_en_texto(original)
    if not t:
        return pregunta

    guia = (f" (Filtrar con DESC_TIPO LIKE '%{t}%'. Considerar UNIDADES > 0 al hablar de ventas.)")
    if re.search(r"(más\\s+vendid[oa]|mas\\s+vendid[oa]|top|ranking|mejor\\s+vendid[oa])", original, re.I):
        guia += (" Mostrar y agrupar por DESC_ARTICULO (no por DESC_TIPO), "
                 "ordenar por SUM(UNIDADES) DESC y usar LIMIT 1 si procede.")
    return pregunta.strip() + guia   


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
    # 7.765.093,83
    return s.replace(",", "X").replace(".", ",").replace("X", ".")
# --- Centros de distribución a excluir (normalizados en MAYÚSCULAS) ---
CD_EXCLUSIONES = {
    "CENTRO DE DISTRIBUCIÓN LEVI",   # con tilde
    "CENTRO DISTRIBUCION LEVI",      # sin tilde
    "CENTRO DISTRIBUCION LEVIS PERU"
}

def es_centro_distribucion(nombre: str) -> bool:
    """True si 'nombre' corresponde a un centro de distribución."""
    if not isinstance(nombre, str):
        return False
    t = nombre.strip().upper()
    # match exacto o por inclusión (por si vienen sufijos/prefijos)
    return any(x == t or x in t for x in CD_EXCLUSIONES)
    
def forzar_distinct_pais_si_corresponde(pregunta, sql_generado):
    if re.search(r"\\bpa[ií]s\\b", pregunta, re.I) and \
       st.session_state.get("__last_ref_replacement__") in ("DESC_TIENDA", "DESC_TIENDA_LIST"):
        if not re.search(r"\\bselect\\s+distinct\\b", sql_generado, re.I):
            return f"SELECT DISTINCT PAIS FROM ({sql_generado}) AS t"
    return sql_generado
    
def aplicar_formato_monetario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formatea columnas monetarias con separador europeo (7.765.093,83) y agrega sufijo de moneda.
    Evita aplicar formato a conteos (COUNT) y totales que no sean de dinero.
    Reglas:
      - Detecta columnas numéricas cuyo NOMBRE sugiera dinero (ingresos, costos, precio, etc.).
      - Excluye columnas de unidades, cantidades, conteos, tiendas, clientes, país, canal, etc.
      - Si todos los valores son enteros (probable COUNT), NO se considera dinero.
      - Si existe columna MONEDA, usa su valor por fila; si no, usa la última moneda elegida si es única.
    """
    if df is None or df.empty:
        return df

    df2 = df.copy()

    # 1) Candidatas por tipo numérico
    numeric_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]

    if not numeric_cols:
        return df2

    # 2) Heurística por nombre de columna
    #    - quitamos "total" genérico del include para no capturar TOTAL_TIENDA(S), TOTAL_CLIENTES, etc.
    #    - permitimos "total" solo si viene acompañado de palabras de dinero (total_ingresos, total_costos, etc.)
    include_pat = re.compile(
        r"(ingres|venta|cost|margen|gm|precio|importe|neto|bruto|valor|ticket|^total_(ingres|venta|cost|margen|gm|precio|importe|neto|bruto|valor|ticket))",
        re.I,
    )
    exclude_pat = re.compile(
        r"(unid|cantidad|count|conteo|nro|numero|tienda|cliente|pais|pa[ií]s|canal|art[ií]culo|articulo|sku)",
        re.I,
    )

    def _series_is_integer_like(s: pd.Series) -> bool:
        """True si todos los valores no nulos son enteros (p.ej. conteos)."""
        vals = s.dropna().astype(float).values
        if vals.size == 0:
            return False
        return np.all(np.mod(vals, 1) == 0)

    money_cols = []
    for c in numeric_cols:
        name = str(c)
        if include_pat.search(name) and not exclude_pat.search(name):
            # Evitar formatear columnas que parecen conteos
            if _series_is_integer_like(df2[c]):
                continue
            money_cols.append(c)

    if not money_cols:
        return df2

    # 3) Determinar sufijo de moneda
    #    - Si existe columna MONEDA, se usa por fila
    #    - Si no, usa la última moneda confirmada si es única
    last = st.session_state.get("clarif_moneda_last")
    single_suffix = None
    if isinstance(last, list) and len(last) == 1:
        single_suffix = last[0]
    elif isinstance(last, str):
        single_suffix = last

    # 4) Aplicar formato
    if "MONEDA" in df2.columns:
        for c in money_cols:
            df2[c] = df2.apply(
                lambda r: f"{_fmt_money(r[c])} {r['MONEDA']}" if pd.notnull(r[c]) else r[c],
                axis=1,
            )
    else:
        for c in money_cols:
            if single_suffix:
                df2[c] = df2[c].map(lambda x: f"{_fmt_money(x)} {single_suffix}" if pd.notnull(x) else x)
            else:
                # sin información de moneda → solo formato numérico
                df2[c] = df2[c].map(lambda x: _fmt_money(x) if pd.notnull(x) else x)

    return df2



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
def _solo_conteo_o_listado_de_paises(texto: str) -> bool:
    patrones = r"(cu[aá]nt[oa]s?\\s+pa[ií]ses|n[uú]mero\\s+de\\s+pa[ií]ses|cantidad\\s+de\\s+pa[ií]ses|" \
               r"(listar|mostrar|muestr[ao])\\s+(los\\s+)?pa[ií]ses|qu[eé]\\s+pa[ií]ses\\b)"
    return bool(re.search(patrones, texto, re.I))

def _extraer_paises(texto: str) -> set[str]:
    """Set de SOCIEDAD_CO presentes explícitamente en el texto (por nombre o código)."""
    codes = set()
    for k, v in _SOC_BY_NAME.items():
        if re.search(rf"\\b{k}\\b", texto, re.I):
            codes.add(v)
    for m in re.findall(r"\\b(1000|2000|3000)\\b", texto):
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
    return bool(re.search(r"\\b(usd|clp|pen|bob|d[oó]lar(?:es)?|pesos?)\\b", texto, re.I))

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
            "❌ No se pudo conectar a MySQL.\\n\\n"
            "Posibles causas: servidor caído, tu IP no está autorizada en cPanel → Remote MySQL, "
            "o límite de conexiones.\\n\\n"
            f"Detalle técnico: {e}"
            + (f"\\n\\nIP detectada: {ip_actual}" if ip_actual else "")
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
    template = """
# CONTEXTO
- Trabajas sobre el tablón único **VENTAS** (no asumas joins externos).
- Devuelve **solo** la consulta **SQL** (MySQL/MariaDB), una sentencia por SELECT, **sin texto extra**.
- La pregunta puede traer **anotaciones** (p. ej. “Filtrar con …”, “usar FECHA_DOCUMENTO entre …”); **obedécelas** literalmente.

# MAPEO DE CAMPOS
- Si se menciona “tienda, cliente, marca, canal, producto/artículo, temporada, calidad…”, usa **DESC_*** (no COD_*), salvo que pidan **“código de …”**.
  - “tienda”→DESC_TIENDA | “marca”→DESC_MARCA | “calidad”→DESC_CALIDAD | “temporada”→DESC_TEMPORADA | “producto/artículo/sku”→DESC_ARTICULO o DESC_SKU según contexto.
- País (SOCIEDAD_CO): 1000=Chile, 2000=Perú, 3000=Bolivia. Para “por país / ranking por país / ¿en qué país…?” usa:
  `CASE SOCIEDAD_CO WHEN '1000' THEN 'Chile' WHEN '2000' THEN 'Perú' WHEN '3000' THEN 'Bolivia' END AS PAIS`.

# DEFINICIONES DE ARTÍCULO VS SERVICIO
- **Artículo** ⇢ `COD_TIPOARTICULO='MODE'`.
- **Servicio / NO Artículo** ⇢ `COD_TIPOARTICULO<>'MODE'` o descripciones de servicio.
- **IMPORTANTE (producto/artículo)**: cuando la intención es **producto/artículo**:
  1) **Restringe** a `COD_TIPOARTICULO='MODE'`.
  2) **Excluye** explícitamente **bolsas/packing** y servicios:
     - `UPPER(DESC_ARTICULO) NOT LIKE '%BOLSA%'`
     - `UPPER(DESC_ARTICULO) NOT LIKE 'DESPACHO A DOMICILIO'`
     - `UPPER(DESC_ARTICULO) NOT LIKE 'FLETE%'`
   - Artículo ⇢ CODE_TIPOARTICULO='MODE'. Servicio ⇢ CODE_TIPOARTICULO<>'MODE'.
- PACKING BAGS es un valor de DESC_TIPO (no de DESC_TIPOARTICULO). 
  Cuando la intención es **producto/artículo**:
  • Forzar COD_TIPOARTICULO='MODE'
  • Excluir bolsas/servicios: 
    UPPER(DESC_ARTICULO) NOT LIKE '%BOLSA%' 
    AND UPPER(DESC_ARTICULO) NOT LIKE 'DESPACHO A DOMICILIO' 
    AND UPPER(DESC_ARTICULO) NOT LIKE 'FLETE%'
  • Excluir PACKING BAGS por tipo: (UPPER(DESC_TIPO) <> 'PACKING BAGS')
- Solo incluir bolsas/packing/fletes/despachos si el usuario lo pide explícitamente.

- Solo **incluye** bolsas/packing/fletes/ despachos/cualquier servicio si el usuario lo pide **explícitamente** (“bolsas”, “packing bags”, “flete”, “despacho”, “servicio”).
- Corrección: **“DESPACHO A DOMICILIO” no es artículo** (trátalo como servicio).
- Marcas:
  - Si la pregunta menciona LEVI’S / LEVIS / LEVI / LV → filtra SIEMPRE por DESC_MARCA (no por tienda/tipo/artículo):
    AND DESC_MARCA LIKE '%LEVI%'
  - Si la pregunta menciona DOCKERS / DK → filtra SIEMPRE por DESC_MARCA:
    AND DESC_MARCA LIKE '%DOCKERS%'
  - No uses DESC_TIENDA/ARTICULO/TIPO para reconocer marcas, salvo que el usuario lo pida explícitamente.

# REGLAS GENERALES
1) **Filtros texto**: en DESC_* usa `LIKE '%valor%'` (case-insensitive); nunca `=`.
2) **Moneda**: filtra por MONEDA **solo** si la métrica es monetaria (INGRESOS, COSTOS, MARGEN, PRECIO, IMPORTE, VALOR, TICKET).  
   - Para conteos/listados no monetarios (tiendas, clientes, unidades), **no** agregues condición de MONEDA.
3) **Unidades negativas**: devoluciones. Si hablan de “vende/ventas/top/más vendido/baratos/caros”, agrega `UNIDADES > 0`.
4) **Fecha**: FECHA_DOCUMENTO es `'YYYYMMDD'` sin guiones. Ej.: `BETWEEN '20250101' AND '20250131'`.
5) **Tiendas**: los **Centros de Distribución** no son tiendas; **exclúyelos**:
   - `DESC_TIENDA NOT IN ('Centro de Distribución LEVI','CENTRO DISTRIBUCION LEVI','CENTRO DISTRIBUCION LEVIS PERU')`.
6) **Conteos**:
   - “¿Cuántas tiendas?” → `COUNT(DISTINCT DESC_TIENDA)` + exclusión de CD.
   - “¿Cuántos canales?” → `COUNT(DISTINCT DESC_CANAL)`.
   - “¿Cuántos clientes?” → `COUNT(DISTINCT NOMBRE_CLIENTE)`.
7) **Canal de una tienda** (“¿de qué canal es esa tienda?”): `SELECT DISTINCT DESC_CANAL ...` (evita duplicados).
8) **Top / ranking**:
   - Artículos: agrupa por **DESC_ARTICULO** (y extras si los piden: COD_MODELO, TALLA, LARGO, COD_COLOR), con `UNIDADES > 0`,
     `ORDER BY SUM(UNIDADES) DESC` y `LIMIT k` si corresponde.  
     **Cuando la intención es artículo/producto, respeta la exclusión de bolsas/fletes/despachos (ver arriba).**
   - “Ventas por tipo” (resumen) ⇢ agrupa por **DESC_TIPO**. En listados comunes **no** muestres DESC_TIPO como descripción.
9) **País**:
   - Si piden comparación/ranking por país, agrupa por SOCIEDAD_CO y expón **PAIS** con el CASE.
10) **Promociones**:
   - Descripción: `D_PROMO`; código: `PROMO`. Se considera **con promoción** cuando ambos **no** son nulos.  
   - “Promociones” también puede significar `PROMO <> '0.00'` (si aplica en tus datos).
11) **Documentos**: `TIPO_DOC='BO'` significa **boleta**.
12) **Precio de venta**: si preguntan por “precio de venta” considera el **ingreso unitario cuando cantidad = 1** (usa DISTINCT si aplica).
13) **Líneas/Dominios**: “Accesorios”, “Bottoms”, “Tops”, “Customization”, “Insumos” corresponden a **DESC_LINEA**.
14) **Pronombres/Contexto**:
   - “este/ese artículo/producto” ⇒ `DESC_ARTICULO LIKE '%valor%'` y `UNIDADES > 0` (no uses DESC_TIPO).
   - “estos/esos artículos/productos” cuando venga una lista anotada ⇒ `DESC_ARTICULO IN (...)` y `UNIDADES > 0`.
15) **Fechas en filtros**: siempre en **YYYYMMDD** sin guiones.
16) **Seguridad de sintaxis**: no coloques `;` antes de `AND/WHERE/GROUP/ORDER/LIMIT`; el `;` solo puede ir al **final** de la sentencia.

# PATRONES COMUNES
- “¿Qué producto/artículo se vende más por tienda?”  
  - Agrupa por `DESC_TIENDA, DESC_ARTICULO`, filtra `UNIDADES > 0`, **excluye CD**, **excluye bolsas/fletes/despachos** (ver sección de producto/artículo).  
  - Ordena por `DESC_TIENDA, SUM(UNIDADES) DESC`. (Si piden “el más vendido por tienda” en una sola fila por tienda, puedes usar subconsulta con ranking o LIMIT por tienda si lo soporta tu versión; si no, devolver listado ordenado por tienda es aceptable.)
- “Lista de países disponibles / cuántos países hay”  
  - No filtres por MONEDA. Devuelve dos SELECTs:
    1) `SELECT COUNT(DISTINCT SOCIEDAD_CO) AS TOTAL_PAISES FROM VENTAS;`
    2) `SELECT DISTINCT CASE SOCIEDAD_CO ... END AS PAIS FROM VENTAS;`

# CHECKLIST (AUTO-VERIFICACIÓN, NO MOSTRAR)
- ¿La intención dice **producto/artículo**? → ¿agregué `DESC_TIPOARTICULO='MODE'` y **excluí** `%BOLSA%`, `DESPACHO A DOMICILIO`, `FLETE%` y `DESC_TIPO<>'PACKING BAGS'`?
- ¿La métrica es **monetaria**? Si **no**, ¿evité filtrar por MONEDA?
- ¿Excluí **Centros de Distribución** cuando corresponde a tiendas?
- ¿Apliqué `UNIDADES > 0` cuando hablan de ventas/top/más vendido/precios?
- ¿Usé `LIKE '%valor%'` en DESC_* y fechas en **YYYYMMDD**?
- Si es por país, ¿expones **PAIS** con el CASE?
- ¿Evité `; AND` / `; WHERE` en medio de la sentencia?

# INSTRUCCIÓN FINAL
Genera **solo** el SQL limpio y optimizado para MySQL/MariaDB, obedeciendo cualquier anotación presente en la pregunta.

Pregunta: {pregunta}
""",
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
    "ese país": "SOCIEDAD_CO",
    "ese accesorio":"DESC_ARTICULO",
    "ese bottom":"DESC_ARTICULO",
    "ese top":"DESC_ARTICULO",
    "ese customization":"DESC_ARTICULO",
    "ese insumo":"DESC_ARTICULO",
    # Prioriza ARTICULO sobre TIPO
    "ese pin": ["DESC_ARTICULO", "DESC_TIPO"],
    "ese producto": ["DESC_ARTICULO", "DESC_TIPO"],
    "ese artículo": ["DESC_ARTICULO", "DESC_TIPO"],
    "ese articulo": ["DESC_ARTICULO", "DESC_TIPO"],
}
referencias.update({
    "este artículo": "DESC_ARTICULO",
    "este articulo": "DESC_ARTICULO",
    "este producto": "DESC_ARTICULO",
})
referencias.update({
    "ese tipo": "DESC_TIPO",
    "ese categoria de tipo": "DESC_TIPO",
    "esa tienda": "DESC_TIENDA",
    "estas tiendas": "DESC_TIENDA",   # nuevo (plural con “estas”)
    "esas tiendas": "DESC_TIENDA",    # nuevo (plural con “esas”)
})

def aplicar_contexto(pregunta: str) -> str:
    pregunta_mod = pregunta
    lower_q = pregunta.lower()
    st.session_state["__last_ref_replacement__"] = None
    # --- manejo especial: "esos/estos artículos|productos|pines" -> usar lista previa ---
    if ("esos articulos" in lower_q or "estos articulos" in lower_q or
        "esos artículos" in lower_q or "estos artículos" in lower_q or
        "esos productos" in lower_q or "estos productos" in lower_q or
        "esos pines" in lower_q or "estos pines" in lower_q) and \
        "DESC_ARTICULO_LIST" in st.session_state.get("contexto", {}):
        
        lista = st.session_state["contexto"]["DESC_ARTICULO_LIST"]
        # Escapa comillas simples para SQL
        lista_sql = "', '".join(s.replace("'", "''") for s in lista)

        # Anotación para guiar al generador SQL:
        guia_in = (" (Filtrar con DESC_ARTICULO IN ('" + lista_sql + "')" 
                   " y UNIDADES > 0. No uses DESC_TIPO para este filtro.)")

        # Normaliza las frases al texto guía
        pregunta_mod = re.sub(r"(es[eo]s)\s+art[ií]culos", "los artículos indicados", pregunta_mod, flags=re.I)
        pregunta_mod = re.sub(r"(es[eo]s)\s+productos", "los artículos indicados", pregunta_mod, flags=re.I)
        pregunta_mod = re.sub(r"(es[eo]s)\s+pines", "los artículos indicados", pregunta_mod, flags=re.I)

        pregunta_mod += guia_in
        st.session_state["__last_ref_replacement__"] = "DESC_ARTICULO_LIST"
        st.session_state["__last_ref_value__"] = lista
        return pregunta_mod
    # --- manejo especial: "esas/estas tiendas" -> usar lista previa ---
    if ("esas tiendas" in lower_q or "estas tiendas" in lower_q) and \
       "DESC_TIENDA_LIST" in st.session_state.get("contexto", {}):
        lista = st.session_state["contexto"]["DESC_TIENDA_LIST"]
        # escapa comillas simples
        lista_sql = "', '".join(s.replace("'", "''") for s in lista)
        # Anotación guía para el generador SQL
        guia_in = f" (Filtrar con DESC_TIENDA IN ('{lista_sql}'))"
        pregunta_mod = re.sub(r"(esas|estas)\\s+tiendas", "las tiendas indicadas", pregunta_mod, flags=re.I)
        pregunta_mod += guia_in
        # marca que el reemplazo fue por tiendas (para saltarse aclaraciones)
        st.session_state["__last_ref_replacement__"] = "DESC_TIENDA_LIST"
        st.session_state["__last_ref_value__"] = lista

    # --- tu lógica existente de referencias singulares ---
    for ref, campos in referencias.items():
        if ref in lower_q:
            for campo in campos if isinstance(campos, list) else [campos]:
                if campo in st.session_state.get("contexto", {}):
                    val_original = st.session_state["contexto"][campo]
                    val_escapado = re.escape(val_original)
                    pregunta_mod = re.sub(ref, val_escapado, pregunta_mod, flags=re.IGNORECASE)
                    st.session_state["__last_ref_replacement__"] = campo
                    st.session_state["__last_ref_value__"] = val_original
                    break

    return pregunta_mod


campos_contexto = [
    "DESC_TIENDA","DESC_CANAL","DESC_MARCA","DESC_ARTICULO",
    "DESC_GENERO","NOMBRE_CLIENTE","SOCIEDAD_CO","DESC_TIPO"
]



def actualizar_contexto(df: pd.DataFrame):
    alias = {
        "DESC_TIENDA": ["DESC_TIENDA", "TIENDA", "Tienda"],
        "DESC_CANAL": ["DESC_CANAL", "CANAL", "Canal"],
        "DESC_MARCA": ["DESC_MARCA", "MARCA", "Marca"],
        "DESC_ARTICULO": ["DESC_ARTICULO", "ARTICULO", "Artículo", "Articulo"],
        "DESC_GENERO": ["DESC_GENERO", "GENERO", "Género", "Genero"],
        "DESC_TIPO": ["DESC_TIPO", "TIPO", "Tipo"],
        "NOMBRE_CLIENTE": ["NOMBRE_CLIENTE", "CLIENTE", "Cliente"],
        "SOCIEDAD_CO": ["PAIS", "PAISES", "Pais","Paises","Países","País"]
    }
    # Guardar una LISTA de tiendas (excluyendo CDs)
    if "DESC_TIENDA" in df.columns:
        tiendas = (
            df["DESC_TIENDA"]
            .dropna()
            .astype(str)
            .map(str.strip)
            .unique()
            .tolist()
        )
        tiendas = [t for t in tiendas if t and not es_centro_distribucion(t)]
        if tiendas:
            st.session_state.setdefault("contexto", {})["DESC_TIENDA_LIST"] = tiendas
            # Guardar LISTA de artículos (únicos) para poder referirnos a "esos/estos artículos"
    if "DESC_ARTICULO" in df.columns:
        articulos = (
            df["DESC_ARTICULO"]
            .dropna()
            .astype(str)
            .map(str.strip)
            .unique()
            .tolist()
        )
        # Evita strings vacíos y servicios si quisieras (opcional)
        articulos = [a for a in articulos if a]
        if articulos:
            # Limita a 50 para evitar queries gigantes
            st.session_state.setdefault("contexto", {})["DESC_ARTICULO_LIST"] = articulos[:50]
    articulo_capturado = False

    for canonico, posibles in alias.items():
        for col in posibles:
            if col in df.columns and not df[col].isnull().all():
                valor = str(df[col].dropna().iloc[0]).strip()
                if not valor:
                    continue
                if canonico == "DESC_TIENDA" and es_centro_distribucion(valor):
                    continue
                st.session_state.setdefault("contexto", {})[canonico] = valor
                if canonico == "DESC_ARTICULO":
                    articulo_capturado = True
                break

    # Si guardamos un ARTICULO, limpiamos TIPO para que no interfiera
    if articulo_capturado and "DESC_TIPO" in st.session_state["contexto"]:
        st.session_state["contexto"].pop("DESC_TIPO", None)
def forzar_distinct_canal_si_corresponde(pregunta, sql_generado):
    """
    Si la pregunta pide el canal de una tienda (ej: '¿de qué canal es esa tienda?'),
    envuelve el SQL en un SELECT DISTINCT para evitar filas duplicadas.
    Si la pregunta pide el pais de una tienda (ej: '¿de qué pais es esa tienda?'),
    envuelve el SQL en un SELECT DISTINCT para evitar filas duplicadas.
    """
    if re.search(r"\\bcanal(es)?\\b", pregunta, flags=re.IGNORECASE) and \
       re.search(r"\\btienda\\b|esa tienda", pregunta, flags=re.IGNORECASE):
        # Evitar doble DISTINCT si ya viene correcto
        if not re.search(r"\\bselect\\s+distinct\\b", sql_generado, flags=re.IGNORECASE):
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
# --- Reglas ligeras para validar reutilización del SQL cacheado ---

_NEED_DESC_TIENDA_RE = re.compile(r"\b(descripci[oó]n|por\s+descripci[oó]n|tienda[s]?\s+por)\b", re.I)
_NEED_PAIS_RE = re.compile(r"\bpa[ií]s(es)?\b", re.I)
_NEED_CANAL_RE = re.compile(r"\bcanal(es)?\b", re.I)
_NEED_GROUPING_RE = re.compile(r"\bpor\b", re.I)  # "por descripción/país/canal" suele implicar agrupación

def _sql_has_col(sql: str, col_patterns: list[str]) -> bool:
    s = sql.lower()
    return any(p.lower() in s for p in col_patterns)

def _sql_has_pais(sql: str) -> bool:
    s = sql.lower()
    # aceptar cualquiera de estas señales de país: CASE SOCIEDAD_CO ... AS PAIS, alias PAIS, o la propia SOCIEDAD_CO
    return (" as pais" in s) or ("case sociedad_co" in s) or ("sociedad_co" in s)

# === Helpers para validar si podemos reutilizar un SQL de la caché ===
_NEED_DESC_TIENDA_RE = re.compile(r"\b(descripci[oó]n|por\s+descripci[oó]n|tienda[s]?\s+por)\b", re.I)
_NEED_PAIS_RE = re.compile(r"\bpa[ií]s(es)?\b", re.I)
_NEED_CANAL_RE = re.compile(r"\bcanal(es)?\b", re.I)
_NEED_GROUPING_RE = re.compile(r"\bpor\b", re.I)  # "por descripción/país/canal" suele implicar agrupación

def _sql_has_col(sql: str, col_patterns):
    s = sql.lower()
    return any(p.lower() in s for p in col_patterns)

def _sql_has_pais(sql: str) -> bool:
    s = sql.lower()
    # Acepta cualquiera de estas señales de país: alias PAIS, CASE SOCIEDAD_CO ... AS PAIS, o la propia SOCIEDAD_CO
    return (" as pais" in s) or ("case sociedad_co" in s) or ("sociedad_co" in s)

def _should_reuse_cached_sql(pregunta: str, sql: str) -> bool:
    """
    Devuelve True si el SQL cacheado satisface la estructura que la nueva pregunta sugiere.
    Reglas mínimas para no romper nada:
    - Si la pregunta trae "por ..." ⇒ debe haber GROUP BY.
    - Si pide descripción/tienda ⇒ debe aparecer DESC_TIENDA en el SQL.
    - Si pide país ⇒ el SQL debe tener PAIS (o CASE SOCIEDAD_CO) o SOCIEDAD_CO.
    - Si pide canal ⇒ debe aparecer DESC_CANAL en el SQL.
    """
    q = (pregunta or "")
    s = (sql or "").lower()

    needs_grouping = bool(_NEED_GROUPING_RE.search(q))
    if needs_grouping and ("group by" not in s):
        return False

    needs_desc = bool(_NEED_DESC_TIENDA_RE.search(q))
    if needs_desc and not _sql_has_col(s, ["desc_tienda"]):
        return False

    needs_pais = bool(_NEED_PAIS_RE.search(q))
    if needs_pais and not _sql_has_pais(s):
        return False

    needs_canal = bool(_NEED_CANAL_RE.search(q))
    if needs_canal and not _sql_has_col(s, ["desc_canal"]):
        return False

    return True

def buscar_sql_en_cache(pregunta_nueva, umbral_similitud=0.94):
    # Calcula embedding de la nueva pregunta
    embedding_nuevo = obtener_embedding(pregunta_nueva)
    if embedding_nuevo is None:
        return None

    try:
        conn = connect_db()
        if conn is None:
            return None  # sin conexión -> no hay cache

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT pregunta, embedding, sql_generado FROM semantic_cache")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        vec_nuevo = np.array(embedding_nuevo)
        n_nuevo = np.linalg.norm(vec_nuevo)
        if n_nuevo == 0:
            return None

        mejor_sql, mejor_sim = None, -1.0
        for row in rows:
            vec_guardado = np.array(json.loads(row["embedding"]))
            n_guardado = np.linalg.norm(vec_guardado)
            if n_guardado == 0:
                continue

            similitud = float(np.dot(vec_nuevo, vec_guardado) / (n_nuevo * n_guardado))
            if similitud > mejor_sim:
                mejor_sim, mejor_sql = similitud, row["sql_generado"]

        if mejor_sim >= umbral_similitud:
            # ✅ Validación adicional: solo reutiliza si el SQL satisface la intención de la nueva pregunta
            if _should_reuse_cached_sql(pregunta_nueva, mejor_sql):
                return mejor_sql
            # Si no cumple, no reutilizamos caché y forzamos nueva generación
            return None

        return None

    except Exception as e:
        st.warning(f"❌ Error buscando en cache: {e}")
        return None


# ==== DESAMBIGUACIÓN: detectores y UI ========================================

import datetime as _dt
from typing import Optional, Tuple

# Palabras que delatan montos:
_MONEY_KEYS = (
    r"(venta|vende|ventas|ingreso|ingresos|margen|utilidad|gm|revenue|sales|facturaci[oó]n|"
    r"precio|precios|car[oa]s?|barat[oa]s?|cost[eo]s?|ticket\\s*promedio|valor(?:es)?)"
)
    # Palabras que delatan pais:
# --- País: detectores ----------------------------------------
 # --- Normalizadores de SQL (margen, importe) y exclusión de servicios --------
import re

def normalizar_margen_sql(sql: str) -> str:
    """
    Tu tablón no tiene columna MARGEN. Convierte expresiones con MARGEN a
    (INGRESOS - COSTOS). Además soporta el típico GM% = SUM(MARGEN)/SUM(INGRESOS)*100.
    """
    if not sql or not isinstance(sql, str):
        return sql
    s = sql

    # 1) GM%: SUM(MARGEN)/SUM(INGRESOS) * 100  -> ((SUM(INGRESOS)-SUM(COSTOS))/NULLIF(SUM(INGRESOS),0))*100
    s = re.sub(
        r"SUM\s*\(\s*MARGEN\s*\)\s*/\s*SUM\s*\(\s*INGRESOS\s*\)\s*\*\s*100",
        r"((SUM(INGRESOS) - SUM(COSTOS)) / NULLIF(SUM(INGRESOS),0)) * 100",
        s,
        flags=re.IGNORECASE
    )

    # 2) SUM(MARGEN) -> SUM(INGRESOS) - SUM(COSTOS)
    s = re.sub(
        r"\bSUM\s*\(\s*MARGEN\s*\)",
        r"SUM(INGRESOS) - SUM(COSTOS)",
        s,
        flags=re.IGNORECASE
    )

    # 3) AVG(MARGEN) (raro, pero por si acaso) -> (SUM(INGRESOS)-SUM(COSTOS))/NULLIF(COUNT(*),0)
    s = re.sub(
        r"\bAVG\s*\(\s*MARGEN\s*\)",
        r"(SUM(INGRESOS) - SUM(COSTOS)) / NULLIF(COUNT(*),0)",
        s,
        flags=re.IGNORECASE
    )

    return s


def normalizar_importe_sql(sql: str) -> str:
    """
    Si aparece IMPORTE (que no existe en tu tabla), cámbialo por INGRESOS.
    No toca alias ni comentarios, sólo el identificador.
    """
    if not sql or not isinstance(sql, str):
        return sql
    # Reemplazo seguro por palabra completa
    return re.sub(r"\bIMPORTE\b", "INGRESOS", sql, flags=re.IGNORECASE)


def _insertar_predicado(sql: str, predicado: str) -> str:
    """
    Inserta un predicado de forma segura: si hay WHERE, agrega 'AND ...'
    antes de GROUP/ORDER/LIMIT; si no hay WHERE, crea 'WHERE ...'.
    """
    if not sql or "from ventas" not in sql.lower():
        return sql

    tail_re = re.compile(r"(?i)\b(group\s+by|order\s+by|limit)\b")
    if re.search(r"(?i)\bwhere\b", sql):
        m = tail_re.search(sql)
        if m:
            idx = m.start()
            return sql[:idx].rstrip() + f" AND {predicado} " + sql[idx:]
        return sql.rstrip() + f" AND {predicado}"
    else:
        m = tail_re.search(sql)
        if m:
            idx = m.start()
            return sql[:idx].rstrip() + f" WHERE {predicado} " + sql[idx:]
        return sql.rstrip() + f" WHERE {predicado}"


def asegurar_exclusion_servicios(sql: str) -> str:
    if not sql or not isinstance(sql, str):
        return sql
    s = sql
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'FLETE%'", 
               "UPPER(DESC_ARTICULO) NOT LIKE 'FLETE%'", s)
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'DESPACHO A DOMICILIO'",
               "UPPER(DESC_ARTICULO) NOT LIKE 'DESPACHO A DOMICILIO'", s)
    s = re.sub(r"(?i)UPPER\(\s*DESC_ARTICULO\s*\)\s+LIKE\s+'%BOLSA%'", 
               "UPPER(DESC_ARTICULO) NOT LIKE '%BOLSA%'", s)
    return s


def _habla_de_pais(texto: str) -> bool:
    # ¿se menciona la noción de país en general?
    return bool(re.search(_COUNTRY_REGEX, texto, re.I))

def _tiene_pais(texto: str) -> bool:
    # ¿viene un país explícito (por nombre o código SOCIEDAD_CO)?
    return bool(re.search(r"\\b(1000|2000|3000|chile|per[uú]|bolivia)\\b", texto, re.I))

def _agregacion_por_pais(texto: str) -> bool:
    # intenciones de ranking/agrupación/comparación por país
    patrones = (
        r"(por\\s+pa[ií]s|seg[uú]n\\s+pa[ií]s|ranking\\s+de\\s+pa[ií]ses|"
        r"top\\s+\\d+\\s+pa[ií]ses|comparaci[oó]n\\s+por\\s+pa[ií]s|"
        r"cu[aá]l(?:es)?\\s+es\\s+el\\s+pa[ií]s\\s+que\\s+(?:m[aá]s|menos))"
    )
    return bool(re.search(patrones, texto, re.I))
# Palabras que delatan fechas explícitas:
_DATE_KEYS = r"(hoy|ayer|semana|mes|año|anio|últim|ultimo|desde|hasta|entre|rango|202\\d|20\\d\\d|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"



# Mapa utilitario para SOCIEDAD_CO
_PAIS_MAP = {"chile": "1000", "peru": "2000", "perú": "2000", "bolivia": "3000"}

def _extraer_pais(texto: str):
    """Devuelve (codigo, etiqueta) si aparece un país en el texto, si no (None, None)."""
    m = re.search(r"\\b(chile|per[uú]|bolivia)\\b", texto, re.I)
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
    return bool(re.search(r"\\btienda(s)?\\b", texto, re.I))


def _menciona_cd(texto: str) -> bool:
    # si el usuario ya dijo explícitamente CD o ese nombre, no preguntamos
    return bool(
        re.search(r"centro\\s+de\\s+distribuci[oó]n", texto, re.I)
        or re.search(r"\\bcentro\\s+distribucion\\b", texto, re.I)
        or re.search(r"\\bCD\\b", texto, re.I)
    )
# --- País: detectores (definir una sola vez) -----------------
_COUNTRY_REGEX = r"\\b(chile|per[uú]|bolivia|pa[ií]s(?:es)?)\\b"

def _habla_de_pais(texto: str) -> bool:
    # ¿se menciona la noción de país en general?
    return bool(re.search(_COUNTRY_REGEX, texto, re.I))

def _tiene_pais(texto: str) -> bool:
    # ¿hay un país explícito por nombre o por código SOCIEDAD_CO?
    return bool(re.search(r"\\b(1000|2000|3000|chile|per[uú]|bolivia)\\b", texto, re.I))

def _agregacion_por_pais(texto: str) -> bool:
    # intenciones de ranking/agrupación/comparación por país
    patrones = (
        r"(por\\s+pa[ií]s|seg[uú]n\\s+pa[ií]s|ranking\\s+de\\s+pa[ií]ses|"
        r"top\\s+\\d+\\s+pa[ií]ses|comparaci[oó]n\\s+por\\s+pa[ií]s|"
        r"cu[aá]l(?:es)?\\s+es\\s+el\\s+pa[ií]s\\s+que\\s+(?:m[aá]s|menos)|"
        r"en\\s+qu[eé]\\s+pa[ií]s\\s+se\\s+vend(?:e|i[óo]a)|"   # se vende / se vendió / se vendía
        r"en\\s+qu[eé]\\s+pa[ií]s\\s+se\\s+vende\\s+(?:m[aá]s|menos))"
    )
    return bool(re.search(patrones, texto, re.I))

def _extraer_pais(texto: str):
    """Devuelve (codigo, etiqueta) si aparece un país en el texto; si no, (None, None)."""
    m = re.search(r"\\b(chile|per[uú]|bolivia)\\b", texto, re.I)
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
    conteo_o_listado = _solo_conteo_o_listado_de_paises(texto)

    # NUEVO: referencia a lista de tiendas capturada
    ref_tiendas = (("esas tiendas" in texto.lower()) or ("estas tiendas" in texto.lower())) and \
                  ("DESC_TIENDA_LIST" in st.session_state.get("contexto", {}))

    return {
        "moneda": (_pide_montos(texto) and not _tiene_moneda(texto)),
        # NO pedir país si es conteo/listado… y TAMPOCO si refiere a "esas tiendas"
        "pais":   (habla_pais and not tiene_pais and not agrega_pais and not conteo_o_listado and not ref_tiendas),
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
        if re.search(rf"\\b{k}\\b", texto, re.I):
            codes.add(v)
    for m in re.findall(r"\\b(1000|2000|3000)\\b", texto):
        codes.add(m)
    return codes

def _sugerir_monedas(paises: set[str], es_agrupado_por_pais: bool) -> list[str]:
    if es_agrupado_por_pais or len(paises) != 1:
        return ["USD"]
    unico = next(iter(paises))
    return ["USD", _LOCAL_CURRENCY_BY_SOC.get(unico, "USD")]

def _tiene_moneda(texto: str) -> bool:
    # Detecta USD/CLP/PEN/BOB
    return bool(re.search(r"\\b(usd|clp|pen|bob|d[oó]lar(?:es)?|pesos?)\\b", texto, re.I))


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
        monedas_permitidas = ["USD", "CLP", "PEN", "BOB"]   # <-- NUEVO
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

    # País (sólo si no viene claro y no es ranking por país)
    pais_code, pais_label = _extraer_pais(pregunta)
    if flags.get("pais"):
        st.subheader("País")
        if not pais_code:
            pais_label = st.radio(
                "¿Para qué país?",
                options=["Chile", "Perú", "Bolivia"],
                horizontal=True,
                key=f"k_pais_radio_{abs(hash(pregunta))%100000}",
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

    # Confirmar (¡sólo un botón con esta key!)
    if st.button("✅ Continuar con estas opciones", type="primary", key="btn_continuar_opciones"):
        moneda_sel = st.session_state.get("clarif_moneda")
        d = st.session_state.get("clarif_fecha_desde") if flags["fecha"] else None
        h = st.session_state.get("clarif_fecha_hasta") if flags["fecha"] else None
        if flags["fecha"] and (d is None or h is None):
            st.warning("Falta completar el rango de fechas.")
            st.stop()

        rango = (d, h) if flags["fecha"] else None
        excluir_cd = st.session_state.get("clarif_excluir_cd") if flags["tienda_vs_cd"] else None
        pais_code_ui = st.session_state.get("clarif_pais_code") if flags.get("pais") else None
        pais_label_ui = st.session_state.get("clarif_pais_label") if flags.get("pais") else None

        moneda_txt = ", ".join(moneda_sel) if isinstance(moneda_sel, (list, tuple, set)) else moneda_sel
        pregunta_enriquecida = _inyectar_aclaraciones_en_pregunta(pregunta, moneda_txt, rango, excluir_cd)

        if pais_code_ui and pais_label_ui:
            pregunta_enriquecida += f" para {pais_label_ui} (SOCIEDAD_CO={pais_code_ui})"

        # Guarda la(s) moneda(s) confirmada(s) para formateo posterior
        st.session_state["clarif_moneda_last"] = moneda_sel

        # Limpieza
        for k in ["clarif_moneda","clarif_fecha_desde","clarif_fecha_hasta",
                  "clarif_excluir_cd","clarif_pais_code","clarif_pais_label"]:
            st.session_state.pop(k, None)

        return pregunta_enriquecida

    st.stop()



# ENTRADA DEL USUARIO
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")
if not pregunta and st.session_state.get("pending_question"):
    pregunta = st.session_state["pending_question"]
# --- Si el usuario pide "Qué puedo preguntarte", mostrar ayuda y no generar SQL ---
if pregunta and _HELP_TRIGGERS_RE.search(pregunta or ""):
    with st.chat_message("assistant"):
        render_help_capacidades()
    # Limpio la pending_question para que no reprocese la ayuda al siguiente render
    st.session_state.pop("pending_question", None)
    st.stop()


sql_query = None
resultado = ""
guardar_en_cache_pending = None

if pregunta:
    # Guarda siempre la última pregunta mientras dure la desambiguación
    st.session_state["pending_question"] = pregunta

    # Guarda el texto ORIGINAL del usuario (antes de cualquier sustitución)
    st.session_state["__last_user_question__"] = pregunta
    st.session_state["__last_ref_replacement__"] = None  # reset de tracking opcional

    # Normaliza DESC_TIPO desde español a inglés SOLO para la pregunta que viaja al prompt
    pregunta = mapear_desc_tipo_es_en(pregunta)

    # Desambiguación (moneda/fechas/etc.)
    pregunta_clara = manejar_aclaracion(pregunta)
    if pregunta_clara:
        # Reemplaza y limpia
        pregunta = pregunta_clara
        st.session_state["pending_question"] = pregunta


    with st.chat_message("user"):
        st.markdown(pregunta)

    # 1) Cache semántica
    sql_query = buscar_sql_en_cache(pregunta)
    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        # 2) Derivar género
        if re.search(r"\\b(mujer|femenin[oa])\\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Woman"
        elif re.search(r"\\b(hombre|masculin[oa]|varón|varon|caballero)\\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Men"
        elif re.search(r"\\bunisex\\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Unisex"

        # 3) Aplicar contexto y guía de TIPO  ⬇⬇⬇ TODO este bloque re-indentar aquí
        pregunta_con_contexto = aplicar_contexto(pregunta)

        # Si el pronombre se resolvió a ARTÍCULO, forzar DESC_ARTICULO y no usar DESC_TIPO
        if st.session_state.get("__last_ref_replacement__") == "DESC_ARTICULO":
            art_val = st.session_state.get("__last_ref_value__", "")
            if art_val:
                pregunta_con_contexto += (
                    f" Usa estrictamente DESC_ARTICULO LIKE '%{art_val}%' (case-insensitive) "
                    f"y UNIDADES > 0. No uses DESC_TIPO para este filtro."
                )

        # Añade guía de TIPO solo si aplica
        pregunta_con_contexto = _anotar_tipo_en_pregunta(pregunta_con_contexto)

        # Caso “meta países”
        if _solo_conteo_o_listado_de_paises(pregunta_con_contexto):
            pregunta_con_contexto += (
                " Nota: Si la pregunta es 'cuántos países hay' o 'lista/descripcion de países', "
                "no filtres por MONEDA y devuelve DOS SELECTs: "
                "(1) SELECT COUNT(DISTINCT SOCIEDAD_CO) AS TOTAL_PAISES FROM VENTAS; "
                "(2) SELECT DISTINCT CASE SOCIEDAD_CO WHEN '1000' THEN 'Chile' "
                "WHEN '2000' THEN 'Perú' WHEN '3000' THEN 'Bolivia' END AS PAIS FROM VENTAS;"
            )

        prompt_text = sql_prompt.format(pregunta=pregunta_con_contexto)
        sql_query = llm.predict(prompt_text).replace("```sql", "").replace("```", "").strip()

        # 4) Forzar DISTINCT si corresponde
        sql_query = forzar_distinct_canal_si_corresponde(pregunta_con_contexto, sql_query)
        # 4b) Forzar exclusión de Centros de Distribución
        sql_query = forzar_excluir_centros_distribucion(sql_query)
        # ➜ NUEVO: fuerza marca cuando la pregunta menciona LEVI'S/DOCKERS
        sql_query = forzar_marca_en_sql_si_corresponde(pregunta_con_contexto, sql_query)

        # ➜ NUEVO: si la intención es producto/artículo, restringe a MODE y excluye bolsas/fletes/despachos/PACKING BAGS
        sql_query = forzar_articulo_y_excluir_bolsas(pregunta_con_contexto, sql_query)
        # 4c) 🔧 Saneador de ';' mal puestos
        # 👇 nuevos seguros
        sql_query = normalizar_margen_sql(sql_query)
        sql_query = normalizar_importe_sql(sql_query)
        sql_query = asegurar_exclusion_servicios(sql_query)  # opcional
        sql_query = _sanear_puntos_y_comas(sql_query)
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
                     # Guarda en caché automáticamente si hubo al menos 1 dataframe mostrado
                        try:
                            if guardar_en_cache_pending and dfs_mostrados > 0:
                                guardar_en_cache(pregunta, sql_query, guardar_en_cache_pending)
                        except Exception as _e:
                            st.warning(f"⚠️ No se pudo guardar en cache automáticamente: {_e}")


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




# ====== Construcción de CHIPS (píldoras) para el bloque SQL ======
chips = []
_pregunta_ctx = locals().get("pregunta_con_contexto", pregunta or "")
# Moneda confirmada/sugerida
mon_last = st.session_state.get("clarif_moneda_last")
if isinstance(mon_last, list) and mon_last:
    chips.append("Moneda: " + ", ".join(mon_last))
elif isinstance(mon_last, str) and mon_last:
    chips.append("Moneda: " + mon_last)
# Rango YYYYMMDD si fue inyectado
m = re.search(r"FECHA_DOCUMENTO entre (\\d{8}) y (\\d{8})", _pregunta_ctx, re.I)
if m:
    chips.append(f"Rango: {m.group(1)} → {m.group(2)}")
# Inclusión/Exclusión de CD
if "excluyendo el Centro de Distribución" in _pregunta_ctx:
    chips.append("CDs excluidos")
elif "incluyendo el Centro de Distribución" in _pregunta_ctx:
    chips.append("CDs incluidos")
# País (si quedó seteado por el aclarador)
if "clarif_pais_label" in st.session_state:
    chips.append("País: " + str(st.session_state["clarif_pais_label"]))
# Lista de tiendas capturadas
tiendas_list = st.session_state.get("contexto", {}).get("DESC_TIENDA_LIST")
if isinstance(tiendas_list, list) and tiendas_list:
    chips.append(f"Tiendas: {len(tiendas_list)} seleccionada(s)")



# MOSTRAR TODAS LAS INTERACCIONES COMO CHAT
# UI MEJORADA EN STREAMLIT
# (Esta parte va justo al final del archivo app.py, reemplazando el bloque de visualización actual de interacciones)

if pregunta and sql_query is not None:
    with st.chat_message("user"):
        st.markdown("### 🤖 Pregunta actual:")
        st.markdown(f"> {pregunta}")

    with st.chat_message("assistant"):
        st.markdown("### 🔍 Consulta SQL Generada:")
        if chips:
            st.markdown(" ".join([f"<span class='pill'>{c}</span>" for c in chips]), unsafe_allow_html=True)
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

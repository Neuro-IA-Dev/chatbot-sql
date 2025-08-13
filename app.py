# -*- coding: utf-8 -*-
import os
import io
import re
import json
import numpy as np
import pandas as pd
import requests
import streamlit as st
import mysql.connector
from openai import OpenAI
from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
import datetime as _dt

# ==================== CONFIG STREAMLIT ====================
st.set_page_config(page_title="Asistente Inteligente de Ventas Retail", page_icon="🧠")
st.markdown(
    """
<style>
.block-container { max-width: 1100px; padding-top: .75rem; }
html, body, [class*="css"] { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
.pill { display:inline-block; padding:4px 10px; border-radius:999px; border:1px solid #2b3340; background:#0e1116; color:#aab3c5; font-size:12px; margin-right:6px; }
.pill b { color:#e2e8f0; }
.stCode { border-radius: 12px !important; border: 1px solid #1f2530; }
.dataframe tbody tr:hover { background: rgba(96,165,250,.08); }
</style>
    """,
    unsafe_allow_html=True,
)
st.image("assets/logo_neurovia.png", width=180)
st.title(":brain: Asistente Inteligente de Intanis Ventas Retail")

# ==================== CONSTANTES/REGEX ====================
_COUNTRY_REGEX = r"\b(chile|per[uú]|bolivia|pa[ií]s(?:es)?)\b"

_HELP_TRIGGERS_RE = re.compile(
    r"\b(qu[eé]\s+puedo\s+preguntarte|ayuda|qu[eé]\s+sabes\s+hacer|help)\b",
    re.IGNORECASE
)

# ==================== UTILIDADES ====================
def obtener_ip_publica():
    try:
        return requests.get("https://api.ipify.org", timeout=2).text
    except Exception:
        return None

ip_actual = obtener_ip_publica()
if ip_actual:
    st.caption(f"IP saliente detectada: {ip_actual} — agrégala en cPanel → Remote MySQL (Add Access Host).")
else:
    st.caption("No se pudo detectar la IP saliente (timeout/red).")

def _fmt_money(v: float) -> str:
    if pd.isna(v): return ""
    s = f"{float(v):,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def make_excel_download_bytes(df: pd.DataFrame, sheet_name="Datos"):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    bio.seek(0)
    return bio.getvalue()

def split_queries(sql_text: str) -> list[str]:
    return [q.strip() for q in sql_text.strip().split(";") if q.strip()]

def ejecutar_select(conn, query: str) -> pd.DataFrame | None:
    q = query.strip()
    if not q.lower().startswith("select"):
        cur = conn.cursor(buffered=True)
        cur.execute(q)
        conn.commit()
        cur.close()
        return None
    df = pd.read_sql_query(q, conn)
    if "FECHA_DOCUMENTO" in df.columns:
        df["FECHA_DOCUMENTO"] = pd.to_datetime(
            df["FECHA_DOCUMENTO"].astype(str), format="%Y%m%d", errors="coerce"
        ).dt.strftime("%d/%m/%Y")
    return df

def connect_db():
    try:
        return mysql.connector.connect(
            host="s1355.use1.mysecurecloudhost.com",
            port=3306,
            user="domolabs_RedTabBot_USER",
            password="Pa$$w0rd_123",
            database="domolabs_RedTabBot_DB",
            connection_timeout=8,
        )
    except mysql.connector.Error as e:
        st.error(
            "❌ No se pudo conectar a MySQL.\n\n"
            "Posibles causas: servidor caído, tu IP no está autorizada en cPanel → Remote MySQL, "
            "o límite de conexiones.\n\n"
            f"Detalle técnico: {e}" + (f"\n\nIP detectada: {ip_actual}" if ip_actual else "")
        )
        return None

def es_consulta_segura(sql):
    if not sql or not isinstance(sql, str): return False
    sql_l = sql.lower()
    peligrosos = ["drop", "delete", "truncate", "alter", "update", "insert", "--", "/*", "grant", "revoke"]
    return not any(c in sql_l for c in peligrosos)

# ==================== REGLAS DE NEGOCIO ====================
CD_EXCLUSIONES = {
    "CENTRO DE DISTRIBUCIÓN LEVI",
    "CENTRO DISTRIBUCION LEVI",
    "CENTRO DISTRIBUCION LEVIS PERU"
}

def es_centro_distribucion(nombre: str) -> bool:
    if not isinstance(nombre, str): return False
    t = nombre.strip().upper()
    return any(x == t or x in t for x in CD_EXCLUSIONES)

def aplicar_formato_monetario(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return df
    df2 = df.copy()
    numeric_cols = [c for c in df2.columns if pd.api.types.is_numeric_dtype(df2[c])]
    include_pat = re.compile(r"(ingres|venta|cost|margen|gm|precio|importe|neto|bruto|total|valor|ticket)", re.I)
    exclude_pat = re.compile(r"(unid|cantidad|count|nro|numero)", re.I)
    money_cols = [c for c in numeric_cols if include_pat.search(c) and not exclude_pat.search(c)]
    if not money_cols: return df2

    last = st.session_state.get("clarif_moneda_last")
    single_suffix = last[0] if isinstance(last, list) and len(last) == 1 else (last if isinstance(last, str) else None)

    if "MONEDA" in df2.columns:
        for c in money_cols:
            df2[c] = df2.apply(lambda r: f"{_fmt_money(r[c])} {r['MONEDA']}" if pd.notnull(r[c]) else r[c], axis=1)
    else:
        for c in money_cols:
            if single_suffix:
                df2[c] = df2[c].map(lambda x: f"{_fmt_money(x)} {single_suffix}" if pd.notnull(x) else x)
            else:
                df2[c] = df2[c].map(lambda x: _fmt_money(x) if pd.notnull(x) else x)
    return df2

# ======== Normalización de DESC_TIPO (es→en) para guiar al LLM =========
EQUIV_DESC_TIPO_ES_EN = {
    r"\bchaqueta(s)?\b": "Jackets",
    r"\bcamisa(s)?\b": "Shirts",
    r"\bpolera(s)?\b": "Shirts",
    r"\bpoler[óo]n(es)?\b": "Sweatshirts",
    r"\bjean(s)?\b": "Jeans",
    r"\bpantal[oó]n(es)?\b": "Pants",
    r"\bsu[eé]ter(es)?\b": "Sweaters",
    r"\bparche(s)?\b": "Patches",
    r"\bbot[oó]n(es)?\b": "Buttons",
    r"\bp[ií]n(es)?\b": "Pines",
    r"\bbolsa(s)?\b": "Packing Bags",
}
def mapear_desc_tipo_es_en(texto: str) -> str:
    if not isinstance(texto, str) or not texto: return texto
    t = texto
    for patron, canonico in EQUIV_DESC_TIPO_ES_EN.items():
        t = re.sub(patron, canonico, t, flags=re.IGNORECASE)
    return t

_TIPOS_VALIDOS = [
    "Back Patches","Buttons","Jackets","Jeans","Knits","Packing Bags","Pants",
    "Patches","Pines","Shirts","Sin Tipo","Sweaters","Sweatshirts","Tabs","(Vacías)"
]
_TIPOS_SET = {t.lower(): t for t in _TIPOS_VALIDOS}

def _detectar_tipo_en_texto(texto: str) -> str | None:
    tx = (texto or "").lower()
    for k, original in _TIPOS_SET.items():
        if re.search(rf"\b{re.escape(k)}\b", tx) or k in tx:
            return original
    return None

def _anotar_tipo_en_pregunta(pregunta: str) -> str:
    if st.session_state.get("__last_ref_replacement__") == "DESC_ARTICULO":
        return pregunta
    original = st.session_state.get("__last_user_question__", pregunta)
    t = _detectar_tipo_en_texto(original)
    if not t: return pregunta
    guia = f" (Filtrar con DESC_TIPO LIKE '%{t}%'. Considerar UNIDADES > 0 al hablar de ventas.)"
    if re.search(r"(más\s+vendid[oa]|mas\s+vendid[oa]|top|ranking|mejor\s+vendid[oa])", original, re.I):
        guia += (" Mostrar y agrupar por DESC_ARTICULO (no por DESC_TIPO), "
                 "ordenar por SUM(UNIDADES) DESC y usar LIMIT 1 si procede.")
    return (pregunta or "").strip() + guia

# ======== Bolsas/servicios y helpers SQL ========
def _quiere_bolsas(pregunta: str) -> bool:
    return bool(re.search(r"\b(bolsa|bolsas|bag|bags|packing\s*bags)\b", str(pregunta), re.I))

def _inyectar_condicion_where(sql: str, condicion: str) -> str:
    m_where = re.search(r"\bwhere\b", sql, re.I)
    m_group = re.search(r"\bgroup\s+by\b", sql, re.I)
    m_order = re.search(r"\border\s+by\b", sql, re.I)
    m_limit = re.search(r"\blimit\b", sql, re.I)
    cut = min([p.start() for p in [m_group, m_order, m_limit] if p] + [len(sql)])
    if m_where:
        return sql[:cut] + f" AND ({condicion}) " + sql[cut:]
    # no WHERE → insertamos antes de GROUP/ORDER/LIMIT
    return sql[:cut] + f" WHERE ({condicion}) " + sql[cut:]

def excluir_bolsas_y_servicios_post_sql(sql_texto: str, pregunta: str) -> str:
    if _quiere_bolsas(pregunta): return sql_texto
    clausula = (
        "UPPER(COALESCE(DESC_ARTICULO,'')) NOT LIKE '%BOLSA%'"
        " AND UPPER(COALESCE(DESC_ARTICULO,'')) NOT LIKE '%PACKING BAG%'"
        " AND UPPER(COALESCE(DESC_ARTICULO,'')) <> 'DESPACHO A DOMICILIO'"
    )
    sentencias = [s.strip() for s in sql_texto.split(";") if s.strip()]
    nuevas = []
    for s in sentencias:
        if re.search(r"^\s*select\b", s, re.I):
            nuevas.append(_inyectar_condicion_where(s, clausula))
        else:
            nuevas.append(s)
    return ";\n".join(nuevas)

# ======== Marca (LEVI/DOCKERS) y tienda ========
_BRAND_ALIASES = {
    "LEVI": [r"levis", r"levi['´`’]s", r"levi\s*s", r"\blv\b"],
    "DOCKERS": [r"dockers", r"\bdk\b"],
}
_BRAND_TO_LIKE = {"LEVI": "LEVI", "DOCKERS": "DOCKERS"}

def _detectar_marcas(texto: str) -> set[str]:
    found = set()
    t = (texto or "").lower()
    for brand, pats in _BRAND_ALIASES.items():
        if any(re.search(p, t, re.I) for p in pats):
            found.add(brand)
    return found

def _quitar_marca_de_fragmentos_tienda(texto: str) -> str:
    t = texto
    for pats in _BRAND_ALIASES.values():
        for p in pats:
            t = re.sub(rf"\b{p}\s+(\w+)", r"\1", t, flags=re.I)
    return t

def normalizar_marcas_en_pregunta(pregunta: str) -> tuple[str, list[str]]:
    marcas = sorted(_detectar_marcas(pregunta))
    if not marcas:
        return pregunta, []
    pregunta_sin_prefijo_en_tienda = _quitar_marca_de_fragmentos_tienda(pregunta)
    hints = []
    for m in marcas:
        like = _BRAND_TO_LIKE.get(m, m)
        hints.append(f"Usar DESC_MARCA LIKE '%{like}%'")
    guia = " (" + "; ".join(hints) + ")."
    return (pregunta_sin_prefijo_en_tienda.strip() + guia), marcas

def _sql_inyectar_brand_clause(sql: str, brand_like: str) -> str:
    m_where = re.search(r"\bwhere\b", sql, re.I)
    m_group = re.search(r"\bgroup\s+by\b", sql, re.I)
    m_order = re.search(r"\border\s+by\b", sql, re.I)
    m_limit = re.search(r"\blimit\b", sql, re.I)
    cut = min([p.start() for p in [m_group, m_order, m_limit] if p] + [len(sql)])
    clause = f" DESC_MARCA LIKE '%{brand_like}%' "
    if m_where: return sql[:cut] + " AND " + clause + sql[cut:]
    return sql[:cut] + " WHERE " + clause + sql[cut:]

def separar_marca_de_tienda_en_sql(sql_texto: str) -> str:
    sentencias = [s.strip() for s in sql_texto.split(";") if s.strip()]
    nuevas = []
    for s in sentencias:
        if not re.search(r"^\s*select\b", s, re.I):
            nuevas.append(s); continue
        corrected = s
        for brand, pats in _BRAND_ALIASES.items():
            for p in pats:
                regex = rf"(DESC_TIENDA\s+LIKE\s*'%\s*){p}\s+([^%']+)(%')"
                m = re.search(regex, corrected, re.I)
                if m:
                    resto = m.group(2).strip()
                    corrected = re.sub(regex, rf"\1{resto}\3", corrected, flags=re.I)
                    corrected = _sql_inyectar_brand_clause(corrected, _BRAND_TO_LIKE.get(brand, brand))
                    break
        nuevas.append(corrected)
    return ";\n".join(nuevas)

# ======== TIPO (Jeans/Jackets/…) debe ir en DESC_TIPO, no en ARTÍCULO ========
def _enforce_tipo_like(sql_texto: str, pregunta: str) -> str:
    tipo_mencionado = None
    low_q = (pregunta or "").lower()
    for t in _TIPOS_VALIDOS:
        if t.lower() in low_q:
            tipo_mencionado = t
            break
    if not tipo_mencionado:
        return sql_texto

    sentencias = [s.strip() for s in sql_texto.split(";") if s.strip()]
    nuevas = []

    pat_art_like = re.compile(
        rf"(DESC_ARTICULO\s+LIKE\s*'%\s*{re.escape(tipo_mencionado)}\s*%')",
        re.IGNORECASE
    )
    pat_tipo_like = re.compile(
        rf"DESC_TIPO\s+LIKE\s*'%\s*{re.escape(tipo_mencionado)}\s*%'", re.IGNORECASE
    )

    for s in sentencias:
        if not re.search(r"^\s*select\b", s, re.I):
            nuevas.append(s); continue
        s_corr = pat_art_like.sub(f"DESC_TIPO LIKE '%{tipo_mencionado}%'", s)
        if not pat_tipo_like.search(s_corr):
            s_corr = _inyectar_condicion_where(s_corr, f"DESC_TIPO LIKE '%{tipo_mencionado}%'")
        nuevas.append(s_corr)

    return ";\n".join(nuevas)

# ==================== AYUDA ====================
def render_help_capacidades():
    st.markdown("## 🤖 ¿Qué puedes preguntarme?")
    st.markdown("""
Puedo entender preguntas de **ventas retail** y generar la **consulta SQL** adecuada sobre el **tablon `VENTAS`**, aplicando automáticamente filtros y reglas del negocio que ya definiste.

### Pistas rápidas
- Usa campos `DESC_*` para filtros (tienda, marca, canal, producto…).
- `FECHA_DOCUMENTO` en formato `YYYYMMDD` sin guiones.
- Unidades negativas son devoluciones → si piden “más vendido” uso `UNIDADES > 0`.
- Centros de distribución con nombre similar a “CENTRO DISTRIBUCION …” no cuentan como tienda.
- Tipos (Jeans, Jackets…) se usan **como filtro** en `DESC_TIPO`, no para mostrar.
""")

# ==================== MONEDA/PAÍS/FECHA (aclaraciones) ====================
_LOCAL_CURRENCY_BY_SOC = {"1000": "CLP", "2000": "PEN", "3000": "BOB"}
_SOC_BY_NAME = {"chile": "1000", "perú": "2000", "peru": "2000", "bolivia": "3000"}

def _solo_conteo_o_listado_de_paises(texto: str) -> bool:
    patrones = r"(cu[aá]nt[oa]s?\s+pa[ií]ses|n[uú]mero\s+de\s+pa[ií]ses|cantidad\s+de\s+pa[ií]ses|(listar|mostrar|muestr[ao])\s+(los\s+)?pa[ií]ses|qu[eé]\s+pa[ií]ses\b)"
    return bool(re.search(patrones, texto, re.I))

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
    return bool(re.search(r"\b(usd|clp|pen|bob|d[oó]lar(?:es)?|pesos?)\b", texto, re.I))

def _habla_de_pais(texto: str) -> bool:
    return bool(re.search(_COUNTRY_REGEX, texto, re.I))

def _tiene_pais(texto: str) -> bool:
    return bool(re.search(r"\b(1000|2000|3000|chile|per[uú]|bolivia)\b", texto, re.I))

def _agregacion_por_pais(texto: str) -> bool:
    patrones = (r"(por\s+pa[ií]s|seg[uú]n\s+pa[ií]s|ranking\s+de\s+pa[ií]ses|top\s+\d+\s+pa[ií]ses|"
                r"comparaci[oó]n\s+por\s+pa[ií]s|cu[aá]l(?:es)?\s+es\s+el\s+pa[ií]s\s+que\s+(?:m[aá]s|menos)|"
                r"en\s+qu[eé]\s+pa[ií]s\s+se\s+vend(?:e|i[óo]a)|en\s+qu[eé]\s+pa[ií]s\s+se\s+vende\s+(?:m[aá]s|menos))")
    return bool(re.search(patrones, texto, re.I))

_DATE_KEYS = r"(hoy|ayer|semana|mes|año|anio|últim|ultimo|desde|hasta|entre|rango|202\d|20\d\d|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)"

def _pide_montos(texto: str) -> bool:
    return bool(re.search(r"(venta|vende|ventas|ingreso|ingresos|margen|utilidad|gm|revenue|sales|facturaci[oó]n|precio|precios|car[oa]s?|barat[oa]s?|cost[eo]s?|ticket\s*promedio|valor(?:es)?)", texto, re.I))

def _tiene_fecha(texto: str) -> bool:
    return bool(re.search(_DATE_KEYS, texto, re.I))

def _habla_de_tienda(texto: str) -> bool:
    return bool(re.search(r"\btienda(s)?\b", texto, re.I))

def _menciona_cd(texto: str) -> bool:
    return bool(
        re.search(r"centro\s+de\s+distribuci[oó]n", texto, re.I) or
        re.search(r"\bcentro\s+distribucion\b", texto, re.I) or
        re.search(r"\bCD\b", texto, re.I)
    )

def _to_yyyymmdd(v) -> str:
    if isinstance(v, _dt.date): return v.strftime("%Y%m%d")
    if isinstance(v, str):
        v = v.strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                d = _dt.datetime.strptime(v, fmt).date()
                return d.strftime("%Y%m%d")
            except Exception:
                pass
    return str(v)

def _necesita_aclaracion(texto: str) -> dict:
    habla_pais  = _habla_de_pais(texto)
    tiene_pais  = _tiene_pais(texto)
    agrega_pais = _agregacion_por_pais(texto)
    conteo_o_listado = _solo_conteo_o_listado_de_paises(texto)

    ref_tiendas = (("esas tiendas" in texto.lower()) or ("estas tiendas" in texto.lower())) and \
                  ("DESC_TIENDA_LIST" in st.session_state.get("contexto", {}))

    return {
        "moneda": (_pide_montos(texto) and not _tiene_moneda(texto)),
        "pais":   (habla_pais and not tiene_pais and not agrega_pais and not conteo_o_listado and not ref_tiendas),
        "fecha":  (not _tiene_fecha(texto)),
        "tienda_vs_cd": (_habla_de_tienda(texto) and not _menciona_cd(texto)),
    }

def _inyectar_aclaraciones_en_pregunta(pregunta: str, moneda, rango, excluir_cd):
    partes = [pregunta.strip()]
    if moneda: partes.append(f" en moneda {moneda}")
    if rango:
        d, h = rango
        partes.append(
            f" usando FECHA_DOCUMENTO entre {_to_yyyymmdd(d)} y {_to_yyyymmdd(h)} (formato YYYYMMDD sin guiones)"
        )
    if excluir_cd is not None:
        partes.append(" excluyendo el Centro de Distribución" if excluir_cd else " incluyendo el Centro de Distribución")
    return " ".join(partes).strip()

def manejar_aclaracion(pregunta: str):
    flags = _necesita_aclaracion(pregunta)
    _SUF = str(abs(hash(pregunta)) % 100000)
    if not any(flags.values()): return None

    st.info("Antes de ejecutar, aclaremos algunos detalles para evitar resultados ambiguos 👇")

    st.session_state.setdefault("clarif_moneda", None)
    st.session_state.setdefault("clarif_fecha_desde", None)
    st.session_state.setdefault("clarif_fecha_hasta", None)
    st.session_state.setdefault("clarif_excluir_cd", True)

    paises_texto = _extraer_paises(pregunta)
    es_agrupado = _agregacion_por_pais(pregunta)
    sugeridas = _sugerir_monedas(paises_texto, es_agrupado)

    if es_agrupado or len(paises_texto) != 1:
        monedas_permitidas = ["USD"]
    elif len(paises_texto) == 1:
        local = _LOCAL_CURRENCY_BY_SOC[list(paises_texto)[0]]
        monedas_permitidas = ["USD", local]
    else:
        monedas_permitidas = ["USD", "CLP", "PEN", "BOB"]

    if flags["moneda"]:
        st.subheader("Moneda")
        st.session_state["clarif_moneda"] = st.multiselect(
            "¿En qué moneda(s) quieres ver los montos?",
            options=monedas_permitidas,
            default=sugeridas,
            key=f"k_moneda_multi_{_SUF}",
            help="Si comparas varios países o pides ranking por país, sólo USD.",
        )
    else:
        if st.session_state.get("clarif_moneda") is None:
            st.session_state["clarif_moneda"] = sugeridas

    if flags["fecha"]:
        st.subheader("Rango de fechas")
        hoy = _dt.date.today()
        desde_def = hoy - _dt.timedelta(days=30)
        val = st.date_input("Selecciona el rango", value=(desde_def, hoy), key=f"k_rango_fechas_{_SUF}")
        if isinstance(val, tuple) and len(val) == 2:
            d, h = val
        else:
            d, h = val, None
        st.session_state["clarif_fecha_desde"] = d
        st.session_state["clarif_fecha_hasta"] = h
        if h is None:
            st.caption("Elige también la fecha de término para continuar.")
            st.stop()

    pais_code, pais_label = None, None
    if flags.get("pais"):
        st.subheader("País")
        pais_label = st.radio(
            "¿Para qué país?",
            options=["Chile", "Perú", "Bolivia"],
            horizontal=True,
            key=f"k_pais_radio_{_SUF}",
        )
        pais_code = {"Chile": "1000", "Perú": "2000", "Bolivia": "3000"}[pais_label]
        st.session_state["clarif_pais_code"] = pais_code
        st.session_state["clarif_pais_label"] = pais_label

    if flags["tienda_vs_cd"]:
        st.subheader("Tipo de ubicación")
        st.session_state["clarif_excluir_cd"] = st.checkbox(
            "Excluir Centros de Distribución (CD)", value=True, key=f"k_excluir_cd_{_SUF}",
        )

    if st.button("✅ Continuar con estas opciones", type="primary", key=f"btn_continuar_opciones_{_SUF}"):
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

        st.session_state["clarif_moneda_last"] = moneda_sel
        for k in ["clarif_moneda","clarif_fecha_desde","clarif_fecha_hasta",
                  "clarif_excluir_cd","clarif_pais_code","clarif_pais_label"]:
            st.session_state.pop(k, None)
        return pregunta_enriquecida

    st.stop()

# ==================== PROMPT LLM ====================
sql_prompt = PromptTemplate(
    input_variables=["pregunta"],
    template="""
1. Usa campos descriptivos DESC_* (no COD_*), salvo que se pida explícitamente "código".
2. Fechas: FECHA_DOCUMENTO en formato 'YYYYMMDD' sin guiones.
3. Ventas / montos: INGRESOS; costos: COSTOS; unidades: UNIDADES (>0 para ventas).
4. Centros de Distribución no cuentan como TIENDA.
5. "Despacho a domicilio" es un servicio (no artículo).
6. Tipos (Jeans, Jackets, ...) se usan como filtro en DESC_TIPO (no para mostrar), salvo que se pida "por tipo".
7. Si la consulta es por país, agrupa por SOCIEDAD_CO y decodifica con CASE a nombre de país.
8. Cuando reemplaces referencias ("esa tienda", etc.), usa siempre LIKE '%valor%'.
9. Cuando pidas canales de una tienda, usa SELECT DISTINCT DESC_CANAL.

🖍️ Devuelve SOLO SQL listo para MySQL.

Pregunta: {pregunta}
""",
)

# ==================== CONTEXTO/REFERENCIAS ====================
referencias = {
    "esa tienda": "DESC_TIENDA",
    "esta tienda": "DESC_TIENDA",
    "ese canal": "DESC_CANAL",
    "esa marca": "DESC_MARCA",
    "ese producto": ["DESC_ARTICULO", "DESC_TIPO"],
    "ese artículo": ["DESC_ARTICULO", "DESC_TIPO"],
    "ese articulo": ["DESC_ARTICULO", "DESC_TIPO"],
    "esa categoría": "DESC_CATEGORIA",
    "esa categoria": "DESC_CATEGORIA",
    "ese cliente": "NOMBRE_CLIENTE",
    "ese género": "DESC_GENERO",
    "ese genero": "DESC_GENERO",
    "ese sexo": "DESC_GENERO",
    "ese país": "SOCIEDAD_CO",
    "ese pais": "SOCIEDAD_CO",
    "ese tipo": "DESC_TIPO",
    "esas tiendas": "DESC_TIENDA",
    "estas tiendas": "DESC_TIENDA",
}

def aplicar_contexto(pregunta: str) -> str:
    pregunta_mod = pregunta
    lower_q = (pregunta or "").lower()
    st.session_state["__last_ref_replacement__"] = None

    if ("esas tiendas" in lower_q or "estas tiendas" in lower_q) and \
       "DESC_TIENDA_LIST" in st.session_state.get("contexto", {}):
        lista = st.session_state["contexto"]["DESC_TIENDA_LIST"]
        lista_sql = "', '".join(s.replace("'", "''") for s in lista)
        guia_in = f" (Filtrar con DESC_TIENDA IN ('{lista_sql}'))"
        pregunta_mod = re.sub(r"(esas|estas)\s+tiendas", "las tiendas indicadas", pregunta_mod, flags=re.I)
        pregunta_mod += guia_in
        st.session_state["__last_ref_replacement__"] = "DESC_TIENDA_LIST"
        st.session_state["__last_ref_value__"] = lista

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

def actualizar_contexto(df: pd.DataFrame):
    alias = {
        "DESC_TIENDA": ["DESC_TIENDA", "TIENDA", "Tienda"],
        "DESC_CANAL": ["DESC_CANAL", "CANAL", "Canal"],
        "DESC_MARCA": ["DESC_MARCA", "MARCA", "Marca"],
        "DESC_ARTICULO": ["DESC_ARTICULO", "ARTICULO", "Artículo", "Articulo"],
        "DESC_GENERO": ["DESC_GENERO", "GENERO", "Género", "Genero"],
        "DESC_TIPO": ["DESC_TIPO", "TIPO", "Tipo"],
        "NOMBRE_CLIENTE": ["NOMBRE_CLIENTE", "CLIENTE", "Cliente"],
        "SOCIEDAD_CO": ["PAIS", "PAISES", "Pais","Paises","Países","País"],
    }
    if "DESC_TIENDA" in df.columns:
        tiendas = (
            df["DESC_TIENDA"].dropna().astype(str).map(str.strip).unique().tolist()
        )
        tiendas = [t for t in tiendas if t and not es_centro_distribucion(t)]
        if tiendas:
            st.session_state.setdefault("contexto", {})["DESC_TIENDA_LIST"] = tiendas

    articulo_capturado = False
    for canonico, posibles in alias.items():
        for col in posibles:
            if col in df.columns and not df[col].isnull().all():
                valor = str(df[col].dropna().iloc[0]).strip()
                if not valor: continue
                if canonico == "DESC_TIENDA" and es_centro_distribucion(valor):
                    continue
                st.session_state.setdefault("contexto", {})[canonico] = valor
                if canonico == "DESC_ARTICULO": articulo_capturado = True
                break

    if articulo_capturado and "DESC_TIPO" in st.session_state["contexto"]:
        st.session_state["contexto"].pop("DESC_TIPO", None)

def forzar_distinct_canal_si_corresponde(pregunta, sql_generado):
    if re.search(r"\bcanal(es)?\b", pregunta, flags=re.IGNORECASE) and \
       re.search(r"\btienda\b|esa tienda", pregunta, flags=re.IGNORECASE):
        if not re.search(r"\bselect\s+distinct\b", sql_generado, flags=re.IGNORECASE):
            return f"SELECT DISTINCT DESC_CANAL FROM ({sql_generado}) AS t"
    return sql_generado

# ==================== CACHE/LOG ====================
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
        if conn is None: return
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
    if embedding_nuevo is None: return None
    try:
        conn = connect_db()
        if conn is None: return None
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

def log_interaction(pregunta, sql, resultado, feedback=None):
    try:
        conn = connect_db()
        if conn is None: return
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO chat_logs (pregunta, sql_generado, resultado, feedback) VALUES (%s, %s, %s, %s)",
            (pregunta, sql, resultado, feedback)
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        st.warning(f"⚠️ No se pudo guardar el log: {e}")

# ==================== UI BÁSICA ====================
if "historial" not in st.session_state: st.session_state["historial"] = []
if "conversacion" not in st.session_state: st.session_state["conversacion"] = []
if "contexto" not in st.session_state: st.session_state["contexto"] = {}

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

# ==================== ENTRADA DEL USUARIO ====================
pregunta = st.chat_input("🧠 Pregunta en lenguaje natural")
if not pregunta and st.session_state.get("pending_question"):
    pregunta = st.session_state["pending_question"]

if pregunta and _HELP_TRIGGERS_RE.search(pregunta or ""):
    with st.chat_message("assistant"):
        render_help_capacidades()
    st.session_state.pop("pending_question", None)
    st.stop()

sql_query = None
resultado = ""
guardar_en_cache_pending = None

if pregunta:
    st.session_state["pending_question"] = pregunta
    st.session_state["__last_user_question__"] = pregunta
    st.session_state["__last_ref_replacement__"] = None

    # Normaliza marcas y evita contaminar DESC_TIENDA
    pregunta, _marcas_detectadas = normalizar_marcas_en_pregunta(pregunta)
    if _marcas_detectadas:
        st.session_state.setdefault("contexto", {})["DESC_MARCA"] = _BRAND_TO_LIKE.get(
            _marcas_detectadas[0], _marcas_detectadas[0]
        )

    # Normaliza TIPO es→en para que el LLM reconozca correctamente (Jeans, Jackets, ...)
    pregunta = mapear_desc_tipo_es_en(pregunta)

    # Desambiguación (moneda/fechas/CD/país)
    pregunta_clara = manejar_aclaracion(pregunta)  # (puede hacer st.stop())
    if pregunta_clara:
        pregunta = pregunta_clara
        st.session_state["pending_question"] = pregunta

    with st.chat_message("user"):
        st.markdown(pregunta)

    # Cache semántica
    sql_query = buscar_sql_en_cache(pregunta)
    if sql_query:
        st.info("🔁 Consulta reutilizada desde la cache.")
    else:
        # Derivar género simple desde texto
        if re.search(r"\b(mujer|femenin[oa])\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Woman"
        elif re.search(r"\b(hombre|masculin[oa]|varón|varon|caballero)\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Men"
        elif re.search(r"\bunisex\b", pregunta, flags=re.IGNORECASE):
            st.session_state["contexto"]["DESC_GENERO"] = "Unisex"

        # Aplicar contexto (pronombres, listas de tiendas)
        pregunta_con_contexto = aplicar_contexto(pregunta)

        # Si el pronombre resolvió a ARTÍCULO, forzar DESC_ARTICULO (no TIPO)
        if st.session_state.get("__last_ref_replacement__") == "DESC_ARTICULO":
            art_val = st.session_state.get("__last_ref_value__", "")
            if art_val:
                pregunta_con_contexto += (
                    f" Usa estrictamente DESC_ARTICULO LIKE '%{art_val}%' (case-insensitive) "
                    f"y UNIDADES > 0. No uses DESC_TIPO para este filtro."
                )

        # Añade guía de TIPO si aplica
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

        # Prompt → SQL
        prompt_text = sql_prompt.format(pregunta=pregunta_con_contexto)
        sql_query = llm.predict(prompt_text).replace("```sql", "").replace("```", "").strip()

        # Post-parches sobre el SQL
        sql_query = excluir_bolsas_y_servicios_post_sql(sql_query, pregunta)
        sql_query = _enforce_tipo_like(sql_query, pregunta)          # Jeans/Jackets → DESC_TIPO
        sql_query = forzar_distinct_canal_si_corresponde(pregunta_con_contexto, sql_query)
        sql_query = separar_marca_de_tienda_en_sql(sql_query)        # Levis <tienda> → DESC_MARCA + DESC_TIENDA

        # Preparar cache semántica
        embedding = obtener_embedding(pregunta)
        guardar_en_cache_pending = embedding if embedding else None

# ==================== EJECUCIÓN DEL SQL ====================
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
                        df_sub = aplicar_formato_monetario(df_sub)
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
    resultado = ""

# ==================== LOG & CHAT UI ====================
if sql_query:
    st.session_state["conversacion"].append({
        "pregunta": pregunta,
        "respuesta": resultado,
        "sql": sql_query,
        "cache": guardar_en_cache_pending
    })
    st.session_state.pop("pending_question", None)

chips = []
_pregunta_ctx = locals().get("pregunta_con_contexto", pregunta or "")
mon_last = st.session_state.get("clarif_moneda_last")
if isinstance(mon_last, list) and mon_last:
    chips.append("Moneda: " + ", ".join(mon_last))
elif isinstance(mon_last, str) and mon_last:
    chips.append("Moneda: " + mon_last)
m = re.search(r"FECHA_DOCUMENTO entre (\d{8}) y (\d{8})", _pregunta_ctx, re.I)
if m: chips.append(f"Rango: {m.group(1)} → {m.group(2)}")
if "excluyendo el Centro de Distribución" in _pregunta_ctx: chips.append("CDs excluidos")
elif "incluyendo el Centro de Distribución" in _pregunta_ctx: chips.append("CDs incluidos")
if "clarif_pais_label" in st.session_state: chips.append("País: " + str(st.session_state["clarif_pais_label"]))
tiendas_list = st.session_state.get("contexto", {}).get("DESC_TIENDA_LIST")
if isinstance(tiendas_list, list) and tiendas_list:
    chips.append(f"Tiendas: {len(tiendas_list)} seleccionada(s)")

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
            if st.button("✅ Fue acertada", key="ok_last"):
                st.success("Gracias por tu feedback. 👍")
                if guardar_en_cache_pending:
                    guardar_en_cache(pregunta, sql_query, guardar_en_cache_pending)
                log_interaction(pregunta, sql_query, resultado, "acertada")
        with col2:
            if st.button("❌ No fue correcta", key="fail_last"):
                st.warning("Gracias por reportarlo. Mejoraremos esta consulta. 🚲")
                log_interaction(pregunta, sql_query, resultado, "incorrecta")

    st.markdown("---")

if st.session_state["conversacion"]:
    st.markdown("## ⌛ Historial de preguntas anteriores")
    st.session_state["conversacion"] = [
        it for it in st.session_state["conversacion"]
        if it and it.get("pregunta") and it.get("sql")
    ]
    for i, item in enumerate(reversed(st.session_state["conversacion"][:-1])):
        pregunta_hist = item.get("pregunta", "—")
        sql_hist = item.get("sql")
        if not sql_hist: continue
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
                        for idx, q in enumerate(split_queries(sql_hist), start=1):
                            if not es_consulta_segura(q):
                                st.warning(f"⚠️ Subconsulta {idx} bloqueada por seguridad.")
                                continue
                            df_hist = ejecutar_select(conn, q)
                            if df_hist is not None:
                                df_hist = aplicar_formato_monetario(df_hist)
                                st.subheader(f"Resultado {idx}")
                                st.dataframe(df_hist, hide_index=True, use_container_width=True)
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

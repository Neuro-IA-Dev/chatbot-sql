# --- MONITOREO DE COSTOS OPENAI ---
def obtener_consumo_openai(api_key):
    try:
        hoy = datetime.date.today()
        inicio_mes = hoy.replace(day=1)
        url = f"https://api.openai.com/v1/dashboard/billing/usage?start_date={inicio_mes}&end_date={hoy}"

        headers = {
            "Authorization": f"Bearer {api_key}"
            # Si tu cuenta usa organización, descomenta esta línea y reemplaza con tu ID:
            # "OpenAI-Organization": "org-xxxxxxxx"
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            usd = data.get("total_usage", 0) / 100  # OpenAI entrega en centavos
            return round(usd, 2)

        elif response.status_code == 401:
            return "❌ Error 401: API Key inválida o sin permisos de uso"

        else:
            return f"❌ Error {response.status_code}: {response.text}"

    except Exception as e:
        return f"❌ Excepción: {e}"

if st.toggle("💰 Ver costo acumulado en OpenAI"):
    with st.spinner("Consultando consumo..."):
        consumo = obtener_consumo_openai(st.secrets["OPENAI_API_KEY"])
        st.metric("Consumo actual OpenAI (mes)", f"{consumo}")





# Revisar IP
#import requests

#try:
 #   ip = requests.get("https://api64.ipify.org").text
  #  st.markdown(f"🌐 **IP pública del servidor (Streamlit):** `{ip}`")
#except Exception as e:
 #   st.warning(f"No se pudo obtener la IP pública: {e}")

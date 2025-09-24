import json
import time
import google.generativeai as genai

# Funciones de DB movidas a `db_gnoticias.py`
from gnoticias.db_gnoticias import (
    get_news_without_sentiment,
    update_news_sentiment
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update

# ================= CONSTANTES =================
API_KEYS_PATH = "api_keys.txt"

# ================= FUNCIONES =================

# --- LÓGICA DE ANÁLISIS DE IA (Reutilizada de ex_gnoticias.py) ---
def get_api_key(key_name="GEMINI_API_KEY"):
    """Lee la clave de API desde el archivo api_keys.txt."""
    try:
        with open(API_KEYS_PATH, 'r') as f:
            for line in f:
                if line.startswith(key_name):
                    return line.strip().split('=')[1]
    except FileNotFoundError:
        print(f"Error: El archivo {API_KEYS_PATH} no fue encontrado.")
        return None
    except Exception as e:
        print(f"Error al leer la clave de API: {e}")
        return None

def analizar_sentimiento(texto, candidata):
    """Analiza el sentimiento de un texto usando Gemini."""
    if not texto:
        return None
    prompt = f'''
    Analiza el siguiente titular de una noticia sobre {candidata}.
    Extrae el tema principal en una frase corta (máx 5 palabras).
    Clasifica el sentimiento hacia {candidata} en: Positivo, Negativo, o Neutral.
    Retorna solo un JSON con claves "tema_principal" y "sentimiento".
    Titular: {texto}
    '''
    response_schema = {
        "type": "object",
        "properties": {
            "tema_principal": {"type": "string"},
            "sentimiento": {"type": "string", "enum": ["Positivo", "Negativo", "Neutral"]}
        },
        "required": ["tema_principal", "sentimiento"]
    }
    modelo = genai.GenerativeModel(
        "gemini-1.5-flash",
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        }
    )
    try:
        respuesta = modelo.generate_content(prompt)
        return json.loads(respuesta.text)
    except Exception as e:
        print(f"Ocurrió un error al generar contenido con Gemini: {e}")
        return None

def procesar_lote_sentimientos(log_id=None):
    """Procesa un único lote de hasta 250 noticias sin sentimiento."""
    print("\nBuscando un lote de noticias sin sentimiento...")
    try:
        news_batch = get_news_without_sentiment(limit=250)
    except Exception as e:
        print(f"❌ Error inesperado al obtener noticias: {e}")
        if log_id:
            log_error_update(log_id, e)
        return 0, False # Devuelve 0 procesadas, y False para no continuar

    if not news_batch:
        print("✅ No hay más noticias por procesar.")
        return 0, False # Devuelve 0 procesadas, y False para no continuar

    print(f"Lote de {len(news_batch)} noticias encontrado. Procesando...")
    procesadas_en_lote = 0

    for news in news_batch:
        id_gnoticia = news["id_gnoticia"]
        titular = news["noticia"]
        candidato_nombre = news["candidato_nombre"]

        print(f"-> Analizando: {titular}")
        try:
            analisis_json = analizar_sentimiento(titular, candidato_nombre)
            
            if analisis_json:
                sentimiento = analisis_json.get('sentimiento')
                tema = analisis_json.get('tema_principal')
                update_news_sentiment(id_gnoticia, sentimiento, tema)
                procesadas_en_lote += 1
                print(f"   -> Análisis exitoso. Sentimiento: {sentimiento}")
            else:
                print("   -> Falló el análisis con IA. Se reintentará en la próxima ejecución.")

            time.sleep(4) # Pausa para no exceder limites de API

        except Exception as e:
            print(f"❌ Error procesando noticia {id_gnoticia}: {e}")
            if log_id:
                log_error_update(log_id, e)
            continue
    
    print(f"Lote procesado. {procesadas_en_lote} noticias actualizadas.")
    return procesadas_en_lote, True # Devuelve las procesadas y True para continuar

# ========= EJECUCIÓN PRINCIPAL =========
def main():
    """Función principal para procesar todas las noticias sin sentimiento en bucle."""
    api_key = get_api_key()
    if not api_key:
        print("No se pudo obtener la API Key de Gemini. Abortando.")
        return
    genai.configure(api_key=api_key)

    log_id = log_start('procesar_sentimientos', 'Inicio de análisis de sentimientos faltantes')
    
    total_procesadas = 0
    while True:
        procesadas, continuar = procesar_lote_sentimientos(log_id)
        total_procesadas += procesadas
        if not continuar:
            break

    print(f"\n🏁 Proceso completado. Total de noticias actualizadas: {total_procesadas}")
    log_end(log_id, estado='completed', mensaje=f'Total procesadas: {total_procesadas}')

if __name__ == "__main__":
    main()

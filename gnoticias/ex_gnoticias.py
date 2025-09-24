import re
import unicodedata
from difflib import SequenceMatcher
import sqlite3
import feedparser
import hashlib
import time
import json
import urllib.parse
from datetime import datetime, timedelta
from googlenewsdecoder import gnewsdecoder
import requests
import google.generativeai as genai

# Funciones de DB
from gnoticias.db_gnoticias import (
    get_db_connection,
    news_exists_in_gnoticias,
    save_news_to_gnoticias_with_sentiment,
    marcar_candidato_como_procesado,
    reset_candidatos_news,
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update, log_error_new
from gnoticias.procesar_sentimientos import procesar_todas_las_noticias_sin_sentimiento

# ================= CONSTANTES =================
STOPWORDS_APELLIDO = {"de", "del", "la", "las", "los", "y", "san", "santa"}
API_KEYS_PATH = "api_keys.txt"
MODEL_NAME = "gemini-2.5-flash-lite"

# ================= FUNCIONES =================
def similarity(a, b):
    """Similitud usando SequenceMatcher (0-100)."""
    return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
    """Normaliza el texto: minúsculas, sin tildes, puntuación como espacios."""
    if not text:
        return ""
    text = unicodedata.normalize('NFD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9ñ\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

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

def analizar_sentimiento(prompt):
    """Analiza el sentimiento de un texto usando un modelo de IA con reintentos exponenciales."""
    print(f"🤖 Usando modelo: {MODEL_NAME}")
    
    response_schema = {
        "type": "object",
        "properties": {
            "tema_principal": {"type": "string"},
            "sentimiento": {"type": "string", "enum": ["Positivo", "Negativo", "Neutral"]}
        },
        "required": ["tema_principal", "sentimiento"]
    }
    
    modelo = genai.GenerativeModel(
        MODEL_NAME,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        }
    )
    
    reintentos = 0
    while reintentos < 5:  # Intenta hasta 5 veces
        try:
            respuesta = modelo.generate_content(prompt)
            return json.loads(respuesta.text)
        except Exception as e:
            if "quota" in str(e).lower():
                print(f"Error de cuota, reintentando en {2**reintentos} segundos...")
                time.sleep(2**reintentos)  # Espera exponencial
                reintentos += 1
            else:
                print(f"Ocurrió un error al generar contenido con {MODEL_NAME}: {e}")
                return None
    print(f"❌ Falló el análisis de sentimiento después de {reintentos} reintentos.")
    return None

def limpiar_apellidos(tokens):
    """Quita partículas de apellidos (de, del, la, etc.)"""
    return [t for t in tokens if t not in STOPWORDS_APELLIDO]

def buscar_nombre_en_titular(nombre, titular, umbral_nombre=85, umbral_fuzzy=85):
    """Busca el nombre de un candidato en un titular con reglas estrictas."""
    nombre_norm = normalize_text(nombre)
    titular_norm = normalize_text(titular)
    
    partes = nombre_norm.split()
    if not partes:
        return (None, 0, "nombre_invalido")

    partes_limpias = limpiar_apellidos(partes)
    nombre_simple = partes_limpias[0]
    
    if len(partes_limpias) >= 2:
        apellido_paterno = partes_limpias[-2] if len(partes_limpias) >= 3 else partes_limpias[-1]
        apellido_materno = partes_limpias[-1]
        apellido_compuesto = " ".join(partes_limpias[-2:]) if len(partes_limpias) >= 3 else None

        if apellido_compuesto and apellido_compuesto in titular_norm:
            return (apellido_compuesto, 100, "apellido_compuesto")

        nombre_encontrado = nombre_simple in titular_norm.split()
        paterno_encontrado = apellido_paterno in titular_norm.split()
        materno_encontrado = apellido_materno in titular_norm.split()

        if nombre_encontrado and (paterno_encontrado or materno_encontrado):
            match_encontrado = nombre_simple
            if paterno_encontrado:
                match_encontrado += " " + apellido_paterno
            if materno_encontrado and not paterno_encontrado:
                 match_encontrado += " " + apellido_materno
            return (match_encontrado.strip(), 100, "nombre_y_apellido")
            
    elif len(partes_limpias) == 1:
        if nombre_simple in titular_norm.split():
            return (nombre_simple, 100, "single_word_match")

    return (None, 0, "no_match_estricto")

def process_feed_entry(entry, candidato_id):
    titulo = entry.get("title", "")
    published_parsed = entry.get("published_parsed")
    if not published_parsed:
        print(f"⚠️ Entrada sin fecha publicada, omitiendo: {titulo}")
        return None
    try:
        fecha_dt = datetime(*published_parsed[:6])
        published_year, published_month, published_day = fecha_dt.year, fecha_dt.month, fecha_dt.day
        published_hour, published_minute = fecha_dt.hour, fecha_dt.minute
        published_wday, published_yday = fecha_dt.weekday(), fecha_dt.timetuple().tm_yday
    except Exception as e:
        print(f"⚠️ Error al parsear fecha de entrada '{titulo}': {e}, omitiendo.")
        return None
    if " - " in titulo:
        parts = titulo.rsplit(" - ", 1)
        noticia = parts[0].strip()
        medio = parts[1].strip()
    else:
        noticia = titulo.strip()
        medio = "Desconocido"
    google_news_url = entry.get("link", "")
    link = gnewsdecoder(google_news_url).get("decoded_url", google_news_url) if google_news_url else ""
    if not link:
        return None
    entry_id = entry.get("id", google_news_url)
    id_corto = hashlib.md5(entry_id.encode("utf-8")).hexdigest()
    return {
        "candidato_id": candidato_id, "id": id_corto, "noticia": noticia, "medio": medio,
        "fecha": fecha_dt, "source_href": entry.source.get("href", "") if hasattr(entry, "source") else "",
        "ano": published_year, "mes": published_month, "dia": published_day, "hora": published_hour,
        "minuto": published_minute, "dia_sem": published_wday, "dia_ano": published_yday,
        "link": link, "id_largo": entry_id,
    }

def verificar_relevancia_especial(candidato_nombre, titular):
    nombre_norm = normalize_text(candidato_nombre)
    titular_norm = normalize_text(titular)
    if nombre_norm == "miguel uribe londoño":
        if "londoño" in titular_norm or (("padre" in titular_norm or "papa" in titular_norm) and "uribe" in titular_norm):
            return True
        return False
    if nombre_norm == "miguel uribe turbay":
        if "miguel uribe" in titular_norm:
            return True
        return False
    return None

def fetch_news_for_candidate(candidato_id, candidato_nombre, start_date, end_date, id_tema=None):
    """Obtiene noticias, las analiza y las guarda directamente en la tabla gnoticias."""
    safe_name = " ".join(str(candidato_nombre).split())
    query = f'"{safe_name}" when:1d'
    url = "https://news.google.com/rss/search?" + urllib.parse.urlencode({"q": query, "hl": "es-419", "gl": "CO", "ceid": "CO:es-419"})
    print(f"\n📅 hoy | URL: {url}")
    try:
        feed = feedparser.parse(url)
        if feed.bozo:
            print(f"⚠️ Error al parsear el feed: {feed.bozo_exception}")

        for entry in feed.entries:
            prelim = process_feed_entry(entry, candidato_id)
            if not prelim:
                continue

            es_relevante = False
            relevancia_especial = verificar_relevancia_especial(candidato_nombre, prelim["noticia"])
            if relevancia_especial is not None:
                if relevancia_especial:
                    es_relevante = True
                else:
                    continue
            else:
                coincidencia, _, _ = buscar_nombre_en_titular(candidato_nombre, prelim["noticia"])
                if coincidencia:
                    es_relevante = True
            
            if not es_relevante:
                print(f"⏩ Omitida por baja similitud: {prelim['noticia']}")
                continue

            if news_exists_in_gnoticias(prelim["id"], candidato_id):
                print(f"⚠️ Duplicada en gnoticias (omitida): {prelim['noticia']}")
                continue

            print(f"-> Relevante. Analizando sentimiento para: {prelim['noticia']}")
            
            prompt = f'''
            Analiza el siguiente titular de una noticia sobre {candidato_nombre}.
            Extrae el tema principal en una frase corta (máx 5 palabras).
            Clasifica el sentimiento hacia {candidato_nombre} en: Positivo, Negativo, o Neutral.
            Retorna solo un JSON con claves "tema_principal" y "sentimiento".
            Titular: {prelim['noticia']}
            '''
            analisis_json = analizar_sentimiento(prompt)
            
            if analisis_json:
                prelim['sentimiento'] = analisis_json.get('sentimiento')
                prelim['tema'] = analisis_json.get('tema_principal')
                print(f"   -> Análisis exitoso. Sentimiento: {prelim['sentimiento']}")
            else:
                print("   -> Falló el análisis con IA o se alcanzó la cuota. Se guardará sin análisis y se detiene el proceso para este candidato.")
                # Guardar sin análisis y detener para este candidato
                save_news_to_gnoticias_with_sentiment(prelim)
                break # Detiene el bucle for para no procesar más noticias de este candidato

            save_news_to_gnoticias_with_sentiment(prelim)
            time.sleep(5)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de red al obtener noticias: {e}")
    except Exception as e:
        print(f"❌ Error inesperado al procesar feed: {e}")

    marcar_candidato_como_procesado(candidato_id, campo="ex")

def main(start_date_str=None, end_date_str=None):
    """Función principal para procesar todos los candidatos pendientes."""
    api_key = get_api_key()
    if not api_key:
        print("No se pudo obtener la API Key de Gemini. Abortando.")
        return
    genai.configure(api_key=api_key)

    log_id = log_start('ex_gnoticias_diario', 'inicio procesamiento')
    try:
        reset_candidatos_news()
    except Exception as e:
        print(f"❌ No se pudo resetear 'news' en candidatos: {e}")

    while True:
        with get_db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT id_candidato, nombre, id_tema FROM candidatos WHERE ex IS NOT 1 AND id_tema IS NOT NULL ORDER BY id_candidato ASC LIMIT 1")
                row = cur.fetchone()
            except Exception as e:
                print(f"❌ Error inesperado al obtener candidato: {e}")
                row = None

        if not row:
            print("✅ No hay más candidatos por procesar. Proceso finalizado.")
            break

        candidato_id, candidato_nombre, id_tema = row
        print(f"✅ Procesando: {candidato_nombre} (ID {candidato_id})")
        try:
            fetch_news_for_candidate(candidato_id, candidato_nombre, None, None, id_tema=id_tema)
        except Exception as e:
            print(f"❌ Error procesando candidato {candidato_id}: {e}")
            log_error_update(log_id, e)

    print("\n---")
    print("🚀 Iniciando lote de procesamiento de sentimientos faltantes...")
    try:
        procesar_todas_las_noticias_sin_sentimiento(log_id=log_id)
    except Exception as e:
        print(f"❌ Error inesperado durante el procesamiento de sentimientos por lote: {e}")
        log_error_update(log_id, e)
    finally:
        log_end(log_id, estado='finished', mensaje='Proceso diario y lote de sentimientos completado.')

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Obtener y analizar noticias de Google News para candidatos.")
    args = parser.parse_args()
    main()

import re
import unicodedata
from difflib import SequenceMatcher
import sqlite3
import feedparser
import hashlib
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from googlenewsdecoder import gnewsdecoder
import requests

# Funciones de DB
from gnoticias.db_gnoticias import (
    get_db_connection,
    news_exists_in_gnoticias,
    save_news_to_gnoticias,
    marcar_candidato_como_procesado,
)
from gnoticias.db_log_ejecucion import log_start, log_end, log_error_update, log_error_new

STOPWORDS_APELLIDO = {"de", "del", "la", "las", "los", "y", "san", "santa"}

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9ñ\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Zona horaria de Colombia
COL_TZ = timezone(timedelta(hours=-5))

def normalize_to_colombia_time(fecha_dt):
    # Si fecha_dt NO tiene tzinfo, asumimos que viene en UTC (caso Google News)
    if fecha_dt.tzinfo is None:
        fecha_dt = fecha_dt.replace(tzinfo=timezone.utc)
    # Convertimos a UTC-5
    return fecha_dt.astimezone(COL_TZ)

def process_feed_entry(entry, candidato_id, fecha_dt):
    titulo = entry.get("title", "")
    if " - " in titulo:
        parts = titulo.rsplit(" - ", 1)
        noticia = parts[0].strip()
        medio = parts[1].strip()
    else:
        noticia = titulo.strip()
        medio = "Desconocido"

    # Normalizar timezone
    fecha_local = normalize_to_colombia_time(fecha_dt)

    google_news_url = entry.get("link", "")
    link = gnewsdecoder(google_news_url).get("decoded_url", google_news_url) if google_news_url else ""
    if not link:
        return None

    entry_id = entry.get("id", google_news_url)
    id_corto = hashlib.md5(entry_id.encode("utf-8")).hexdigest()

    return {
        "candidato_id": candidato_id,
        "id": id_corto,
        "noticia": noticia,
        "medio": medio,
        "fecha": fecha_local,   # <-- fecha final en Colombia
        "source_href": entry.source.get("href", "") if hasattr(entry, "source") else "",
        "ano": fecha_local.year,
        "mes": fecha_local.month,
        "dia": fecha_local.day,
        "hora": fecha_local.hour,
        "minuto": fecha_local.minute,
        "dia_sem": fecha_local.weekday(),
        "dia_ano": fecha_local.timetuple().tm_yday,
        "link": link,
        "id_largo": entry_id,
    }

def fetch_news_for_candidate_historico(candidato_id, candidato_nombre, keywords, start_date, end_date, log_id=None):
    safe_name = " ".join(str(candidato_nombre).split())
    current_date = start_date
    while current_date <= end_date:
        query = f'"{safe_name}" after:{current_date.strftime("%Y-%m-%d")} before:{(current_date + timedelta(days=1)).strftime("%Y-%m-%d")}'
        url = "https://news.google.com/rss/search?" + urllib.parse.urlencode({"q": query, "hl": "es-419", "gl": "CO", "ceid": "CO:es-419"})
        print(f"\n📅 {current_date.strftime('%Y-%m-%d')} | URL: {url}")
        try:
            feed = feedparser.parse(url)
            if feed.bozo:
                print(f"⚠️ Error al parsear el feed: {feed.bozo_exception}")
            for entry in feed.entries:
                prelim = process_feed_entry(entry, candidato_id, current_date)
                if not prelim:
                    continue
                if not keywords:
                    print(f"⏩ Omitida por falta de keywords para el candidato: {candidato_nombre}")
                    continue
                keyword_list = [k.strip() for k in keywords.split(',')]
                normalized_noticia = normalize_text(prelim["noticia"])
                es_relevante = False
                for keyword in keyword_list:
                    normalized_keyword = normalize_text(keyword)
                    if normalized_keyword in normalized_noticia:
                        es_relevante = True
                        break
                if not es_relevante:
                    print(f"⏩ Omitida por no contener keywords: {prelim['noticia']}")
                    continue
                if news_exists_in_gnoticias(prelim["id"], candidato_id):
                    print(f"⚠️ Duplicada en gnoticias (omitida): {prelim['noticia']}")
                    continue
                prelim['id_log'] = log_id
                print(f"-> Relevante. Guardando noticia: {prelim['noticia']}")
                save_news_to_gnoticias(prelim)
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error de red al obtener noticias: {e}")
        except Exception as e:
            print(f"❌ Error inesperado al procesar feed: {e}")
        current_date += timedelta(days=1)
    marcar_candidato_como_procesado(candidato_id, campo="his")


# ==== CONFIGURACIÓN MANUAL ====
# IDs de candidatos a procesar
CANDIDATOS_IDS = [76]  # <-- Edita aquí los IDs deseados
# Fechas de inicio y fin (YYYY-MM-DD)
START_DATE = "2024-01-01"    # <-- Edita aquí la fecha de inicio
END_DATE = "2025-11-22"      # <-- Edita aquí la fecha de fin

def main():
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    log_id = log_start('ex_gnoticias_historico', f'procesando ids {CANDIDATOS_IDS}')
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            for candidato_id in CANDIDATOS_IDS:
                cur.execute("SELECT nombre, keywords FROM candidatos WHERE id_candidato = ?", (candidato_id,))
                row = cur.fetchone()
                if not row:
                    print(f"❌ Candidato con id {candidato_id} no encontrado.")
                    continue
                candidato_nombre, keywords = row
                print(f"✅ Procesando histórico: {candidato_nombre} (ID {candidato_id})")
                fetch_news_for_candidate_historico(candidato_id, candidato_nombre, keywords, start_date, end_date, log_id=log_id)
    except Exception as e:
        print(f"❌ Error inesperado en el procesamiento histórico: {e}")
        log_error_update(log_id, e)
    log_end(log_id, estado='finished', mensaje='Proceso histórico completado.')

if __name__ == "__main__":
    main()

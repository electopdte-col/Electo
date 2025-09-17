import re
import unicodedata
from difflib import SequenceMatcher
# Partículas comunes en apellidos hispanos
STOPWORDS_APELLIDO = {"de", "del", "la", "las", "los", "y", "san", "santa"}

def similarity(a, b):
	"""Similitud usando SequenceMatcher (0-100)."""
	return SequenceMatcher(None, a, b).ratio() * 100

def normalize_text(text):
	"""Normaliza el texto: minúsculas, sin tildes, puntuación como espacios."""
	text = text.lower().strip()
	text = ''.join(
		c for c in unicodedata.normalize('NFD', text)
		if unicodedata.category(c) != 'Mn'
	)
	text = re.sub(r'[^a-z0-9\s]', ' ', text)
	text = re.sub(r'\s+', ' ', text).strip()
	return text

def limpiar_apellidos(tokens):
	"""
	Quita partículas de apellidos (de, del, la, etc.)
	"""
	return [t for t in tokens if t not in STOPWORDS_APELLIDO]

def buscar_nombre_en_titular(nombre, titular, umbral_nombre=85, umbral_fuzzy=85):
	nombre_norm = normalize_text(nombre)
	titular_norm = normalize_text(titular)

	partes = nombre_norm.split()
	if len(partes) < 2:
		return (None, 0, "nombre_invalido")

	# Quitar partículas de los apellidos
	partes_limpias = limpiar_apellidos(partes)

	nombre_simple = partes_limpias[0]
	apellido_paterno = partes_limpias[-2] if len(partes_limpias) >= 3 else partes_limpias[-1]
	apellido_materno = partes_limpias[-1]
	apellido_compuesto = " ".join(partes_limpias[-2:]) if len(partes_limpias) >= 3 else None

	# --- Coincidencias exactas prioritarias ---
	if apellido_compuesto and apellido_compuesto in titular_norm:
		return (apellido_compuesto, 100, "apellido_compuesto")

	if f"{nombre_simple} {apellido_paterno}" in titular_norm:
		return (f"{nombre_simple} {apellido_paterno}", 100, "nombre_apellido_paterno")

	if f"{nombre_simple} {apellido_materno}" in titular_norm:
		return (f"{nombre_simple} {apellido_materno}", 100, "nombre_apellido_materno")

	if apellido_paterno in titular_norm:
		return (apellido_paterno, 100, "apellido_paterno")

	if apellido_materno in titular_norm:
		return (apellido_materno, 100, "apellido_materno")

	# --- Nombre completo por similitud ---
	score_full = similarity(nombre_norm, titular_norm)
	if score_full >= umbral_nombre:
		return (nombre_norm, score_full, "nombre_completo")

	# --- Comparación fuzzy por tokens ---
	candidatos = []
	for palabra in titular_norm.split():
		for clave, tipo in [
			(apellido_paterno, "fuzzy_apellido_paterno"),
			(apellido_materno, "fuzzy_apellido_materno"),
			(nombre_simple, "fuzzy_nombre"),
		]:
			score = similarity(clave, palabra)
			if score >= umbral_fuzzy:
				candidatos.append((palabra, score, tipo))

	if candidatos:
		return max(candidatos, key=lambda x: x[1])

	return (None, score_full, "no_match")

import sqlite3
import feedparser
import hashlib
import time
import json
import urllib.parse
from datetime import datetime, timedelta
from googlenewsdecoder import gnewsdecoder
import requests # Importa requests para un mejor manejo de errores

# Funciones de DB movidas a `db_gnoticias.py`
from gnoticias.db_gnoticias import (
	get_db_connection,
	news_exists_dia,
	save_news_to_db_dia,
	marcar_candidato_como_procesado,
	reset_candidatos_news,
)
# Copia de ex_gnoticias.py

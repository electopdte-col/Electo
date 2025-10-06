from pytrends.request import TrendReq
import pandas as pd
import time

pytrends = TrendReq(hl="es-ES", tz=360)

# ID del topic (lo tomé de tu URL)
TOPIC_ID = "/g/11y1qxyk48"

pytrends.build_payload(
    kw_list=[TOPIC_ID],
    timeframe="2025-02-02 2025-02-09",
    geo="CO"
)

# Espera breve para evitar bloqueos
time.sleep(5)  # Increased sleep time

related_queries = pytrends.related_queries()
if related_queries and TOPIC_ID in related_queries:
    print("\n=== Consultas TOP ===")
    print(related_queries[TOPIC_ID]["top"].head(5))
    print("\n=== Consultas RISING ===")
    print(related_queries[TOPIC_ID]["rising"].head(5))
else:
    print("⚠️ No hay consultas relacionadas.")

time.sleep(5) # Increased sleep time

try:
    related_topics = pytrends.related_topics()
    if related_topics and TOPIC_ID in related_topics:
        print("\n=== Temas TOP ===")
        print(related_topics[TOPIC_ID]["top"].head(5))
        print("\n=== Temas RISING ===")
        print(related_topics[TOPIC_ID]["rising"].head(5))
    else:
        print("⚠️ No hay temas relacionados.")
except Exception as e:
    print(f"⚠️ Error en related_topics: {e}")
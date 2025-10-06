# interes_diario.py
from pytrends.request import TrendReq
import pandas as pd
import sqlite3
from datetime import datetime
import random
import time
import uuid

DB_PATH = "data/gtrends.db"
TABLE_INTERES = "gt_interes"
TABLE_QUERIES = "gt_queries"
TABLE_TOPICS = "gt_topics"
TABLE_CANDIDATOS = "candidatos"

# Obtiene candidatos activos desde la base SQLite
def get_candidatos_activos_sqlite():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"SELECT nombre, id_gtrend, id_candidato FROM {TABLE_CANDIDATOS} WHERE activo = 1")
    rows = cur.fetchall()
    conn.close()
    return [{"nombre": r[0], "id_gtrend": r[1], "id_candidato": r[2]} for r in rows]

candidatos_db = get_candidatos_activos_sqlite()
CANDIDATOS_GTREND = {c['nombre']: c['id_gtrend'] for c in candidatos_db}
CANDIDATOS_ID = {c['nombre']: c['id_candidato'] for c in candidatos_db}

# Busca el nombre y el id_gtrend del pivote por id_candidato=44
PIVOTE_ID_CANDIDATO = 44
PIVOTE_NAME = next(c['nombre'] for c in candidatos_db if c['id_candidato'] == PIVOTE_ID_CANDIDATO)
PIVOTE_ID = next(c['id_gtrend'] for c in candidatos_db if c['id_candidato'] == PIVOTE_ID_CANDIDATO)

# ====== FUNCIONES ======
def get_trends_data(candidatos_gtrend, timeframe="today 7-d", geo="CO"):
    # Excluye el pivote de los candidatos
    candidatos_sin_pivote = {k: v for k, v in candidatos_gtrend.items() if k != PIVOTE_NAME}
    chunks = [list(candidatos_sin_pivote.items())[i:i+4] for i in range(0, len(candidatos_sin_pivote), 4)]
    df_final = pd.DataFrame()
    for idx, chunk in enumerate(chunks, start=1):
        kws = [PIVOTE_ID] + [c[1] for c in chunk]
        names = [PIVOTE_NAME] + [c[0] for c in chunk]
        print(f"⏳ Consultando batch {idx}/{len(chunks)}: {names}")
        pytrends = TrendReq(hl="es-CO", tz=360, timeout=(10, 30))
        pytrends.build_payload(kws, timeframe=timeframe, geo=geo)
        df = pytrends.interest_over_time().drop(columns="isPartial")
        df.columns = names
        # 🔹 Normalizar: cada candidato / pivote
        for name in names[1:]:
            df[name] = (df[name] / df[PIVOTE_NAME].replace(0, 1)) * 100
        # Solo en el primer batch se guarda el pivote
        if idx == 1:
            df_batch = df
        else:
            df_batch = df.drop(columns=[PIVOTE_NAME])
        if df_final.empty:
            df_final = df_batch
        else:
            df_final = df_final.join(df_batch, how="outer")
        wait = random.uniform(8, 15)
        print(f"😴 Esperando {wait:.2f} segundos antes de la siguiente llamada...")
        time.sleep(wait)
    return df_final.reset_index()

def insertar_gt_interes(df, tipo, id_log=None):
    conn = sqlite3.connect(DB_PATH)
    df_long = df.melt(id_vars=["date"], var_name="nombre", value_name="valor")
    sql = f"""
        INSERT INTO {TABLE_INTERES} (id_candidato, fecha, valor, tipo, id_log)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id_candidato, fecha) DO UPDATE SET
            valor=excluded.valor,
            tipo=excluded.tipo,
            id_log=excluded.id_log
    """
    data = []
    for _, row in df_long.iterrows():
        data.append((
            CANDIDATOS_ID[row["nombre"]],
            row["date"].date() if hasattr(row["date"], 'date') else row["date"],
            int(row["valor"]),
            tipo,
            id_log
        ))
    conn.executemany(sql, data)
    conn.commit()
    conn.close()
    print(f"✅ Insertados/actualizados {len(data)} registros en {TABLE_INTERES} ({tipo})")


def log_ejecucion(proceso, estado, mensaje=None, fecha_inicio=None, fecha_fin=None, id_log=None):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if not id_log:
        id_log = str(uuid.uuid4())
    if not fecha_inicio:
        fecha_inicio = datetime.now().isoformat()
    sql = f"""
        INSERT OR REPLACE INTO log_ejecucion (id, proceso, estado, mensaje, fecha_inicio, fecha_fin)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, (id_log, proceso, estado, mensaje, fecha_inicio, fecha_fin))
    conn.commit()
    conn.close()
    return id_log

def existe_log_interes_ok(proceso):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM log_ejecucion WHERE proceso=? AND estado='INTERES_OK'", (proceso,))
    existe = cur.fetchone()[0] > 0
    conn.close()
    return existe

def cargar_interes_desde_db(tipo):
    conn = sqlite3.connect(DB_PATH)
    sql = f"SELECT fecha, id_candidato, valor FROM {TABLE_INTERES} WHERE tipo=?"
    df = pd.read_sql_query(sql, conn, params=(tipo,))
    conn.close()
    # Reconstruye formato esperado para queries/topics
    candidatos_db = get_candidatos_activos_sqlite()
    id_to_nombre = {c['id_candidato']: c['nombre'] for c in candidatos_db}
    df['nombre'] = df['id_candidato'].map(id_to_nombre)
    df = df.pivot(index='fecha', columns='nombre', values='valor').reset_index()
    df['date'] = pd.to_datetime(df['fecha'])
    df = df.drop(columns=['fecha'])
    return df

# ====== MAIN ======
if __name__ == "__main__":
    proceso = "interes_dia"
    id_log = log_ejecucion(proceso, "INICIADO")
    try:
        if existe_log_interes_ok(proceso):
            print("⚠️ Interés ya cargado, usando datos de la base...")
            df = cargar_interes_desde_db('diario')
        else:
            df = get_trends_data(CANDIDATOS_GTREND)
            insertar_gt_interes(df, 'diario', id_log)
            log_ejecucion(proceso, "INTERES_DIA_OK", mensaje="Carga de interés completada", fecha_inicio=None, fecha_fin=datetime.now().isoformat(), id_log=id_log)
        log_ejecucion(proceso, "FINALIZADO", mensaje="OK", fecha_inicio=None, fecha_fin=datetime.now().isoformat(), id_log=id_log)
    except Exception as e:
        log_ejecucion(proceso, "ERROR", mensaje=str(e), fecha_inicio=None, fecha_fin=datetime.now().isoformat(), id_log=id_log)
        raise

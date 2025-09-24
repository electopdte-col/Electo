import sqlite3
from datetime import date

DB_PATH = "data/Eto_col26.db"

def get_db_connection():
    """Crea y retorna una conexión a la base de datos."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_modelos_config():
    """Retorna la configuración de modelos y sus cuotas diarias."""
    # Se puede expandir para incluir más modelos, claves de API, etc.
    return {
        "gemini-1.5-flash": {"quota": 45} # Dejar un margen de 5 por si acaso
        # "otro-modelo": {"quota": 100}
    }

def get_next_available_model():
    """Encuentra el próximo modelo de IA que no ha alcanzado su cuota diaria."""
    today = date.today().isoformat()
    models_config = get_modelos_config()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        for model_name, config in models_config.items():
            cur.execute(
                "SELECT Calls FROM log_IA WHERE modelo = ? AND fecha = ?",
                (model_name, today)
            )
            result = cur.fetchone()
            
            calls_today = result['Calls'] if result else 0
            
            if calls_today < config['quota']:
                return model_name
        
        # Si todos los modelos alcanzaron su cuota
        return None
    finally:
        if conn:
            conn.close()

def log_api_call(model_name):
    """Registra o incrementa una llamada a la API para un modelo en el día actual."""
    today = date.today().isoformat()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Verificar si ya existe un registro para hoy
        cur.execute(
            "SELECT Calls FROM log_IA WHERE modelo = ? AND fecha = ?",
            (model_name, today)
        )
        result = cur.fetchone()
        
        if result:
            # Incrementar el contador
            new_calls = result['Calls'] + 1
            cur.execute(
                "UPDATE log_IA SET Calls = ? WHERE modelo = ? AND fecha = ?",
                (new_calls, model_name, today)
            )
        else:
            # Crear un nuevo registro
            cur.execute(
                "INSERT INTO log_IA (modelo, fecha, Calls) VALUES (?, ?, 1)",
                (model_name, today)
            )
        conn.commit()
    finally:
        if conn:
            conn.close()

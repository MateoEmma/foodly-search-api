import sys
import os

# Obtener la ruta absoluta del directorio del proyecto
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from code.search.engine import SearchEngine
from dotenv import load_dotenv
import logging
import traceback

log_dir = os.path.join(os.path.expanduser("~"), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "foodly_search_api.log")

try:
    # Crear directorio para logs si no existe
    log_dir = os.path.join(os.path.expanduser("~"), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "foodly_search_api.log")

    # Configuración básica (solo para la salida estándar)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Añadir handler para archivo manualmente
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)

    logging.info("Inicializando API de búsqueda Foodly")
except Exception as e:
    print(f"Error al configurar logging: {e}")


# Log inicial para verificar que la configuración funciona
logging.info("Inicializando API de búsqueda Foodly")


# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
CORS(app)

# Cargar configuración de la base de datos desde config.json
try:
    with open('code/cfg/config.json', 'r') as config_file:
        db_config = json.load(config_file)
except Exception as e:
    print(f"Error cargando configuración desde config.json: {e}")
    # Configuración predeterminada
    db_config = {
        'host': os.environ.get('DB_HOST', 'MateoAlvarez.mysql.pythonanywhere-services.com'),
        'user': os.environ.get('DB_USER', 'MateoAlvarez'),
        'password': os.environ.get('DB_PASSWORD', 'foodlywolrd=!'),
        'database': os.environ.get('DB_NAME', 'MateoAlvarez$foodlyDBCloud')
    }

# Logging adicional de configuración
logging.info("Configuración de base de datos:")
for key, value in db_config.items():
    if key.lower() != 'password':  # No loguear contraseña
        logging.info(f"{key}: {value}")

# Inicializar motor de búsqueda
try:
    import socket

    # Logging de información de red
    logging.info(f"Hostname: {socket.gethostname()}")
    logging.info(f"IP Local: {socket.gethostbyname(socket.gethostname())}")

    search_engine = SearchEngine(db_config)

    if not search_engine.test_database_connection():
        raise Exception("No se pudo establecer conexión con la base de datos")
    print("Motor de búsqueda inicializado correctamente")
except Exception as e:
    logging.error(f"Error al inicializar el motor de búsqueda: {str(e)}")
    logging.error(f"Detalles del error: {traceback.format_exc()}")
    print(f"Error al inicializar el motor de búsqueda: {str(e)}")

@app.route('/search', methods=['POST'])
def search():
    print("========== NUEVA BÚSQUEDA INICIADA ==========")
    try:
        data = request.json
        logging.info(f"Solicitud de búsqueda recibida: {data}")

        # Obtener parámetros de la solicitud
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        radius = min(float(data.get('radius', 5)), 50)
        voice_text = data.get('voice_text', '')

        print(f"Parámetros procesados: lat={latitude}, lon={longitude}, radius={radius}, text='{voice_text}'")

        # Preparar coordenadas si se proporcionan
        coordinates = None
        if latitude is not None and longitude is not None:
            coordinates = {
                'latitude': float(latitude),
                'longitude': float(longitude)
            }

        print(f"Coordenadas: {coordinates}")

        print(f"Configuración DB: {db_config}")

        results = {}

        # Si hay texto de voz, usar el procesador de voz
        if voice_text:
            logging.info(f"Procesando búsqueda por voz: '{voice_text}'")
            search_result = search_engine.process_voice_search(
                voice_text=voice_text,
                coordinates=coordinates
            )
            print(f"Resultados del procesamiento de voz: {search_result.get('search_params', {})}")
            results = search_result['results']
        else:
            # Sin texto de voz, realizar búsqueda por ubicación (radio)
            logging.info("Procesando búsqueda por ubicación")
            search_result = search_engine.search_businesses(
                query="",  # Búsqueda vacía para obtener todos los negocios en el radio
                coordinates=coordinates,
                radius=radius
            )
            results = search_result

        print(f"Resultados obtenidos: {results}")


        # Transformar los resultados al formato esperado por Laravel
        businesses = []

        if 'results' in results:
            for business in results['results']:
                businesses.append({
                    'id': business['id'],
                    'name': business['name'],
                    'distance': business.get('distance_km', 0),
                    'score': business.get('relevance', 1.0)
                })

        logging.info(f"Respuesta enviada: {len(businesses)} negocios encontrados")

        # Estructura de respuesta esperada por Laravel
        response = {
            'success': True,
            'businesses': businesses
        }


        return jsonify(response)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error en búsqueda: {str(e)}\n{error_details}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar que la API está funcionando"""
    return jsonify({
        'status': 'healthy',
        'version': '1.0.0'
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(debug=debug, host='0.0.0.0', port=port)
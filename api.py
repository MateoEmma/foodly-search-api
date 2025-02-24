import sys
import os

# Obtener la ruta absoluta del directorio del proyecto
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from .code.search.engine import SearchEngine
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
CORS(app)

# Cargar configuración de la base de datos desde config.json
try:
    with open('code/cfg/config.json', 'r') as config_file:
        db_config = json.load(config_file)
except Exception as e:
    print(f"Error cargando configuración: {e}")
    # Configuración predeterminada
    db_config = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'user': os.environ.get('DB_USER', 'foodly_user'),
        'password': os.environ.get('DB_PASSWORD', 'foodly_password'),
        'database': os.environ.get('DB_NAME', 'foodly_db')
    }

# Inicializar motor de búsqueda
try:
    search_engine = SearchEngine(db_config)
    print("Motor de búsqueda inicializado correctamente")
except Exception as e:
    print(f"Error al inicializar el motor de búsqueda: {str(e)}")

@app.route('/search', methods=['POST'])
def search():
    try:
        data = request.json

        # Obtener parámetros de la solicitud
        latitude = data.get('latitude')
        longitude = data.get('longitude')
        radius = min(float(data.get('radius', 5)), 50)
        voice_text = data.get('voice_text', '')

        # Preparar coordenadas si se proporcionan
        coordinates = None
        if latitude is not None and longitude is not None:
            coordinates = {
                'latitude': float(latitude),
                'longitude': float(longitude)
            }

        results = {}

        # Si hay texto de voz, usar el procesador de voz
        if voice_text:
            search_result = search_engine.process_voice_search(
                voice_text=voice_text,
                coordinates=coordinates
            )
            results = search_result['results']
        else:
            # Sin texto de voz, realizar búsqueda por ubicación (radio)
            search_result = search_engine.search_businesses(
                query="",  # Búsqueda vacía para obtener todos los negocios en el radio
                coordinates=coordinates,
                radius=radius
            )
            results = search_result

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
import sys
import os

# Configurar el path para encontrar los módulos
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# Manejo robusto de NLTK
def setup_nltk():
    """Configurar NLTK con manejo de errores"""
    import ssl
    import nltk
    
    try:
        # Configurar SSL para descargas de NLTK
        try:
            _create_unverified_https_context = ssl._create_unverified_context
        except AttributeError:
            pass
        else:
            ssl._create_default_https_context = _create_unverified_https_context
        
        # Verificar si los datos ya existen
        try:
            nltk.data.find('tokenizers/punkt')
            logging.info("NLTK data already available")
        except LookupError:
            logging.info("Downloading NLTK data...")
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('punkt_tab', quiet=True)
            logging.info("NLTK data downloaded successfully")
            
    except Exception as e:
        logging.error(f"Error setting up NLTK: {e}")
        raise

# Configurar NLTK
setup_nltk()

from code.search.engine import SearchEngine  # Sin el punto inicial
from dotenv import load_dotenv
import logging
import traceback
import datetime
import mysql.connector

log_dir = os.environ.get('LOG_DIR', os.path.join(os.path.expanduser("~"), "logs"))
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Para CloudWatch
        logging.FileHandler(log_file)  # Archivo local
    ]
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

# Después de: app = Flask(__name__)
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            return obj.strftime('%H:%M:%S')
        return super().default(obj)

app.json_encoder = CustomJSONEncoder

@app.errorhandler(Exception)
def handle_exception(e):
    """Manejador global de excepciones para la aplicación"""
    logging.error(f"Error no capturado: {str(e)}")
    logging.error(traceback.format_exc())
    return jsonify({
        'success': False,
        'message': f'Error del servidor: {str(e)}'
    }), 500

# Cargar configuración de la base de datos desde config.json
try:
    with open('code/cfg/config.json', 'r') as config_file:
        db_config = json.load(config_file)
        # Si estamos en JSON, asegurarse que tenga la estructura correcta
        if 'database' in db_config:
            db_config = db_config['database']
except Exception as e:
    logging.warning(f"Error cargando configuración desde config.json: {e}")
    # Configuración desde variables de entorno (prioridad para AWS)
    db_config = {
        'host': os.environ.get('DB_HOST', 'database-1.cfisa6se87ao.us-east-1.rds.amazonaws.com'),
        'user': os.environ.get('DB_USER', 'admin'),
        'password': os.environ.get('DB_PASSWORD', 'foodly98765='),
        'database': os.environ.get('DB_NAME', 'foodlydb')
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


#Funcion para serializar objetos datetime
def json_serializer(obj):
    '''JSON serializer para objetos datetime y otros tipos no serializables'''
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        return obj.strftime('%H:%M:%S')
    # Fallback para otros tipos
    try:
        return str(obj)
    except:
        return None

def convert_datetime_objects(obj):
    """Convierte recursivamente todos los objetos datetime a strings"""
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, datetime.time):
        return obj.strftime('%H:%M:%S')
    elif isinstance(obj, dict):
        return {k: convert_datetime_objects(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_objects(item) for item in obj]
    return obj

# En api.py - Función para obtener detalles de servicios por IDs:
def get_services_by_ids(service_ids, db_config):
    """
    Obtiene los detalles de servicios basado en una lista de IDs
    """
    if not service_ids or not isinstance(service_ids, list) or len(service_ids) == 0:
        return []

    try:
        # Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Convertir lista de IDs a string para consulta SQL
        ids_str = ','.join([str(id) for id in service_ids])

        # Consulta para obtener detalles de servicios
        query = f"SELECT id, service_uuid, service_name FROM services WHERE id IN ({ids_str})"

        print(f"Consulta de servicios: {query}")

        cursor.execute(query)
        services = cursor.fetchall()

        print(f"Servicios encontrados: {len(services)}")

        return services
    except Exception as e:
        print(f"Error obteniendo servicios: {e}")
        return []
    finally:
        if 'conn' in locals() and conn and conn.is_connected():
            cursor.close()
            conn.close()

#Funcion para obtener horarios de apertura e cierre de un negocio
def get_business_hours(business_id, db_config):
    """"
    Obtiene los horarios de apertura y cierre de un negocio dado su ID
    """
    try:
        #Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        #Consulta para obtener horarios
        query = '''
            SELECT day,
                   open_a,
                   close_a,
                   open_b,
                   close_b
            FROM business_hours
            WHERE business_id = %s
            '''

        cursor.execute(query, (business_id,))
        hours = cursor.fetchall()

        #Formatear los horarios en el formato esperado
        formatted_hours = {}
        for hour in hours:
            day_key = f'day_{hour["day"]}'
            day_data = {}

            if hour['open_a']:
                day_data['open_a'] = hour['open_a'].strftime('%H:%M') if isinstance(hour['open_a'], datetime.time) else hour['open_a']
            if hour['close_a']:
                day_data['close_a'] = hour['close_a'].strftime('%H:%M') if isinstance(hour['close_a'], datetime.time) else hour['close_a']
            if hour['open_b']:
                day_data['open_b'] = hour['open_b'].strftime('%H:%M') if isinstance(hour['open_b'], datetime.time) else hour['open_b']
            if hour['close_b']:
                day_data['close_b'] = hour['close_b'].strftime('%H:%M') if isinstance(hour['close_b'], datetime.time) else hour['close_b']

            formatted_hours[day_key] = day_data

        #Asegurarse de que todos los dias esten presentes
        for i in range(7):
            day_key = f'day_{i}'
            if day_key not in formatted_hours:
                formatted_hours[day_key] = {}

        return formatted_hours
    except Exception as e:
        print(f'Error obteniendo horarios: {e}')
        return {}
    finally:
        if 'conn' in locals() and conn and conn.is_connected():
            cursor.close()
            conn.close()

#Funcion para obtener la categoria de un negocio
def get_business_category(category_id, db_config):
    '''
    Obtiene la categoria de un negocio dado su ID
    '''
    try:
        #Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        #Consulta para obtener la categoria
        query = """
            SELECT id, category_uuid, category_name
            FROM categories
            WHERE id = %s
        """

        cursor.execute(query, (category_id,))
        category = cursor.fetchone()

        if not category:
            return None

        #Añadir subcategorias vacias como en el ejemplo
        category['subcategories'] = []

        #Añadir campos adicionales si son necesarios
        category['category_image_path'] = "https://foodly.s3.amazonaws.com/public/categories_images/default.jpg"

        return category
    except Exception as e:
        print(f'Error obteniendo categoria: {e}')
        return None
    finally:
        if 'conn' in locals() and conn and conn.is_connected():
            cursor.close()
            conn.close()

#Funcion para obtener los menús de un negocio
def get_business_menus(business_id, db_config):
    '''
    Obtiene los menús de un negocio dado su ID
    '''
    try:
        #Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        #Consulta para obtener los menús
        query = """
            SELECT id, uuid, business_id
            FROM business_menus
            WHERE business_id = %s
        """

        cursor.execute(query, (business_id,))
        menus = cursor.fetchall()

        #Obtener el UUID del negocio para incluirlo en la respuesta
        business_uuid_query = '''SELECT business_uuid FROM businesses WHERE id = %s'''
        cursor.execute(business_uuid_query, (business_id,))
        business_result = cursor.fetchone()
        business_uuid = business_result['business_uuid'] if business_result else None

        #Formatesar los menus en el formato esperado
        formatted_menus = []

        for menu in menus:
            formatted_menus.append({
                'id': menu['id'],
                'uuid': menu['uuid'],
                'business_uuid': business_uuid,
            })

        return formatted_menus
    except Exception as e:
        print(f'Error obteniendo menus: {e}')
        return []
    finally:
        if 'conn' in locals() and conn and conn.is_connected():
            cursor.close()
            conn.close()

#Funcion para obtener las imagenes de portada de un negocio
def get_business_cover_images(business_id, db_config):
    '''
    Obtiene las imagenes de portada de un negocio dado su ID
    '''
    try:
        #Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        #Consulta para obtener las imagenes
        query = """
            SELECT id, business_image_uuid, business_image_path
            FROM business_cover_images
            WHERE business_id = %s
        """

        cursor.execute(query, (business_id,))
        images = cursor.fetchall()

        return images
    except Exception as e:
        print(f"Error obteniendo imágenes de portada: {e}")
        return []
    finally:
        if 'conn' in locals() and conn and conn.is_connected():
            cursor.close()
            conn.close()


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
                try:
                    # Imprimir el business crudo para depuración
                    print(f"Business raw: {json.dumps(business, default=str)}")

                    # Procesar IDs de servicios
                    business_services = []
                    # Procesar IDs de servicios SOLO si existen y no son null
                    if 'service_ids' in business and business['service_ids']:
                        try:
                            # Convertir service_ids de string a lista de enteros
                            service_ids = [int(id.strip()) for id in business['service_ids'].split(',') if id.strip()]

                            if service_ids:  # Solo procesar si hay IDs válidos
                                # Obtener detalles de servicios desde la base de datos
                                services = get_services_by_ids(service_ids, db_config)

                                # Preparar la lista de servicios para la respuesta
                                for service in services:
                                    business_services.append({
                                        "id": service['id'],
                                        "service_uuid": service.get('service_uuid', f"service-{service['id']}"),
                                        "service_name": service.get('service_name', f"Service {service['id']}")
                                    })
                        except Exception as service_error:
                            print(f"Error procesando service_ids: {service_error}")
                            # Continuar con business_services como lista vacía si hay error

                     # Obtener categoría completa
                    category = convert_datetime_objects(get_business_category(business.get('category_id'), db_config))

                    # Obtener horarios de apertura
                    business_hours = convert_datetime_objects(get_business_hours(business['id'], db_config))

                    # Obtener menús del negocio
                    business_menus = convert_datetime_objects(get_business_menus(business.get('id', ''), db_config))

                    # Obtener imágenes de portada
                    cover_images = convert_datetime_objects(get_business_cover_images(business['id'], db_config))

                    # Crear estructura del negocio según el formato esperado
                    business_data = {
                        'id': business['id'],
                        'user_id': business.get('user_id', 1),
                        'business_uuid': business.get('business_uuid', f"business-{business['id']}"),
                        'business_logo': business.get('business_logo', ''),
                        'business_name': business['name'],
                        'business_email': business.get('email', ''),
                        'business_phone': business.get('phone', ''),
                        'business_about_us': business.get('business_about_us', ''),
                        'business_services': business_services,
                        'business_additional_info': business.get('business_additional_info', ''),
                        'business_address': business.get('address', ''),
                        'business_zipcode': business.get('business_zipcode', ''),
                        'business_city': business.get('business_city', ''),
                        'business_country': business.get('business_country', ''),
                        'business_website': business.get('business_website', ''),
                        'business_latitude': business.get('latitude', 0),
                        'business_longitude': business.get('longitude', 0),
                        'business_menus': business_menus,
                        'category_id': business.get('category_id', 0),
                        'category': category or {},
                        'business_opening_hours': business_hours,
                        #'created_at': business.get('created_at', datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if isinstance(business.get('created_at'), datetime.datetime) else business.get('created_at', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        #'updated_at': business.get('updated_at', datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') if isinstance(business.get('updated_at'), datetime.datetime) else business.get('updated_at', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                        'cover_images': cover_images or [],
                        'business_promotions': [],
                        'business_branches': []
                    }

                    if 'distance_km' in business:
                        business_data['distance'] = round(business['distance_km'], 2)
                    elif 'distance' in business:
                        business_data['distance'] = round(business['distance'], 2)
                    else:
                        business_data['distance'] = 0.0

                    if 'relevance' in business:
                        business_data['score'] = float(business['relevance'])
                    else:
                        business_data['score'] = 1.0

                    businesses.append(business_data)
                except Exception as e:
                    print(f'Error procesando negocio {business.get("id", "unknown")}: {e}')
                    logging.error(f"Error procesando negocio {business.get('id', 'unknown')}: {e}")

        logging.info(f"Respuesta enviada: {len(businesses)} negocios encontrados")

        # Convertir todos los objetos datetime a strings
        businesses = convert_datetime_objects(businesses)

        # Estructura de respuesta esperada por Laravel
        response = {
            "business": businesses,  # Usar "business" en lugar de "businesses"
            "success": True
        }

        response_json_string = json.dumps(response, default=lambda o: o.isoformat() if isinstance(o, (datetime.datetime, datetime.date, datetime.time)) else str(o))
        parsed_response = json.loads(response_json_string)


        return jsonify(parsed_response)


    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error en búsqueda: {str(e)}\n{error_details}")
        logging.error(f"Error en búsqueda: {str(e)}\n{error_details}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}'
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint para verificar que la API está funcionando"""
    try:
        # Verificar conexión a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'version': '1.0.0',
            'database': 'connected',
            'search_engine': 'available' if search_engine else 'unavailable'
        })
    except Exception as e:
        logging.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

# Para Elastic Beanstalk
application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    application.run(debug=False, host='0.0.0.0', port=port)
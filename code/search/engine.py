from typing import Dict, List, Optional
import mysql.connector
from mysql.connector import Error
import time
import json
from datetime import datetime
from .querys import STATS_GENERAL_QUERY, TOP_SEARCHES_QUERY, HOURLY_DISTRIBUTION_QUERY
from .text_processor import TextProcessor
import logging

class SearchEngine:
    def __init__(self, db_config: Dict):
        """
        Inicializa el motor de búsqueda con la configuración de la base de datos
        """
        self.db_config = db_config
        self.text_processor = TextProcessor()

        self.test_database_connection()

    def test_database_connection(self):
        """
        Método de prueba de conexión detallado
        """
        try:
            import socket

            # Información de red detallada
            logging.info("Información de red:")
            try:
                local_hostname = socket.gethostname()
                local_ip = socket.gethostbyname(local_hostname)
                logging.info(f"Hostname local: {local_hostname}")
                logging.info(f"IP local: {local_ip}")
            except Exception as network_error:
                logging.error(f"Error obteniendo información de red: {network_error}")

            # Intentar establecer conexión
            logging.info("Iniciando prueba de conexión a base de datos")

            # Información detallada de configuración
            logging.info("Detalles de conexión:")
            for key, value in self.db_config.items():
                if key.lower() != 'password':
                    logging.info(f"{key}: {value}")

            # Verificar resolución DNS
            try:
                host_ip = socket.gethostbyname(self.db_config['host'])
                logging.info(f"IP del host de base de datos: {host_ip}")
            except socket.gaierror as dns_error:
                logging.error(f"Error de resolución DNS: {dns_error}")

            # Prueba de conexión por socket
            try:
                test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_socket.settimeout(10)
                result = test_socket.connect_ex((
                    self.db_config['host'],
                    self.db_config.get('port', 3306)
                ))

                if result == 0:
                    logging.info("Puerto de base de datos está abierto")
                else:
                    logging.error("No se puede conectar al puerto de base de datos")

                test_socket.close()
            except Exception as socket_error:
                logging.error(f"Error de conexión por socket: {socket_error}")

            # Conexión MySQL
            import mysql.connector
            conn = mysql.connector.connect(**self.db_config)

            # Crear cursor
            cursor = conn.cursor()

            # Consulta de prueba simple
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

            logging.info(f"Resultado de consulta de prueba: {result}")

            # Cerrar cursor y conexión
            cursor.close()
            conn.close()

            logging.info("Conexión a base de datos establecida exitosamente")
            return True

        except mysql.connector.Error as err:
            # Logging detallado de errores
            logging.error("Error de conexión a base de datos:")
            logging.error(f"Tipo de error: mysql.connector.Error")
            logging.error(f"Código de error: {err.errno}")
            logging.error(f"Mensaje de error: {err}")
            logging.error(f"SQL State: {err.sqlstate}")

            # Análisis de errores comunes
            error_messages = {
                1045: "Error de autenticación: Verifica usuario y contraseña",
                2003: "Error de conexión: Problema de red o firewall",
                2005: "Error de host: Verifica la dirección del servidor"
            }

            logging.error(error_messages.get(err.errno, "Error desconocido"))

            return False
        except socket.error as sock_err:
            logging.error(f"Error de socket: {sock_err}")
            return False
        except Exception as e:
            # Capturar cualquier otro error
            logging.error(f"Error inesperado en conexión a base de datos:")
            logging.error(f"Tipo de error: {type(e)}")
            logging.error(f"Detalles: {e}")
            return False

    def process_voice_search(self, voice_text: str, coordinates: Optional[Dict] = None) -> Dict:
        """
        Procesa una búsqueda de voz y la ejecuta
        """
        logging.info(f"Iniciando procesamiento de búsqueda de voz: '{voice_text}'")
        logging.info(f"Coordenadas proporcionadas: {coordinates}")

        search_params = self.text_processor.process_voice_query(
            text=voice_text,
            coordinates=coordinates
        )
        logging.info(f"Parámetros de búsqueda procesados: {search_params}")

        results = self.search_businesses(
            query=search_params['query'],
            filters=search_params['filters'],
            coordinates=search_params['coordinates'],
            radius=5.0
        )

        if isinstance(results, dict) and 'results' in results:
            logging.info(f"Búsqueda completada. Resultados: {len(results.get('results', []))} negocios encontrados")
        else:
            logging.warning(f"Estructura de resultados inesperada: {type(results)}")

        return {
            'results': results,
            'search_params': search_params
        }

    def search_businesses(
    self,
    query: str,
    filters: Optional[Dict] = None,
    coordinates: Optional[Dict] = None,
    radius: float = 5.0,
    page: int = 1,
    per_page: int = 20
) -> Dict:
        """
        Realiza búsqueda de negocios
        """
        logging.info("=" * 50)
        logging.info("Iniciando búsqueda de negocios")
        logging.info(f"Parámetros de búsqueda:")
        logging.info(f"Query: '{query}'")
        logging.info(f"Filtros: {filters}")
        logging.info(f"Coordenadas: {coordinates}")
        logging.info(f"Radio: {radius} km")
        logging.info(f"Página: {page}, Resultados por página: {per_page}")

        start_time = time.time()

        try:
            # Diagnóstico de red detallado
            import socket
            import requests

            try:
                logging.info("Diagnóstico de conectividad de red:")

                # Hostname y IP local
                local_hostname = socket.gethostname()
                local_ip = socket.gethostbyname(local_hostname)
                logging.info(f"Hostname local: {local_hostname}")
                logging.info(f"IP local: {local_ip}")

                # IP pública
                try:
                    public_ip = requests.get('https://api.ipify.org').text
                    logging.info(f"IP pública: {public_ip}")
                except Exception as ip_error:
                    logging.error(f"Error obteniendo IP pública: {ip_error}")

                # Resolución DNS del host de base de datos
                try:
                    host_ip = socket.gethostbyname(self.db_config['host'])
                    logging.info(f"IP del host de base de datos: {host_ip}")
                except socket.gaierror as dns_error:
                    logging.error(f"Error de resolución DNS: {dns_error}")

                # Prueba de conexión por socket
                try:
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(10)
                    result = test_socket.connect_ex((
                        self.db_config['host'],
                        self.db_config.get('port', 3306)
                    ))

                    if result == 0:
                        logging.info("Puerto de base de datos está abierto")
                    else:
                        logging.error("No se puede conectar al puerto de base de datos")

                    test_socket.close()
                except Exception as socket_error:
                    logging.error(f"Error de conexión por socket: {socket_error}")

            except Exception as network_error:
                logging.error(f"Error en diagnóstico de red: {network_error}")

            # Logging de configuración de base de datos
            logging.info("Configuración de conexión a base de datos:")
            for key, value in self.db_config.items():
                if key.lower() != 'password':
                    logging.info(f"{key}: {value}")

            # Intentar establecer conexión
            conn = mysql.connector.connect(**self.db_config)
            logging.info("Conexión a la base de datos establecida correctamente")
            cursor = conn.cursor(dictionary=True)


            # Calcular offset para paginación
            offset = (page - 1) * per_page

            # Construir consulta base
            sql = """
                SELECT DISTINCT
                    b.id,
                    b.business_name as name,
                    b.business_about_us as description,
                    b.business_address as address,
                    b.business_email as email,
                    b.business_phone as phone,
                    b.business_latitude as latitude,
                    b.business_longitude as longitude,
                    c.id as category_id,
                    c.category_name
            """

            params = []

            # Añadir distancia si hay coordenadas
            if coordinates:
                sql += """,
                    ST_Distance_Sphere(
                    point(b.business_longitude, b.business_latitude),
                    point(%s, %s)
                    ) * 0.001 as distance_km
                    """
                params.extend([
                    coordinates['longitude'],
                    coordinates['latitude']
                ])

            # Para la cláusula de relevancia
            if query and query.strip():
                # Si hay consulta, usar MATCH AGAINST para relevancia
                sql += """
                    , MATCH(b.business_name) AGAINST(%s IN NATURAL LANGUAGE MODE) as relevance
                """
                params.append(query)
            else:
                # Si no hay consulta, todos tienen la misma relevancia
                sql += """
                    , 1 as relevance
                """

            # Añadir resto de la consulta
            sql += """
                , GROUP_CONCAT(DISTINCT s.service_name) as services
                FROM
                    businesses b
                    LEFT JOIN categories c ON b.category_id = c.id
                    LEFT JOIN business_service bs ON b.id = bs.business_id
                    LEFT JOIN services s ON bs.service_id = s.id
                WHERE
                    b.deleted_at IS NULL
            """

            # Aplicar filtro de búsqueda por texto si existe
            if query and query.strip():
                sql += " AND MATCH(b.business_name) AGAINST(%s IN NATURAL LANGUAGE MODE)"
                params.append(query)

            # Aplicar filtro de distancia
            if coordinates:
                sql += """
                    AND ST_Distance_Sphere(
                        point(b.business_longitude, b.business_latitude),
                        point(%s, %s)
                    ) * 0.001 <= %s
                """
                params.extend([
                    coordinates['longitude'],
                    coordinates['latitude'],
                    radius
                ])

            # Agregar filtros específicos
            if filters:
                if 'category_id' in filters:
                    sql += " AND b.category_id = %s"
                    params.append(filters['category_id'])

                if 'service_id' in filters:
                    sql += " AND EXISTS (SELECT 1 FROM business_service bs2 WHERE bs2.business_id = b.id AND bs2.service_id = %s)"
                    params.append(filters['service_id'])

            # Agrupar resultados
            sql += " GROUP BY b.id"

            # Decidir orden según si hay coordenadas o consulta
            if coordinates and (not query or not query.strip()):
                # Si solo hay coordenadas, ordenar por distancia
                sql += " ORDER BY distance_km ASC"
            else:
                # Si hay consulta o ambos, ordenar por relevancia
                sql += " ORDER BY relevance DESC"

                # Si también hay coordenadas, usar orden secundario por distancia
                if coordinates:
                    sql += ", distance_km ASC"

            # Añadir paginación
            sql += " LIMIT %s OFFSET %s"
            params.extend([per_page, offset])

            logging.info("Consulta SQL generada:")
            logging.info(sql)
            logging.info("Parámetros de la consulta:")
            for i, param in enumerate(params, 1):
                logging.info(f"Parámetro {i}: {param}")

            try:
                # Ejecutar búsqueda
                cursor.execute(sql, params)
                results = cursor.fetchall()

                # Logging detallado de resultados
                logging.info(f"Número total de resultados: {len(results)}")

                # Loguear detalles de los primeros 5 resultados
                for i, result in enumerate(results[:5], 1):
                    logging.info(f"Resultado {i}:")
                    logging.info(json.dumps(result, indent=2, ensure_ascii=False))

            except mysql.connector.Error as query_error:
                # Logging detallado de errores de consulta
                logging.error("Error al ejecutar la consulta SQL:")
                logging.error(f"Código de error: {query_error.errno}")
                logging.error(f"Mensaje de error: {query_error}")
                logging.error(f"Consulta SQL: {sql}")
                logging.error(f"Parámetros: {params}")

                # Información adicional de errores
                logging.error("Detalles de configuración de conexión:")
                for key, value in self.db_config.items():
                    if key.lower() != 'password':
                        logging.error(f"{key}: {value}")

                raise

            if results is None:
                results = []  # Asegurarse de que results nunca sea None

            end_time = time.time()
            execution_time = int((end_time - start_time) * 1000)  # En milisegundos

            # Formatear y devolver resultados
            return {
                'results': results,
                'stats': {
                    'total_results': len(results),
                    'execution_time_ms': execution_time,
                    'page': page,
                    'per_page': per_page
                }
            }

        except mysql.connector.Error as err:
            # Logging detallado de errores de conexión
            logging.error("Error de conexión a base de datos:")
            logging.error(f"Tipo de error: mysql.connector.Error")
            logging.error(f"Código de error: {err.errno}")
            logging.error(f"Mensaje de error: {err}")

            # Detalles adicionales de configuración
            logging.error("Detalles de configuración de conexión:")
            for key, value in self.db_config.items():
                if key.lower() != 'password':
                    logging.error(f"{key}: {value}")

            # Análisis de errores comunes
            error_messages = {
                1045: "Error de autenticación: Verifica usuario y contraseña",
                2003: "Error de conexión: Problema de red o firewall",
                2005: "Error de host: Verifica la dirección del servidor"
            }

            logging.error(error_messages.get(err.errno, "Error desconocido"))

            return {
                'results': [],
                'stats': {
                    'error': str(err),
                    'total_results': 0,
                    'page': page,
                    'per_page': per_page
                }
            }

        except Exception as e:
            # Capturar cualquier otro error
            logging.error(f"Error inesperado en búsqueda:")
            logging.error(f"Tipo de error: {type(e)}")
            logging.error(f"Detalles: {e}")

            return {
                'results': [],
                'stats': {
                    'error': str(e),
                    'total_results': 0,
                    'page': page,
                    'per_page': per_page
                }
            }


        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
                logging.info("Conexión a la base de datos cerrada")


    def _log_search(
        self,
        cursor,
        query: str,
        filters: Optional[Dict],
        results_count: int,
        execution_time: int
    ):
        """
        Registra la búsqueda en la tabla de logs
        """
        try:
            sql = """
                INSERT INTO search_logs
                (query, filters, results_count, execution_time_ms)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql, (
                query,
                json.dumps(filters) if filters else None,
                results_count,
                execution_time
            ))
        except Error as e:
            print(f"Error al registrar log: {e}")

    def _format_top_searches(self, searches: List[Dict]) -> List[Dict]:
        """Formatea las búsquedas más frecuentes"""
        return [{
            'query': search['query'],
            'frequency': search['frequency'],
            'average_results': round(float(search['avg_results']), 2)
        } for search in searches]

    def get_search_stats(self, days: int = 7, user_id: Optional[str] = None) -> Dict:
        """
        Obtiene estadísticas de búsqueda
        """
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)

            params = [days]
            sql = STATS_GENERAL_QUERY
            if user_id:
                sql += " AND user_id = %s"
                params.append(user_id)

            # Obtener estadísticas generales
            cursor.execute(sql, params)
            general_stats = cursor.fetchone()

            # Obtener top búsquedas
            cursor.execute(TOP_SEARCHES_QUERY, [days])
            top_searches = cursor.fetchall()

            # Obtener distribución horaria
            cursor.execute(HOURLY_DISTRIBUTION_QUERY, [days])
            hourly_distribution = cursor.fetchall()

            return {
                'period': f'Últimos {days} días',
                'general_stats': self._format_general_stats(general_stats),
                'top_searches': self._format_top_searches(top_searches),
                'hourly_distribution': hourly_distribution
            }

        except Error as e:
            print(f"Error obteniendo estadísticas: {e}")
            raise
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def _format_general_stats(self, stats: Dict) -> Dict:
        """Formatea estadísticas generales"""
        return {
            'total_searches': stats['total_searches'],
            'unique_users': stats['unique_users'],
            'average_results': round(float(stats['avg_results']), 2),
            'average_execution_time_ms': round(float(stats['avg_execution_time']), 2),
            'min_execution_time_ms': stats['min_execution_time'],
            'max_execution_time_ms': stats['max_execution_time'],
            'zero_results_percentage': round(
                (stats['zero_results_searches'] / stats['total_searches']) * 100
                if stats['total_searches'] > 0 else 0,
                2
            )
        }
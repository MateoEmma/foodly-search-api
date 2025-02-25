from typing import Dict, List, Optional
import mysql.connector
from mysql.connector import Error
import time
import json
from datetime import datetime
from .querys import STATS_GENERAL_QUERY, TOP_SEARCHES_QUERY, HOURLY_DISTRIBUTION_QUERY
from .text_processor import TextProcessor

class SearchEngine:
    def __init__(self, db_config: Dict):
        """
        Inicializa el motor de búsqueda con la configuración de la base de datos
        """
        self.db_config = db_config
        self.text_processor = TextProcessor()
    
    def process_voice_search(self, voice_text: str, coordinates: Optional[Dict] = None) -> Dict:
        """
        Procesa una búsqueda de voz y la ejecuta
        """
        search_params = self.text_processor.process_voice_query(
            text=voice_text,
            coordinates=coordinates
        )

        results = self.search_businesses(
            query=search_params['query'],
            filters=search_params['filters'],
            coordinates=search_params['coordinates'],
            radius=5.0
        )

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
        Realiza búsqueda de negocios usando FULLTEXT INDEX
        
        Args:
            query: Término de búsqueda
            filters: Diccionario con filtros (category_id, service_id)
            page: Número de página
            per_page: Resultados por página
        """
        start_time = time.time()
        
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            # Calcular offset para paginación
            offset = (page - 1) * per_page
            
            # Construir consulta base
            sql = """
                SELECT DISTINCT
                    b.id,
                    b.name,
                    b.description,
                    b.address,
                    b.email,
                    b.phone,
                    b.latitude,
                    b.longitude,
                    c.id as category_id,
                    c.category_name
            """
            
            if coordinates:
                sql += """,
                    ST_Distance_Sphere(
                     point(b.longitude, b.latitude),
                     point(%s, %s)
                    ) * 0.001 as distance_km
                    """

            sql += """
                MATCH(b.name) AGAINST(%s IN NATURAL LANGUAGE MODE) as relevance,
                GROUP_CONCAT(DISTINCT s.service_name) as services
                FROM
                    businesses b
                    LEFT JOIN categories c ON b.category_id = c.id
                    LEFT JOIN business_service bs ON b.id = bs.business_id
                    LEFT JOIN services s ON bs.service_id = s.id
                WHERE
                    b.deleted_at IS NULL
                    AND MATCH(b.name) AGAINST(%s IN NATURAL LANGUAGE MODE)
                """
            
            params = []

            if coordinates:
                params.extend([
                    coordinates['longitude'],
                    coordinates['latitude']
                ])
        
            params.extend([query, query])

            if coordinates:
                sql += """
                    AND ST_Distance_Sphere(
                        point(b.longitude, b.latitude),
                        point(%s, %s)
                    ) * 0.001 <= %s
                """
                params.extend([
                    coordinates['longitude'],
                    coordinates['latitude'],
                    radius
                ])
            
            # Agregar filtros
            if filters:
                if 'category_id' in filters:
                    sql += " AND b.category_id = %s"
                    params.append(filters['category_id'])
                    
                if 'service_id' in filters:
                    sql += " AND EXISTS (SELECT 1 FROM business_service bs2 WHERE bs2.business_id = b.id AND bs2.service_id = %s)"
                    params.append(filters['service_id'])
            
            # Agrupar resultados
            sql += " GROUP BY b.id"
            
            # Ordenar por relevancia y agregar paginación
            sql += """ 
                ORDER BY relevance DESC
                LIMIT %s OFFSET %s
            """
            params.extend([per_page, offset])
            
            # Ejecutar búsqueda
            cursor.execute(sql, params)
            results = cursor.fetchall()
            
            # Contar total de resultados (sin paginación)
            count_sql = f"""
                SELECT COUNT(DISTINCT b.id) as total
                FROM businesses b
                WHERE b.deleted_at IS NULL
                AND MATCH(b.name) AGAINST(%s IN NATURAL LANGUAGE MODE)
            """
            count_params = [query]
            
            if filters:
                if 'category_id' in filters:
                    count_sql += " AND b.category_id = %s"
                    count_params.append(filters['category_id'])
                if 'service_id' in filters:
                    count_sql += " AND EXISTS (SELECT 1 FROM business_service bs2 WHERE bs2.business_id = b.id AND bs2.service_id = %s)"
                    count_params.append(filters['service_id'])
            
            cursor.execute(count_sql, count_params)
            total_count = cursor.fetchone()['total']
            
            # Calcular estadísticas
            execution_time = int((time.time() - start_time) * 1000)
            stats = {
                'total_results': total_count,
                'page': page,
                'per_page': per_page,
                'total_pages': (total_count + per_page - 1) // per_page,
                'execution_time_ms': execution_time
            }
            
            # Registrar búsqueda en logs
            self._log_search(
                cursor=cursor,
                query=query,
                filters=filters,
                results_count=len(results),
                execution_time=execution_time
            )
            
            conn.commit()
            
            return {
                'results': results,
                'stats': stats
            }
            
        except Error as e:
            print(f"Error en búsqueda: {e}")
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
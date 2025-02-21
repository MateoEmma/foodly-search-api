from flask import Blueprint, request, jsonify
from mysql.connector import Error
from .engine import SearchEngine
from . import DB_CONFIG

search_bp = Blueprint('search', __name__)
search_engine = SearchEngine(DB_CONFIG)

@search_bp.route('/api/search', methods=['GET'])
def search():
    """Endpoint de búsqueda"""
    try:
        # Validar parámetros
        query = request.args.get('q', '')
        if not query:
            return jsonify({'error': 'Query parameter is required'}), 400
        
        # Obtener parámetros
        coordinates = _get_coordinates_from_request()
        filters = _get_filters_from_request()
        pagination = _get_pagination_from_request()
        
        # Realizar búsqueda
        result = search_engine.search_businesses(
            query=query,
            filters=filters,
            coordinates=coordinates,
            radius=float(request.args.get('radius', 5.0)),
            **pagination
        )
        
        return jsonify(result)
        
    except Exception as e:
        return _handle_error(e)

@search_bp.route('/api/search/stats', methods=['GET'])
def search_stats():
    """Endpoint para estadísticas de búsqueda"""
    try:
        days = int(request.args.get('days', 7))
        user_id = request.args.get('user_id')
        
        stats = search_engine.get_search_stats(days, user_id)
        return jsonify(stats)
        
    except Exception as e:
        return _handle_error(e)

def _get_coordinates_from_request():
    """Extrae coordenadas de la request"""
    if request.args.get('latitude') and request.args.get('longitude'):
        return {
            'latitude': float(request.args.get('latitude')),
            'longitude': float(request.args.get('longitude'))
        }
    return None

def _get_filters_from_request():
    """Extrae filtros de la request"""
    filters = {}
    if request.args.get('category_id'):
        filters['category_id'] = int(request.args.get('category_id'))
    if request.args.get('service_id'):
        filters['service_id'] = int(request.args.get('service_id'))
    return filters

def _get_pagination_from_request():
    """Extrae parámetros de paginación"""
    return {
        'page': int(request.args.get('page', 1)),
        'per_page': int(request.args.get('per_page', 20))
    }

def _handle_error(error: Exception):
    """Maneja errores de forma consistente"""
    return jsonify({
        'error': str(error),
        'results': [],
        'stats': {
            'total_results': 0,
            'page': 1,
            'per_page': 20
        }
    }), 500
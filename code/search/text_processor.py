from typing import Dict, Optional, List
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import json
import os
import re
import unicodedata

class TextProcessor:
    def __init__(self):
         # Inicializar NLTK
        try:
            nltk.download('punkt')
            nltk.download('stopwords')
            nltk.download('punkt_tab')  # Agregamos este recurso
        except Exception as e:
            print(f"Error downloading NLTK resources: {e}")

        self.stemmer = SnowballStemmer('english')
        self.stop_words = set(stopwords.words('english'))
        self.mappings = self._load_mappings()

        # Preparar stems de keywords
        self.category_stems = self._prepare_categories_stems()
        self.service_stems = self._prepare_services_stems()

    def _load_mappings(self) -> Dict:
        """
        Carga los mapeos desde el json
        """
        try:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'cfg',
                'search_map.json'
            )
            with open(config_path, 'r') as f:
                return json.load(f)

        except Exception as e:
            print(f"Error loading mappings: {e}")
            return {}

    def _prepare_categories_stems(self) -> Dict[str, List[str]]:
        """Prepara los stems de las keywords de categorías"""
        return{
            category_name: [self.stemmer.stem(keyword)
                            for keyword in info['keywords']]
            for category_name, info in self.mappings['categories'].items()
        }

    def _prepare_services_stems(self) -> Dict[str, List[str]]:
        """Prepara los stems de las keywords de servicios"""
        return {
            service_name: [self.stemmer.stem(keyword)
                         for keyword in info['keywords']]
            for service_name, info in self.mappings['services'].items()
        }

    def process_voice_query(self, text: str, coordinates: Optional[Dict] = None) -> Dict:
        """
        Procesa la consulta de voz con sistema de prioridades para ubicación
        MANTIENE TODOS LOS FILTROS EXISTENTES
        """
        # Tokenización y normalización básica
        tokens = word_tokenize(text.lower())
        
        # 1. PRIORIDAD ALTA: Verificar si hay una ubicación específica mencionada
        specific_location_info = self._extract_location_from_text(tokens)
        
        # 2. PRIORIDAD MEDIA: Verificar si debe usar ubicación del usuario
        use_user_location = self._should_use_user_location(tokens)
        
        # 3. Determinar qué coordenadas usar y estrategia de búsqueda
        final_coordinates = None
        location_source = "none"
        cleaned_tokens = tokens
        
        if specific_location_info and not use_user_location:
            # HAY ubicación específica mencionada - NO usar coordenadas del usuario
            location_source = "text_specified"
            final_coordinates = None  # Búsqueda por ciudad, no por coordenadas
            
            # Limpiar tokens removiendo la referencia de ubicación
            cleaned_tokens = self._clean_location_from_tokens(tokens)
            
            print(f"Ubicación específica detectada: {specific_location_info['city_name']}")
            print(f"Cambiando a búsqueda por ciudad, ignorando coordenadas del usuario")
            
        elif use_user_location and coordinates:
            # Usuario quiere buscar cerca de su ubicación actual
            final_coordinates = coordinates
            location_source = "user_location"
            print(f"Usando ubicación del usuario: {coordinates}")
            
        elif coordinates and not specific_location_info:
            # Sin indicaciones específicas, usar coordenadas por defecto
            final_coordinates = coordinates
            location_source = "default_coordinates"
            print(f"Usando coordenadas por defecto: {coordinates}")
        
        # Remover stopwords y aplicar stemming a los tokens limpios
        stemmed_tokens = [
            self.stemmer.stem(token)
            for token in cleaned_tokens
            if token not in self.stop_words
        ]
        
        # MANTENER: Identificar categoría y servicio usando stems
        category_id = self._identify_category(stemmed_tokens)
        service_id = self._identify_service(stemmed_tokens)
        location_context = self._check_location_context(tokens)
        time_info = self._extract_time_info(tokens)  # MANTENER
        meal_time = self._identify_meal_time(tokens)  # MANTENER
        
        # MANTENER: Construir filtros existentes
        filters = {}
        if category_id:
            filters['category_id'] = category_id
        if service_id:
            filters['service_id'] = service_id
        if time_info:
            filters['time'] = time_info
        if meal_time:
            filters['meal_time'] = meal_time
        
        # NUEVO: Añadir filtro de ciudad si se detectó
        if specific_location_info:
            filters['city_name'] = specific_location_info['city_name']
            if specific_location_info.get('city_not_found_in_db', False):
                filters['city_not_found_in_db'] = True
        
        return {
            'query': self._clean_search_text(cleaned_tokens, stemmed_tokens),
            'filters': filters,
            'use_location': bool(final_coordinates),
            'coordinates': final_coordinates,
            'location_source': location_source,
            'specific_location_info': specific_location_info,
            'original_text': text
        }

    def _identify_meal_time(self, tokens: List[str]) -> Optional[Dict]:
        """
        Identifica si se está buscando un momento específico de comida
        """
        text = ' '.join(tokens)
        meal_times = self.mappings['meal_times']

        for  meal, info in meal_times.items():
            if any(keyword in text for keyword in info['keywords']):
                return {
                    'type': meal,
                    'typical_hours': info['typical_hours']
                }
        return None

    def _extract_time_info(self, tokens: List[str]) -> Optional[Dict]:
        """
        Extrae información de horarios del texto.
        Ejemplo: "open from 7 PM" -> {'open_from': '19:00'}
        """
        time_keywords = self.mappings['time']['keywords']
        text = ' '.join(tokens).lower()

        time_info = {}

        number_mapping = {
            'one': '1', 'two': '2', 'three': '3', 'four': '4',
        'five': '5', 'six': '6', 'seven': '7', 'eight': '8',
        'nine': '9', 'ten': '10', 'eleven': '11', 'twelve': '12'
        }

        def convert_to_24(hour: int, period: str) -> str:
            """Convierte hora de 12h a formato 24h"""
            if period == 'pm' and hour != 12:
                return hour + 12
            elif period == 'am' and hour == 12:
                hour = 0
            return f"{hour:02d}:00"

        for i, token in enumerate(tokens):
            if token.isdigit() or token in number_mapping:
                hour = int(token if token.isdigit() else number_mapping[token])

                period = None
                if i + 1 < len(tokens) and tokens[i + 1].lower() in ['am', 'pm']:
                    period = tokens[i + 1].lower()

                context_star = max(0, i - 3)
                context_text = ' '.join(tokens[context_star:i])

                if any(keyword in context_text for keyword in time_keywords['open_from']):
                    time_info['open_from'] = convert_to_24(hour, period or 'am')
                elif any(keyword in context_text for keyword in time_keywords['open_until']):
                    time_info['open_until'] = convert_to_24(hour, period or 'pm')

                elif not time_info:
                    time_info['open_from'] = convert_to_24(hour, period or 'am')

        return time_info if time_info else None

    def _check_location_context(self, tokens: List[str]) -> bool:
        """
        Revisa si el texto contiene información de localización
        """
        location_keywords = self.mappings['location']['keywords']
        has_location_keyword = any(keyword in ' '.join(tokens) for keyword in location_keywords)

        # Palabras específicas que indican ubicación actual
        current_location_indicators = ['me', 'my location', 'current location', 'me', 'here']
        has_current_location = any(indicator in tokens for indicator in current_location_indicators)

        return has_location_keyword or has_current_location

    def _identify_service(self, stemmed_tokens: List[str]) -> Optional[int]:
        """
        Identifica el servicio en el texto
        """
        # Convertir tokens a texto para buscar frases completas
        text = ' '.join(stemmed_tokens)

        for service_name, service_info in self.mappings['services'].items():
            # Buscar coincidencias exactas de frases
            if any(keyword in text for keyword in service_info['keywords']):
                return service_info['id']

            # Buscar coincidencias de palabras compuestas
            compound_matches = [
                keyword for keyword in service_info['keywords']
                if all(word in stemmed_tokens for word in keyword.split())
            ]
            if compound_matches:
                return service_info['id']

        return None

    def _identify_category(self, stemmed_tokens: List[str]) -> Optional[int]:
        """Identifica la categoría usando stems"""
        for category_name, stems in self.category_stems.items():
            if any(token in stems for token in stemmed_tokens):
                return self.mappings['categories'][category_name]['id']
        return None

    def _clean_search_text(self, tokens: List[str], stemmed_tokens: List[str]) -> str:
        """Limpia el texto manteniendo términos relevantes"""
        # Obtener todos los stems a remover
        remove_stems = set()

        # Agregar stems de categorías y servicios
        for stems in self.category_stems.values():
            remove_stems.update(stems)
        for stems in self.service_stems.values():
            remove_stems.update(stems)

        # Agregar stems de ubicación y palabras comunes
        location_stems = [self.stemmer.stem(word)
                        for word in self.mappings['location']['keywords']]
        common_stems = [self.stemmer.stem(word)
                    for word in ['find', 'search', 'looking', 'want', 'get']]

        remove_stems.update(location_stems)
        remove_stems.update(common_stems)

        # Mantener solo palabras relevantes
        clean_tokens = []
        for token, stem in zip(tokens, stemmed_tokens):
            if stem not in remove_stems and token not in self.stop_words:
                clean_tokens.append(token)

        return ' '.join(clean_tokens)
    
    def _extract_location_from_text(self, tokens: List[str]) -> Optional[Dict]:
        """
        Detecta si hay una ciudad específica mencionada en el texto
        """
        text = ' '.join(tokens).lower()
        
        # Patrones que indican ubicación específica
        location_patterns = [
            r'in\s+([a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]{1,}?)(?:\s|$|,|\.|!|\?)',
            r'at\s+([a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]{1,}?)(?:\s|$|,|\.|!|\?)',
            r'near\s+([a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]{1,}?)(?:\s|$|,|\.|!|\?)',
            r'around\s+([a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]{1,}?)(?:\s|$|,|\.|!|\?)',
            r'by\s+([a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]{1,}?)(?:\s|$|,|\.|!|\?)'
        ]
        
        for pattern in location_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                location_name = match.strip()
                
                # Filtrar palabras comunes que NO son ubicaciones
                exclude_words = {
                    'me', 'you', 'here', 'there', 'home', 'work', 
                    'the', 'a', 'an', 'my', 'your', 'this', 'that',
                    'good', 'bad', 'nice', 'great', 'best', 'worst',
                    'order', 'delivery', 'pickup', 'takeaway', 'restaurant',
                    'food', 'place', 'area', 'zone', 'street', 'road'
                }
                
                if (len(location_name) >= 2 and 
                    location_name.lower() not in exclude_words and
                    not any(word in location_name.lower().split() for word in exclude_words)):
                    
                    # Limpiar el nombre de la ciudad
                    clean_city_name = self._clean_city_name(location_name)
                    
                    # Verificar si la ciudad existe en la base de datos
                    verified_city = self._verify_city_exists_in_db(clean_city_name)
                    
                    if verified_city:
                        print(f"Ciudad verificada en DB: '{verified_city}' (detectada: '{clean_city_name}')")
                        return {
                            'location_specified': True, 
                            'city_name': verified_city,
                            'detected_name': clean_city_name,
                            'original_match': location_name,
                            'pattern_used': pattern
                        }
                    else:
                        print(f"Ciudad detectada pero no existe en DB: '{clean_city_name}'")
                        return {
                            'location_specified': True, 
                            'city_name': clean_city_name,
                            'detected_name': clean_city_name,
                            'original_match': location_name,
                            'pattern_used': pattern,
                            'city_not_found_in_db': True
                        }
        
        return None
    
    def _clean_city_name(self, city_name: str) -> str:
        """
        Limpia el nombre de la ciudad removiendo palabras extra
        """
        # Remover palabras comunes que pueden acompañar al nombre de la ciudad
        words_to_remove = [
            'city', 'town', 'area', 'downtown', 'center', 'centre',
            'district', 'neighborhood', 'neighbourhood'
        ]
        
        words = city_name.split()
        cleaned_words = []
        
        for word in words:
            if word.lower() not in words_to_remove:
                cleaned_words.append(word)
        
        return ' '.join(cleaned_words).strip()
    
    def _should_use_user_location(self, tokens: List[str]) -> bool:
        """
        Determina si debe usar la ubicación del usuario basándose en el texto
        """
        user_location_indicators = [
            'near me', 'close to me', 'around me', 'nearby me',
            'current location', 'my location', 'here', 'nearby',
            'walking distance', 'close', 'around'
        ]
        
        text = ' '.join(tokens).lower()
        
        # Verificar palabras clave del mapping existente
        location_keywords = self.mappings.get('location', {}).get('keywords', [])
        
        # Combinar ambas listas
        all_indicators = user_location_indicators + location_keywords
        
        # Verificar si alguna palabra clave está presente
        has_user_keywords = any(indicator in text for indicator in all_indicators)
        
        # Verificar si NO hay ubicación específica mencionada
        has_specific_location = self._extract_location_from_text(tokens) is not None
        
        return has_user_keywords and not has_specific_location
    
    def _normalize_city_name(self, city_name: str) -> str:
        """
        Normaliza el nombre de la ciudad para mejor matching
        """
        # Convertir a lowercase
        normalized = city_name.lower()
        
        # Remover acentos/tildes
        normalized = unicodedata.normalize('NFD', normalized)
        normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        
        # Remover palabras comunes que pueden variar
        words_to_remove = [
            'ciudad', 'city', 'autonoma', 'autonomous', 'de', 'del', 'la', 'las', 'el', 'los',
            'metropolitan', 'metro', 'area', 'region', 'province', 'provincia'
        ]
        
        words = normalized.split()
        filtered_words = []
        
        for word in words:
            if word not in words_to_remove and len(word) > 1:
                filtered_words.append(word)
        
        return ' '.join(filtered_words)
    
    def _clean_location_from_tokens(self, tokens: List[str]) -> List[str]:
        """
        Remueve referencias de ubicación del texto para limpiar la búsqueda
        """
        text = ' '.join(tokens)
        
        # Patrones a remover
        patterns = [
            r'\s*in\s+[a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]*',
            r'\s*at\s+[a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]*',
            r'\s*near\s+[a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]*',
            r'\s*around\s+[a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ][a-zA-ZáéíóúÁÉÍÓÚàèìòùÀÈÌÒÙâêîôûÂÊÎÔÛãñÃÑçÇ\s]*'
        ]
        
        cleaned_text = text
        for pattern in patterns:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # Limpiar espacios extra
        cleaned_text = ' '.join(cleaned_text.split())
    
        return cleaned_text.split() if cleaned_text else tokens

    def _get_city_variations(self, city_name: str) -> List[str]:
        """
        Genera variaciones posibles del nombre de la ciudad
        """
        variations = [city_name]
        
        # Versión normalizada
        normalized = self._normalize_city_name(city_name)
        if normalized != city_name.lower():
            variations.append(normalized)
        
        # Variaciones comunes
        city_lower = city_name.lower()
        
        # Variaciones de acentos comunes
        accent_variations = {
            'covilhã': ['covilha', 'covilhan'],
            'covilha': ['covilhã', 'covilhan'], 
            'são': ['sao'],
            'sao': ['são'],
            'josé': ['jose'],
            'jose': ['josé']
        }
        
        for original, variants in accent_variations.items():
            if original in city_lower:
                for variant in variants:
                    variations.append(city_name.lower().replace(original, variant))
        
        # Remover duplicados manteniendo orden
        seen = set()
        unique_variations = []
        for var in variations:
            if var.lower() not in seen:
                seen.add(var.lower())
                unique_variations.append(var)
        
        return unique_variations

    def _verify_city_exists_in_db(self, city_name: str) -> Optional[str]:
        """
        Verifica si la ciudad existe en la base de datos y retorna el nombre exacto
        """
        try:
            import mysql.connector
            
            # Usar configuración de DB del motor de búsqueda
            db_config = getattr(self, 'db_config', None)
            if not db_config:
                print("Warning: No hay configuración de DB disponible para verificar ciudad")
                return city_name
            
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            
            # Generar variaciones de la ciudad
            city_variations = self._get_city_variations(city_name)
            
            for variation in city_variations:
                # Buscar coincidencia exacta
                cursor.execute(
                    "SELECT DISTINCT business_city FROM businesses WHERE LOWER(business_city) = LOWER(%s) LIMIT 1",
                    (variation,)
                )
                result = cursor.fetchone()
                
                if result:
                    print(f"Ciudad encontrada en DB: '{result[0]}' (buscando: '{variation}')")
                    return result[0]
                
                # Buscar coincidencia parcial
                cursor.execute(
                    "SELECT DISTINCT business_city FROM businesses WHERE LOWER(business_city) LIKE LOWER(%s) LIMIT 1",
                    (f"%{variation}%",)
                )
                result = cursor.fetchone()
                
                if result:
                    print(f"Ciudad encontrada parcialmente en DB: '{result[0]}' (buscando: '{variation}')")
                    return result[0]
            
            print(f"Ciudad no encontrada en DB: '{city_name}'")
            return None
            
        except Exception as e:
            print(f"Error verificando ciudad en DB: {e}")
            return city_name
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
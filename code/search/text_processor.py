from typing import Dict, Optional, List
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
import json
import os
import re

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
        REEMPLAZA el método existente process_voice_query
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
        use_global_search = False
        
        if specific_location_info and not use_user_location:
            # HAY ubicación específica mencionada - NO usar coordenadas del usuario
            location_source = "text_specified"
            final_coordinates = None  # Búsqueda global sin restricción geográfica
            use_global_search = True
            
            # Limpiar tokens removiendo la referencia de ubicación
            cleaned_tokens = self._clean_location_from_tokens(tokens)
            
            print(f"Ubicación específica detectada: {specific_location_info['location_name']}")
            print(f"Cambiando a búsqueda global, ignorando coordenadas del usuario")
            
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
        
        # Identificar categoría y servicio usando stems
        category_id = self._identify_category(stemmed_tokens)
        service_id = self._identify_service(stemmed_tokens)
        location_context = self._check_location_context(tokens)
        time_info = self._extract_time_info(tokens)
        meal_time = self._identify_meal_time(tokens)
        
        # Construir filtros
        filters = {}
        if category_id:
            filters['category_id'] = category_id
        if service_id:
            filters['service_id'] = service_id
        if time_info:
            filters['time'] = time_info
        if meal_time:
            filters['meal_time'] = meal_time
        
        return {
            'query': self._clean_search_text(cleaned_tokens, stemmed_tokens),
            'filters': filters,
            'use_location': bool(final_coordinates),
            'coordinates': final_coordinates,
            'location_source': location_source,
            'use_global_search': use_global_search,
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
        '''
        Detecta si hay una ubicacion especifica mencionada en el texto
        '''

        text = ' '.join(tokens).lower()

        import re
        location_patterns = [
            r'in\s+([a-zA-Z][a-zA-Z\s]{2,}?)(?:\s|$|,)',
            r'at\s+([a-zA-Z][a-zA-Z\s]{2,}?)(?:\s|$|,)',
            r'near\s+([a-zA-Z][a-zA-Z\s]{2,}?)(?:\s|$|,)',
            r'around\s+([a-zA-Z][a-zA-Z\s]{2,}?)(?:\s|$|,)'
            r'by\s+([a-zA-Z][a-zA-Z\s]{2,}?)(?:\s|$|,)'
        ]

        for pattern in location_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                location_name = match.strip()
                
                exclude_words = {
                    'me', 'you', 'here', 'there', 'home', 'work', 
                    'the', 'a', 'an', 'my', 'your', 'this', 'that',
                    'good', 'bad', 'nice', 'great', 'best', 'worst',
                    'order', 'delivery', 'pickup', 'takeaway', 'restaurant'
                }

                if (len(location_name) >= 3 and
                    location_name.lower() not in exclude_words and
                    not any(word in location_name.lower().split() for word in exclude_words)):

                    print(f'Ubicacion especifica detectada: {location_name}')
                    return {
                        'location_specified': True,
                        'location_name': location_name,
                        'original_pattern': pattern
                    }
        return None
    
    def _should_use_user_location(self, tokens: List[str]) -> bool:
        """
        Determina si se debe usar la ubicación del usuario
        """
        user_location_indicators = [
            'near me', 'close to me', 'around me', 'nearby me',
            'current location', 'my location', 'here', 'nearby',
            'walking distance', 'close', 'around'
        ]

        text = ' '.join(tokens).lower()

        #Verificar palabras clave del mapping existente
        location_keywords = self.mapping.get('location', {}).get('keywords', [])

        #Combinar ambas listas
        all_indicators = user_location_indicators + location_keywords

        #Verificar si alguna de las palabras clave está en el texto
        has_user_keywords = any(indicator in text for indicator in all_indicators)

        #Verificar si No hay ubicacion especifica mencionada
        has_specific_location = self._extract_location_from_text(tokens) is not None

        return has_user_keywords and not has_specific_location

#processor = TextProcessor()

#result = processor.process_voice_query("I would like to find an Italian pizzeria that is open from 7 PM and has take away please")
#print(result)
"""
FAME Data Space - Semantic Package
===================================
Semantic interoperability with RDF, SKOS, OWL, SPARQL.
"""

from .rdf_generator import FAMERDFGenerator
from .sparql_queries import FAME_QUERIES, get_query, list_queries, execute_query

__all__ = [
    'FAMERDFGenerator',
    'FAME_QUERIES',
    'get_query',
    'list_queries',
    'execute_query'
]

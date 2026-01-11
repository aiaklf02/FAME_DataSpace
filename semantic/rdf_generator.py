"""
FAME Data Space - RDF Generator
================================
Converts structured data to RDF format for semantic integration

Supports:
- RDF/XML
- Turtle (.ttl)
- N-Triples (.nt)
- JSON-LD
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional
import logging

try:
    from rdflib import Graph, Namespace, Literal, URIRef, BNode
    from rdflib.namespace import RDF, RDFS, XSD, SKOS, OWL, DCTERMS
    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False
    print("⚠️ rdflib not installed. Run: pip install rdflib")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FAMERDFGenerator:
    """
    RDF Generator for FAME Data Space
    
    Converts financial data to RDF using the FAME ontology.
    """
    
    # Namespaces
    FAME = Namespace("http://fame.eu/ontology#") if RDFLIB_AVAILABLE else None
    FAME_DATA = Namespace("http://fame.eu/data/") if RDFLIB_AVAILABLE else None
    FAME_VOCAB = Namespace("http://fame.eu/vocabulary#") if RDFLIB_AVAILABLE else None
    
    def __init__(self):
        """Initialize RDF generator."""
        if not RDFLIB_AVAILABLE:
            logger.error("rdflib not available. RDF generation disabled.")
            return
        
        self.graph = Graph()
        
        # Bind namespaces
        self.graph.bind("fame", self.FAME)
        self.graph.bind("fdata", self.FAME_DATA)
        self.graph.bind("fvocab", self.FAME_VOCAB)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("xsd", XSD)
        self.graph.bind("skos", SKOS)
        self.graph.bind("dcterms", DCTERMS)
        self.graph.bind("owl", OWL)
        
        # Add ontology metadata
        self._add_metadata()
        
        self.triple_count = 0
    
    def _add_metadata(self):
        """Add dataset metadata."""
        dataset_uri = URIRef("http://fame.eu/data/dataset")
        self.graph.add((dataset_uri, RDF.type, self.FAME.Dataset))
        self.graph.add((dataset_uri, DCTERMS.title, Literal("FAME Financial Data Space")))
        self.graph.add((dataset_uri, DCTERMS.created, Literal(datetime.now().isoformat(), datatype=XSD.dateTime)))
        self.graph.add((dataset_uri, DCTERMS.creator, Literal("FAME Data Space ETL Pipeline")))
    
    def _create_uri(self, entity_type: str, identifier: str) -> URIRef:
        """Create a URI for an entity."""
        # Clean identifier for URI
        clean_id = str(identifier).replace(" ", "_").replace("/", "_")
        return URIRef(f"{self.FAME_DATA}{entity_type}/{clean_id}")
    
    def add_stock_data(self, df: pd.DataFrame):
        """
        Add stock data to RDF graph.
        
        Args:
            df: DataFrame with stock data
        """
        if not RDFLIB_AVAILABLE:
            return
        
        logger.info(f"🔄 Converting {len(df)} stock records to RDF...")
        
        for _, row in df.iterrows():
            # Create stock URI
            symbol = row.get('symbol', row.get('ticker', f"unknown_{_}"))
            stock_uri = self._create_uri("stock", symbol)
            
            # Add type
            self.graph.add((stock_uri, RDF.type, self.FAME.Stock))
            
            # Add properties
            if 'symbol' in row:
                self.graph.add((stock_uri, self.FAME.hasTicker, Literal(row['symbol'])))
            
            if 'entity_name' in row or 'company_name' in row:
                name = row.get('entity_name', row.get('company_name'))
                self.graph.add((stock_uri, RDFS.label, Literal(name)))
            
            if 'price' in row or 'current_price' in row:
                price = row.get('price', row.get('current_price'))
                self.graph.add((stock_uri, self.FAME.price, Literal(float(price), datatype=XSD.decimal)))
            
            if 'price_eur' in row:
                self.graph.add((stock_uri, self.FAME.priceEUR, Literal(float(row['price_eur']), datatype=XSD.decimal)))
            
            if 'volume' in row:
                self.graph.add((stock_uri, self.FAME.volume, Literal(int(row['volume']), datatype=XSD.integer)))
            
            if 'currency' in row:
                currency_uri = self._create_uri("currency", row['currency'])
                self.graph.add((stock_uri, self.FAME.hasCurrency, currency_uri))
            
            if 'exchange' in row:
                exchange_uri = self._create_uri("exchange", row['exchange'])
                self.graph.add((stock_uri, self.FAME.tradedOn, exchange_uri))
                self.graph.add((exchange_uri, RDF.type, self.FAME.StockExchange))
                self.graph.add((exchange_uri, RDFS.label, Literal(row['exchange'])))
            
            if 'timestamp' in row:
                self.graph.add((stock_uri, self.FAME.timestamp, 
                              Literal(str(row['timestamp']), datatype=XSD.dateTime)))
            
            # Data provenance
            self.graph.add((stock_uri, self.FAME.source, Literal(row.get('source', 'unknown'))))
            self.graph.add((stock_uri, self.FAME.dataDomain, Literal("market_data")))
            
            self.triple_count += 10
        
        logger.info(f"   ✅ Added {len(df)} stocks to RDF graph")
    
    def add_forex_data(self, df: pd.DataFrame):
        """
        Add exchange rate data to RDF graph.
        
        Args:
            df: DataFrame with forex data
        """
        if not RDFLIB_AVAILABLE:
            return
        
        logger.info(f"🔄 Converting {len(df)} forex records to RDF...")
        
        # Add ECB as central bank
        ecb_uri = URIRef(f"{self.FAME}ECB")
        self.graph.add((ecb_uri, RDF.type, self.FAME.CentralBank))
        self.graph.add((ecb_uri, RDFS.label, Literal("European Central Bank")))
        
        for _, row in df.iterrows():
            base = row.get('base_currency', 'EUR')
            target = row.get('target_currency', 'USD')
            
            # Create exchange rate URI
            rate_uri = self._create_uri("fx", f"{base}_{target}")
            
            # Add type
            self.graph.add((rate_uri, RDF.type, self.FAME.ExchangeRate))
            self.graph.add((rate_uri, RDFS.label, Literal(f"{base}/{target} Exchange Rate")))
            
            # Add properties
            self.graph.add((rate_uri, self.FAME.rate, Literal(float(row['rate']), datatype=XSD.decimal)))
            
            # Link currencies
            base_uri = self._create_uri("currency", base)
            target_uri = self._create_uri("currency", target)
            
            self.graph.add((base_uri, RDF.type, self.FAME.Currency))
            self.graph.add((base_uri, RDFS.label, Literal(base)))
            self.graph.add((base_uri, SKOS.notation, Literal(base)))
            
            self.graph.add((target_uri, RDF.type, self.FAME.Currency))
            self.graph.add((target_uri, RDFS.label, Literal(row.get('currency_name', target))))
            self.graph.add((target_uri, SKOS.notation, Literal(target)))
            
            self.graph.add((rate_uri, self.FAME.fromCurrency, base_uri))
            self.graph.add((rate_uri, self.FAME.toCurrency, target_uri))
            
            # Link to ECB
            self.graph.add((rate_uri, self.FAME.publishedBy, ecb_uri))
            
            if 'reference_date' in row:
                self.graph.add((rate_uri, self.FAME.referenceDate, 
                              Literal(str(row['reference_date']), datatype=XSD.date)))
            
            if 'region' in row:
                self.graph.add((target_uri, self.FAME.region, Literal(row['region'])))
            
            self.triple_count += 12
        
        logger.info(f"   ✅ Added {len(df)} exchange rates to RDF graph")
    
    def add_financial_data(self, df: pd.DataFrame):
        """
        Add company financial statement data to RDF graph.
        
        Args:
            df: DataFrame with financial data
        """
        if not RDFLIB_AVAILABLE:
            return
        
        logger.info(f"🔄 Converting {len(df)} financial records to RDF...")
        
        companies_added = set()
        
        for _, row in df.iterrows():
            company_id = row.get('company_id', f"company_{_}")
            
            # Create company (once per company)
            company_uri = self._create_uri("company", company_id)
            
            if company_id not in companies_added:
                self.graph.add((company_uri, RDF.type, self.FAME.FinancialInstitution))
                self.graph.add((company_uri, RDFS.label, Literal(row.get('company_name', company_id))))
                
                if 'ticker' in row:
                    self.graph.add((company_uri, self.FAME.hasTicker, Literal(row['ticker'])))
                
                if 'sector' in row:
                    self.graph.add((company_uri, self.FAME.belongsToSector, Literal(row['sector'])))
                
                if 'country' in row:
                    self.graph.add((company_uri, self.FAME.country, Literal(row['country'])))
                
                companies_added.add(company_id)
            
            # Create financial statement
            period_id = row.get('period_id', f"{row.get('fiscal_year', 'unknown')}_{row.get('fiscal_quarter', 'Q1')}")
            statement_uri = self._create_uri("statement", f"{company_id}_{period_id}")
            
            self.graph.add((statement_uri, RDF.type, self.FAME.FinancialStatement))
            self.graph.add((statement_uri, RDFS.label, 
                          Literal(f"{row.get('company_name', company_id)} - {period_id}")))
            
            # Link to company
            self.graph.add((company_uri, self.FAME.hasFinancialStatement, statement_uri))
            
            # Add financial metrics
            if 'revenue_millions' in row:
                self.graph.add((statement_uri, self.FAME.revenue, 
                              Literal(float(row['revenue_millions']), datatype=XSD.decimal)))
            
            if 'net_income_millions' in row:
                self.graph.add((statement_uri, self.FAME.netIncome, 
                              Literal(float(row['net_income_millions']), datatype=XSD.decimal)))
            
            if 'total_assets_millions' in row:
                self.graph.add((statement_uri, self.FAME.totalAssets, 
                              Literal(float(row['total_assets_millions']), datatype=XSD.decimal)))
            
            if 'profit_margin_pct' in row:
                self.graph.add((statement_uri, self.FAME.profitMargin, 
                              Literal(float(row['profit_margin_pct']), datatype=XSD.decimal)))
            
            if 'roe_pct' in row:
                self.graph.add((statement_uri, self.FAME.returnOnEquity, 
                              Literal(float(row['roe_pct']), datatype=XSD.decimal)))
            
            # Period info
            if 'fiscal_year' in row:
                self.graph.add((statement_uri, self.FAME.fiscalYear, 
                              Literal(int(row['fiscal_year']), datatype=XSD.integer)))
            
            if 'fiscal_quarter' in row:
                self.graph.add((statement_uri, self.FAME.fiscalQuarter, Literal(row['fiscal_quarter'])))
            
            self.triple_count += 15
        
        logger.info(f"   ✅ Added {len(df)} financial statements for {len(companies_added)} companies")
    
    def add_transaction_data(self, df: pd.DataFrame, sample_size: int = 100):
        """
        Add transaction data to RDF graph.
        
        Args:
            df: DataFrame with transaction data
            sample_size: Limit number of transactions (RDF can get large)
        """
        if not RDFLIB_AVAILABLE:
            return
        
        # Sample if too many transactions
        if len(df) > sample_size:
            df = df.sample(n=sample_size)
        
        logger.info(f"🔄 Converting {len(df)} transaction records to RDF...")
        
        customers_added = set()
        banks_added = set()
        
        for _, row in df.iterrows():
            tx_id = row.get('transaction_id', f"tx_{_}")
            
            # Create transaction
            tx_uri = self._create_uri("transaction", tx_id)
            
            self.graph.add((tx_uri, RDF.type, self.FAME.Transaction))
            
            # Transaction type subclass
            tx_type = row.get('transaction_type', 'PAYMENT')
            if 'SEPA' in tx_type:
                self.graph.add((tx_uri, RDF.type, self.FAME.SEPATransfer))
            elif 'INSTANT' in tx_type:
                self.graph.add((tx_uri, RDF.type, self.FAME.InstantPayment))
            
            self.graph.add((tx_uri, RDFS.label, Literal(f"Transaction {tx_id[:8]}...")))
            
            # Amount
            if 'amount' in row:
                self.graph.add((tx_uri, self.FAME.amount, 
                              Literal(float(row['amount']), datatype=XSD.decimal)))
            
            if 'amount_eur' in row:
                self.graph.add((tx_uri, self.FAME.amountEUR, 
                              Literal(float(row['amount_eur']), datatype=XSD.decimal)))
            
            if 'currency' in row:
                currency_uri = self._create_uri("currency", row['currency'])
                self.graph.add((tx_uri, self.FAME.hasCurrency, currency_uri))
            
            # Sender
            sender_id = row.get('sender_id', 'unknown')
            sender_uri = self._create_uri("customer", sender_id)
            
            if sender_id not in customers_added:
                self.graph.add((sender_uri, RDF.type, self.FAME.Customer))
                self.graph.add((sender_uri, RDFS.label, Literal(row.get('sender_name', sender_id))))
                self.graph.add((sender_uri, self.FAME.country, Literal(row.get('sender_country', 'XX'))))
                customers_added.add(sender_id)
            
            self.graph.add((tx_uri, self.FAME.hasSender, sender_uri))
            
            # Receiver
            receiver_id = row.get('receiver_id', 'unknown')
            receiver_uri = self._create_uri("customer", receiver_id)
            
            if receiver_id not in customers_added:
                self.graph.add((receiver_uri, RDF.type, self.FAME.Customer))
                self.graph.add((receiver_uri, RDFS.label, Literal(row.get('receiver_name', receiver_id))))
                self.graph.add((receiver_uri, self.FAME.country, Literal(row.get('receiver_country', 'XX'))))
                customers_added.add(receiver_id)
            
            self.graph.add((tx_uri, self.FAME.hasReceiver, receiver_uri))
            
            # Banks
            sender_bank = row.get('sender_bank', 'Unknown Bank')
            sender_bank_uri = self._create_uri("bank", sender_bank.replace(" ", "_"))
            
            if sender_bank not in banks_added:
                self.graph.add((sender_bank_uri, RDF.type, self.FAME.Bank))
                self.graph.add((sender_bank_uri, RDFS.label, Literal(sender_bank)))
                banks_added.add(sender_bank)
            
            self.graph.add((tx_uri, self.FAME.processedBy, sender_bank_uri))
            
            # Transaction properties
            if 'timestamp' in row:
                self.graph.add((tx_uri, self.FAME.timestamp, 
                              Literal(str(row['timestamp']), datatype=XSD.dateTime)))
            
            if 'status' in row:
                self.graph.add((tx_uri, self.FAME.status, Literal(row['status'])))
            
            if 'channel' in row:
                self.graph.add((tx_uri, self.FAME.usesChannel, Literal(row['channel'])))
            
            # Cross-border flag
            is_cross_border = row.get('is_cross_border', 
                                      row.get('sender_country', '') != row.get('receiver_country', ''))
            self.graph.add((tx_uri, self.FAME.isCrossBorder, 
                          Literal(bool(is_cross_border), datatype=XSD.boolean)))
            
            self.triple_count += 18
        
        logger.info(f"   ✅ Added {len(df)} transactions to RDF graph")
    
    def get_statistics(self) -> Dict:
        """Get statistics about the RDF graph."""
        if not RDFLIB_AVAILABLE:
            return {"error": "rdflib not available"}
        
        return {
            "total_triples": len(self.graph),
            "subjects": len(set(self.graph.subjects())),
            "predicates": len(set(self.graph.predicates())),
            "objects": len(set(self.graph.objects())),
            "classes": len(list(self.graph.subjects(RDF.type, None)))
        }
    
    def save_rdf(self, filepath: str, format: str = 'turtle'):
        """
        Save RDF graph to file.
        
        Args:
            filepath: Output file path
            format: RDF format (turtle, xml, nt, json-ld)
        """
        if not RDFLIB_AVAILABLE:
            logger.error("rdflib not available")
            return
        
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
        
        # Map format names
        format_map = {
            'turtle': 'turtle',
            'ttl': 'turtle',
            'xml': 'xml',
            'rdf': 'xml',
            'rdfxml': 'xml',
            'nt': 'nt',
            'ntriples': 'nt',
            'jsonld': 'json-ld',
            'json-ld': 'json-ld'
        }
        
        rdf_format = format_map.get(format.lower(), 'turtle')
        
        self.graph.serialize(destination=filepath, format=rdf_format)
        logger.info(f"💾 Saved RDF graph to {filepath} ({rdf_format} format)")
        logger.info(f"   Total triples: {len(self.graph)}")
    
    def query(self, sparql_query: str) -> List[Dict]:
        """
        Execute a SPARQL query on the graph.
        
        Args:
            sparql_query: SPARQL query string
            
        Returns:
            List of result dictionaries
        """
        if not RDFLIB_AVAILABLE:
            return []
        
        results = []
        qres = self.graph.query(sparql_query)
        
        for row in qres:
            result = {}
            for var in qres.vars:
                result[str(var)] = str(row[var]) if row[var] else None
            results.append(result)
        
        return results


# Test
if __name__ == "__main__":
    print("=" * 70)
    print("FAME Data Space - RDF Generator Test")
    print("=" * 70)
    
    if not RDFLIB_AVAILABLE:
        print("❌ rdflib not installed. Run: pip install rdflib")
        exit(1)
    
    generator = FAMERDFGenerator()
    
    # Test with sample data
    stock_data = pd.DataFrame([
        {"symbol": "AAPL", "company_name": "Apple Inc.", "price": 185.50, 
         "volume": 50000000, "currency": "USD", "exchange": "NASDAQ", "source": "api"},
        {"symbol": "BNP.PA", "company_name": "BNP Paribas", "price": 62.45, 
         "volume": 5000000, "currency": "EUR", "exchange": "Euronext Paris", "source": "api"}
    ])
    
    forex_data = pd.DataFrame([
        {"base_currency": "EUR", "target_currency": "USD", "rate": 1.08, 
         "currency_name": "US Dollar", "region": "North America"},
        {"base_currency": "EUR", "target_currency": "GBP", "rate": 0.86, 
         "currency_name": "British Pound", "region": "Europe"}
    ])
    
    generator.add_stock_data(stock_data)
    generator.add_forex_data(forex_data)
    
    # Save
    os.makedirs("data/rdf", exist_ok=True)
    generator.save_rdf("data/rdf/fame_test.ttl", format='turtle')
    
    # Stats
    print("\n📊 RDF Graph Statistics:")
    for key, value in generator.get_statistics().items():
        print(f"   {key}: {value}")
    
    print("\n✅ RDF generation test complete!")

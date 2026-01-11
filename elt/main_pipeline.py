"""
FAME Data Space - ELT Pipeline Orchestrator
=============================================
Main pipeline using EtLT pattern (Extract, light Transform, Load, Transform).

Pipeline Flow:
1. EXTRACT: Get raw data from 4 sources (minimal transformation)
2. LOAD: Load raw data to Bronze zone + Staging tables
3. TRANSFORM: Transform IN the warehouse (Staging → Silver → Gold → Star)
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FAMEPipeline:
    """
    FAME EtLT Pipeline Orchestrator.
    
    Pattern: Extract → Light Transform → Load → Transform (in DW)
    
    This is more efficient than traditional ETL because:
    - Raw data is preserved in Bronze zone
    - Transformations use warehouse compute (scalable)
    - Easy to reprocess if logic changes
    """
    
    def __init__(self, data_path: str = "data"):
        """Initialize pipeline components."""
        self.data_path = data_path
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results = {
            "run_id": self.run_id,
            "started_at": None,
            "completed_at": None,
            "extract": [],
            "load": [],
            "transform": [],
            "status": "initialized"
        }
        
        # Initialize components
        from elt.extract import FAMEExtractor
        from elt.load import FAMELoader
        from elt.transform import FAMETransformer
        from elt.warehouse import FAMEWarehouse
        
        self.extractor = FAMEExtractor(data_lake_path=data_path)
        self.loader = FAMELoader(data_lake_path=data_path)
        self.transformer = FAMETransformer(warehouse_path=os.path.join(data_path, "warehouse"))
        self.warehouse = FAMEWarehouse(warehouse_path=os.path.join(data_path, "warehouse"))
    
    def _read_extracted_file(self, filepath: str, source_type: str) -> List[Dict]:
        """
        Read extracted file based on its format.
        
        Supports: JSON, XML, CSV
        """
        import xml.etree.ElementTree as ET
        import pandas as pd
        
        extension = filepath.split('.')[-1].lower()
        
        if extension == 'json':
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        elif extension == 'xml':
            # Parse XML file
            tree = ET.parse(filepath)
            root = tree.getroot()
            data = []
            
            # Handle our custom XML format
            for item in root:
                record = {}
                for child in item:
                    record[child.tag] = child.text
                if record:
                    data.append(record)
            return data
        
        elif extension == 'csv':
            df = pd.read_csv(filepath)
            return df.to_dict('records')
        
        else:
            # Default: try JSON
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
    
    def run(self, 
            skip_extract: bool = False,
            skip_load: bool = False,
            skip_transform: bool = False) -> Dict[str, Any]:
        """
        Run the complete EtLT pipeline.
        
        Args:
            skip_extract: Skip extraction phase
            skip_load: Skip loading phase
            skip_transform: Skip transformation phase
            
        Returns:
            Pipeline results dictionary
        """
        self.results["started_at"] = datetime.now().isoformat()
        
        print("=" * 70)
        print("🏦 FAME DATA SPACE - EtLT PIPELINE")
        print("=" * 70)
        print(f"📋 Run ID: {self.run_id}")
        print(f"🏗️  Architecture: Data Lake + Data Fabric + Data Warehouse")
        print(f"🔄 Pattern: EtLT (Extract, light Transform, Load, Transform)")
        print("=" * 70)
        
        extracted_data = {}
        
        try:
            # =====================================================================
            # PHASE 1: EXTRACT
            # =====================================================================
            if not skip_extract:
                print("\n" + "=" * 70)
                print("📥 PHASE 1: EXTRACT (from 4 heterogeneous sources)")
                print("=" * 70)
                
                extraction_results = self.extractor.extract_all()
                self.results["extract"] = [
                    {
                        "source": r.source_name,
                        "records": r.record_count,
                        "file": r.file_path,
                        "status": r.status
                    }
                    for r in extraction_results
                ]
                
                # Load extracted data into memory for loading phase
                # Data is now directly in result.data (no file reading needed for real-time sources)
                for result in extraction_results:
                    # Use data directly from ExtractionResult
                    data = result.data if result.data else []
                    
                    # If no data in result but file exists, read from file
                    if not data and result.file_path and os.path.exists(result.file_path):
                        data = self._read_extracted_file(result.file_path, result.source_type)
                            
                    if "stock" in result.source_name.lower() or "yahoo" in result.source_name.lower():
                        extracted_data["stocks"] = data
                    elif "ecb" in result.source_name.lower() or "forex" in result.source_name.lower():
                        extracted_data["forex"] = data
                    elif "financial" in result.source_name.lower() or "csv" in result.source_name.lower():
                        extracted_data["financials"] = data
                    elif "transaction" in result.source_name.lower() or "postgresql" in result.source_name.lower():
                        extracted_data["transactions"] = data
                
                print(f"\n✅ Extraction complete: {sum(r.record_count for r in extraction_results)} records")
            else:
                print("\n⏭️  Skipping EXTRACT phase")
            
            # =====================================================================
            # PHASE 2: LOAD
            # =====================================================================
            if not skip_load:
                print("\n" + "=" * 70)
                print("📦 PHASE 2: LOAD (to Data Lake Bronze + DW Staging)")
                print("=" * 70)
                
                if extracted_data:
                    load_results = self.loader.load_all_to_staging(extracted_data)
                    self.results["load"] = [
                        {
                            "table": r.table_name,
                            "destination": r.destination,
                            "records": r.record_count,
                            "status": r.status
                        }
                        for r in load_results
                    ]
                    print(f"\n✅ Load complete: {sum(r.record_count for r in load_results)} records")
                else:
                    print("⚠️  No data to load. Run extract phase first.")
            else:
                print("\n⏭️  Skipping LOAD phase")
            
            # =====================================================================
            # PHASE 3: TRANSFORM (in warehouse)
            # =====================================================================
            if not skip_transform:
                print("\n" + "=" * 70)
                print("🔄 PHASE 3: TRANSFORM (in Data Warehouse)")
                print("=" * 70)
                
                transform_results = self.transformer.transform_all()
                self.results["transform"] = [
                    {
                        "source": r.source_table,
                        "target": r.target_table,
                        "records": r.record_count,
                        "status": r.status
                    }
                    for r in transform_results
                ]
                print(f"\n✅ Transform complete: {len(transform_results)} tables created")
            else:
                print("\n⏭️  Skipping TRANSFORM phase")
            
            # =====================================================================
            # SUMMARY
            # =====================================================================
            self.results["status"] = "success"
            self.results["completed_at"] = datetime.now().isoformat()
            
            print("\n" + "=" * 70)
            print("✅ PIPELINE COMPLETE")
            print("=" * 70)
            
            self._print_summary()
            
        except Exception as e:
            self.results["status"] = "failed"
            self.results["error"] = str(e)
            logger.error(f"Pipeline failed: {e}")
            raise
        
        finally:
            # Cleanup connections
            self.loader.close()
            self.transformer.close()
            self.warehouse.close()
        
        return self.results
    
    def _print_summary(self):
        """Print pipeline summary."""
        print("\n📊 Pipeline Summary:")
        print(f"   Run ID:     {self.run_id}")
        print(f"   Status:     {self.results['status']}")
        print(f"   Started:    {self.results['started_at']}")
        print(f"   Completed:  {self.results['completed_at']}")
        
        if self.results["extract"]:
            total_extracted = sum(r["records"] for r in self.results["extract"])
            print(f"\n📥 Extracted:  {total_extracted} records from {len(self.results['extract'])} sources")
        
        if self.results["load"]:
            total_loaded = sum(r["records"] for r in self.results["load"])
            print(f"📦 Loaded:     {total_loaded} records to {len(self.results['load'])} staging tables")
        
        if self.results["transform"]:
            print(f"🔄 Transformed: {len(self.results['transform'])} tables")
            
            # Show layer breakdown
            silver = [t for t in self.results["transform"] if "silver" in t["target"]]
            gold = [t for t in self.results["transform"] if "gold" in t["target"]]
            dims = [t for t in self.results["transform"] if "dim_" in t["target"]]
            facts = [t for t in self.results["transform"] if "fact_" in t["target"]]
            
            print(f"   • Silver Layer: {len(silver)} tables")
            print(f"   • Gold Layer:   {len(gold)} tables")
            print(f"   • Dimensions:   {len(dims)} tables")
            print(f"   • Facts:        {len(facts)} tables")
        
        print("\n📍 Data Locations:")
        print(f"   Bronze Zone:  {self.data_path}/bronze/")
        print(f"   Warehouse:    {self.data_path}/warehouse/fame_warehouse.duckdb")
        
        print("\n🔗 Access Points:")
        print("   SQL Queries:  Use FAMEWarehouse class or DuckDB CLI")
        print("   Dashboard:    Run: streamlit run prototype/app.py")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='FAME Data Space - EtLT Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m elt.main_pipeline                    # Run full pipeline
  python -m elt.main_pipeline --skip-extract    # Only load + transform
  python -m elt.main_pipeline --skip-transform  # Only extract + load
        """
    )
    
    parser.add_argument('--skip-extract', action='store_true',
                       help='Skip extraction phase')
    parser.add_argument('--skip-load', action='store_true',
                       help='Skip loading phase')
    parser.add_argument('--skip-transform', action='store_true',
                       help='Skip transformation phase')
    parser.add_argument('--data-path', default='data',
                       help='Path to data directory')
    
    args = parser.parse_args()
    
    pipeline = FAMEPipeline(data_path=args.data_path)
    results = pipeline.run(
        skip_extract=args.skip_extract,
        skip_load=args.skip_load,
        skip_transform=args.skip_transform
    )
    
    return 0 if results["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())

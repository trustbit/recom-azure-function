import json
from pathlib import Path
from typing import List, Dict, Any

def load_products(path: str) -> List[Dict[str, Any]]:
    products = []
    try:
        directory = Path(path)
        if not directory.exists():
            return []
            
        for file in directory.glob('*_pdf.json'):
            try:
                with open(file, 'r') as f:
                    items = json.load(f)

                    products.extend(items["power_converters"])
            except (json.JSONDecodeError, FileNotFoundError):
                continue
                
        return products
    except Exception:
        return []

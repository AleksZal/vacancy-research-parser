"""Read the shared database structure; never modify it."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from vacancy_parser.shared_db import connect_shared, inspect_schema, safe_error

try:
    with connect_shared() as connection:
        schema = inspect_schema(connection)
    (ROOT / 'database/shared_schema.json').write_text(
        json.dumps(schema, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    tables = sorted({(r['table_schema'], r['table_name']) for r in schema['columns']})
    print(json.dumps({'read_only': True, 'tables': tables,
                      'constraints': len(schema['constraints']),
                      'enums': schema['enums']}, ensure_ascii=False))
except Exception as exc:
    print(json.dumps(safe_error(exc)))
    raise SystemExit(1)

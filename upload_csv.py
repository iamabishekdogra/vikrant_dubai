import pandas as pd
import pyodbc
import re
import sys
from decimal import Decimal

def normalize_name(s: str) -> str:
    return re.sub(r'[^0-9a-z]', '', s.lower())

# SQL type groups
_INT_TYPES   = {'tinyint', 'smallint', 'int', 'bigint', 'bit'}
_FLOAT_TYPES = {'float', 'real'}
_DEC_TYPES   = {'decimal', 'numeric'}
_DATE_TYPES  = {'date', 'datetime', 'datetime2', 'smalldatetime', 'time'}

def append_csv_to_sqlserver(
    csv_path: str,
    table_name: str,
    server: str,
    database: str,
    username: str,
    password: str,
    dayfirst: bool = True
):
    # 1) Read all as strings
    df = pd.read_csv(csv_path, dtype=str).fillna('')

    # 2) Get schema info
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str, timeout=5)
    cursor = conn.cursor()

    if '.' in table_name:
        schema, tbl = table_name.split('.', 1)
    else:
        schema, tbl = 'dbo', table_name

    cursor.execute("""
      SELECT 
        COLUMN_NAME, DATA_TYPE, 
        NUMERIC_PRECISION, NUMERIC_SCALE
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """, schema, tbl)
    cols_info = [
        {
            'name': row.COLUMN_NAME,
            'dtype': row.DATA_TYPE.lower(),
            'prec': row.NUMERIC_PRECISION,
            'scale': row.NUMERIC_SCALE
        }
        for row in cursor.fetchall()
    ]
    cursor.close()
    conn.close()

    # 3) Build normalized→info map
    norm_to_info = {
        normalize_name(ci['name']): ci
        for ci in cols_info
    }

    # 4) Map CSV headers → real column names
    col_map = {}
    for hdr in df.columns:
        norm = normalize_name(hdr)
        if norm in norm_to_info:
            info = norm_to_info[norm]
        else:
            # substring fallback
            cands = [ci for n, ci in norm_to_info.items() if norm in n]
            if len(cands) == 1:
                info = cands[0]
            else:
                raise ValueError(
                    f"Cannot map CSV column '{hdr}'. "
                    f"Candidates: {[ci['name'] for ci in cands]}"
                )
        col_map[hdr] = info['name']
    df = df.rename(columns=col_map)
    real_cols = list(col_map.values())

    # 5) Prepare a row‐by‐row converter that handles NULLs
    def make_converter(ci):
        name, dtype, prec, scale = ci['name'], ci['dtype'], ci['prec'], ci['scale']
        if dtype in _DATE_TYPES:
            def conv(v):
                if v == '':
                    return None
                dt = pd.to_datetime(v, dayfirst=dayfirst, errors='raise')
                return dt.date() if dtype == 'date' else dt
        elif dtype in _INT_TYPES:
            def conv(v):
                if v == '':
                    return None
                return int(v)
        elif dtype in _FLOAT_TYPES:
            def conv(v):
                if v == '':
                    return None
                return float(v)
        elif dtype in _DEC_TYPES:
            # quantize to the table’s scale
            quantum = Decimal((0, (1,), -scale))
            def conv(v):
                if v == '':
                    return None
                d = Decimal(v)
                return d.quantize(quantum)
        else:
            def conv(v):
                # leave strings and any other types untouched
                return v if v != '' else None

        return conv

    converters = {
        ci['name']: make_converter(ci)
        for ci in cols_info
        if ci['name'] in real_cols
    }

    # 6) Build INSERT SQL
    col_list = ", ".join(f"[{c}]" for c in real_cols)
    ph       = ", ".join("?" for _ in real_cols)
    insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({ph})"

    # 7) Open fresh connection & bulk‐insert
    conn = pyodbc.connect(conn_str, timeout=5)
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # Convert & collect rows
    data = []
    for _, row in df.iterrows():
        converted = []
        for col in real_cols:
            val = row[col]
            converted.append(converters[col](val))
        data.append(tuple(converted))

    cursor.executemany(insert_sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Appended {len(data)} rows to {table_name}.")

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python upload_csv.py "
              "<csv_path> <schema.table> <server> <database> <user> <password>")
        sys.exit(1)

    _, csv_path, table_name, server, db, user, pwd = sys.argv
    append_csv_to_sqlserver(
        csv_path=csv_path,
        table_name=table_name,
        server=server,
        database=db,
        username=user,
        password=pwd
    )

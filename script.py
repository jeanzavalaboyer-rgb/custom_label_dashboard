#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import gzip
import requests
import urllib3
import pandas as pd
import gspread

from google.oauth2.service_account import Credentials
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================
# CONFIG
# ==========================
SERVICE_PATH = "https://meta.feedonomics.com/api.php"
OUTPUT_FOLDER = "exports_downloads"
MAX_WORKERS = 10
TIMEOUT_SEC = 120
RETRIES = 4
RETRY_SLEEP_BASE = 1.0
PAGE_SIZE = 1000

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()

WORKSHEET_COMBINED = "combined_custom_labels"
WORKSHEET_MAPPING = "exports_mapping"
WORKSHEET_ERRORS = "errores"

DATABASES = [
    {"db_name": "Global - OrderCode", "db_id": 87354},
    {"db_name": "Global - Sku", "db_id": 87355},
    {"db_name": "Global - Variant", "db_id": 87356},
    {"db_name": "APAC - AU_en", "db_id": 104541},
    {"db_name": "APAC - NZ_en", "db_id": 103399},
    {"db_name": "APAC - IN_en", "db_id": 105061},
    {"db_name": "APAC - MY_en", "db_id": 103756},
    {"db_name": "APAC - JP_ja", "db_id": 105553},
    {"db_name": "APAC - SG_en", "db_id": 103664},
    {"db_name": "APAC - CN_zh", "db_id": 107871},
    {"db_name": "NA - US_en", "db_id": 89366},
    {"db_name": "NA - CA_en", "db_id": 99921},
    {"db_name": "NA - CA_fr", "db_id": 99930},
    {"db_name": "EMEA - UK_en", "db_id": 101178},
    {"db_name": "EMEA - DE_de", "db_id": 101184},
    {"db_name": "EMEA - FR_fr", "db_id": 102569},
    {"db_name": "EMEA - SE_sv", "db_id": 107651},
    {"db_name": "EMEA - NL_nl", "db_id": 97333},
    {"db_name": "EMEA - ES_es", "db_id": 105938},
    {"db_name": "EMEA - IT_it", "db_id": 134807},
    {"db_name": "EMEA - CH_de", "db_id": 106506},
    {"db_name": "EMEA - CH_fr", "db_id": 106535},
    {"db_name": "SA - BR_pt", "db_id": 103404},
    {"db_name": "Outlet - EMEA - UK_en", "db_id": 54336},
    {"db_name": "Outlet - NA - US_en", "db_id": 53604},
]

CUSTOM_COLS = [f"custom_label_{i}" for i in range(5)]

FINAL_ORDER = [
    "db_name",
    "db_id",
    "export_name",
    "export_id",
    "export_date",
    "country_code",
    "custom_label_0",
    "custom_label_1",
    "custom_label_2",
    "custom_label_3",
    "custom_label_4",
]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ==========================
# AUTH
# ==========================
API_KEY = os.getenv("FEEDONOMICS_API_KEY", "").strip()
if not API_KEY:
    raise ValueError("Falta la variable de entorno FEEDONOMICS_API_KEY")

if not SPREADSHEET_ID:
    raise ValueError("Falta la variable de entorno SPREADSHEET_ID")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
}

errores = []


# ==========================
# GOOGLE SHEETS HELPERS
# ==========================
def get_google_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_json = os.getenv("GOOGLE_CREDS_JSON", "").strip()
    if not creds_json:
        raise ValueError("Falta la variable de entorno GOOGLE_CREDS_JSON")

    try:
        creds_info = json.loads(creds_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_CREDS_JSON no es un JSON válido: {e}") from e

    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_worksheet(spreadsheet, worksheet_name: str, rows: int = 1000, cols: int = 20):
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=str(max(rows, 1000)),
            cols=str(max(cols, 20)),
        )
    return worksheet


def save_to_google_sheet(df: pd.DataFrame, worksheet_name: str):
    client = get_google_client()
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    rows_needed = max(len(df) + 10, 1000)
    cols_needed = max(len(df.columns) + 5, 20)

    worksheet = ensure_worksheet(
        spreadsheet,
        worksheet_name,
        rows=rows_needed,
        cols=cols_needed,
    )

    worksheet.clear()

    if df.empty:
        worksheet.update("A1", [["Sin datos"]])
        print(f"⚠️ Google Sheets actualizado sin datos → {worksheet_name}")
        return

    safe_df = df.copy().fillna("").astype(str)
    values = [safe_df.columns.tolist()] + safe_df.values.tolist()
    worksheet.update("A1", values)

    print(f"✅ Google Sheets actualizado → {worksheet_name} | rows={len(df)}")


# ==========================
# HTTP HELPERS
# ==========================
def make_api_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Any] = None,
) -> Any:
    last_err = None

    for attempt in range(1, RETRIES + 1):
        try:
            if method.upper() == "GET":
                r = session.get(url, params=params, timeout=TIMEOUT_SEC, verify=False)
            elif method.upper() == "POST":
                r = session.post(url, params=params, json=json_payload, timeout=TIMEOUT_SEC, verify=False)
            else:
                raise ValueError(f"Método no soportado: {method}")

            if r.status_code == 200:
                return r.json()

            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
        except Exception as e:
            last_err = str(e)

        if attempt < RETRIES:
            time.sleep(RETRY_SLEEP_BASE * attempt)

    raise RuntimeError(f"Failed {method} {url} | {last_err}")


def get_exports(session: requests.Session, db_id: int) -> List[Dict[str, Any]]:
    data = request_json(session, "GET", f"{SERVICE_PATH}/dbs/{db_id}/exports")
    return data if isinstance(data, list) else []


# ==========================
# TRANSFORMED DATA PARSER
# ==========================
def response_to_payload(resp: requests.Response) -> dict:
    content = resp.content

    if content[:2] == b"\x1f\x8b":
        text = gzip.decompress(content).decode("utf-8", errors="replace")
    else:
        text = resp.text

    try:
        payload = json.loads(text)
    except Exception:
        payload = resp.json()

    if not isinstance(payload, dict):
        raise ValueError("La respuesta de transformed_data no es un dict.")

    return payload


def transformed_rows_to_df(rows: List[List[Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    max_len = max(len(r) if isinstance(r, list) else 0 for r in rows)
    if max_len == 0:
        return pd.DataFrame()

    columns = [f"col{i}" for i in range(1, max_len + 1)]
    normalized_rows = []

    for r in rows:
        if not isinstance(r, list):
            r = []
        padded = r + [""] * (max_len - len(r))
        normalized_rows.append(padded)

    return pd.DataFrame(normalized_rows, columns=columns, dtype=str)


def download_all_transformed_rows(
    session: requests.Session,
    db_id: int,
    export_id: str,
    page_size: int = PAGE_SIZE
) -> List[List[Any]]:
    all_rows = []
    offset = 0
    total_count_reported = None

    while True:
        url = f"{SERVICE_PATH}/dbs/{db_id}/transformed_data/{export_id}?limit={page_size}&offset={offset}"
        last_err = None
        payload = None

        for attempt in range(1, RETRIES + 1):
            try:
                resp = session.get(url, timeout=TIMEOUT_SEC, verify=False)
                if resp.status_code != 200:
                    last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                else:
                    payload = response_to_payload(resp)
                    break
            except Exception as e:
                last_err = str(e)

            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_BASE * attempt)

        if payload is None:
            raise RuntimeError(f"Failed GET {url} | {last_err}")

        if total_count_reported is None:
            total_count_reported = payload.get("total_count")

        rows = payload.get("data", [])
        if not isinstance(rows, list) or not rows:
            break

        all_rows.extend(rows)

        print(
            f"   ↳ page export={export_id} offset={offset} fetched={len(rows)} "
            f"accumulated={len(all_rows)} total_reported={total_count_reported}"
        )

        if len(rows) < page_size:
            break

        offset += page_size

        if total_count_reported is not None and len(all_rows) >= int(total_count_reported):
            break

    return all_rows


# ==========================
# MAPEAR COLX -> CUSTOM LABEL
# ==========================
def extract_export_fields(export_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    fields = export_obj.get("export_fields") or []
    return fields if isinstance(fields, list) else []


def normalize_str(value: Any) -> str:
    return str(value or "").strip().lower()


def detectar_mapa_custom_labels(fields: List[Dict[str, Any]]) -> Dict[str, str]:
    mapping = {}

    for idx, field in enumerate(fields, start=1):
        if not isinstance(field, dict):
            continue

        export_field_name = normalize_str(field.get("export_field_name"))
        field_name = normalize_str(field.get("field_name"))

        for custom in CUSTOM_COLS:
            if export_field_name == custom or field_name == custom:
                mapping[f"col{idx}"] = custom
                break

    return mapping


# ==========================
# DETECTAR EXPORTS VÁLIDOS
# ==========================
def export_name_starts_with_single_asterisk(export_name: str) -> bool:
    name = str(export_name or "").lstrip()
    return name.startswith("*") and not name.startswith("**")


def detectar_exports_con_custom_labels() -> pd.DataFrame:
    session = make_api_session()
    rows = []

    for db in DATABASES:
        db_id = int(db["db_id"])
        db_name = str(db["db_name"])

        try:
            exports = get_exports(session, db_id)
        except Exception as e:
            errores.append((db_id, "", f"Error listando exports: {e}"))
            print(f"❌ Error listando exports para DB {db_id}: {e}")
            continue

        for exp in exports:
            export_id = str(exp.get("id", "")).strip()
            export_name = str(exp.get("name", "")).strip()

            if not export_id:
                continue

            # Solo exports que comiencen con un único *
            if not export_name_starts_with_single_asterisk(export_name):
                continue

            fields = extract_export_fields(exp)
            if not fields:
                continue

            col_mapping = detectar_mapa_custom_labels(fields)

            if col_mapping:
                row = {
                    "db_name": db_name,
                    "db_id": db_id,
                    "export_name": export_name,
                    "export_id": export_id,
                }

                for custom in CUSTOM_COLS:
                    matched_col = ""
                    for colx, label in col_mapping.items():
                        if label == custom:
                            matched_col = colx
                            break
                    row[f"{custom}_source_col"] = matched_col

                rows.append(row)

    return pd.DataFrame(rows)


# ==========================
# PROCESAR EXPORT
# ==========================
def procesar_export(row: pd.Series) -> Optional[pd.DataFrame]:
    db_id = row["db_id"]
    export_id = row["export_id"]
    db_name = row["db_name"]
    export_name = row["export_name"]
    country_code = db_name.split(" - ")[-1] if " - " in db_name else db_name
    export_date = datetime.now().strftime("%Y-%m-%d")

    col_mapping = {}
    for custom in CUSTOM_COLS:
        source_col = str(row.get(f"{custom}_source_col", "") or "").strip()
        if source_col:
            col_mapping[source_col] = custom

    try:
        session = make_api_session()
        all_rows = download_all_transformed_rows(session, db_id, export_id, PAGE_SIZE)

        if not all_rows:
            print(f"⚠️ Export sin filas → DB {db_id} Export {export_id}")
            return None

        df = transformed_rows_to_df(all_rows)
        if df.empty:
            print(f"⚠️ Export vacío tras parse → DB {db_id} Export {export_id}")
            return None

        rename_map = {src: dst for src, dst in col_mapping.items() if src in df.columns}
        df = df.rename(columns=rename_map)

        result_df = pd.DataFrame()
        for col in CUSTOM_COLS:
            if col in df.columns:
                result_df[col] = df[col].fillna("")
            else:
                result_df[col] = ""

        result_df["db_name"] = db_name
        result_df["db_id"] = db_id
        result_df["export_name"] = export_name
        result_df["export_id"] = export_id
        result_df["export_date"] = export_date
        result_df["country_code"] = country_code

        result_df = result_df[FINAL_ORDER]

        print(f"✅ Export procesado → DB {db_id} Export {export_id} | rows={len(result_df)}")
        return result_df

    except Exception as e:
        errores.append((db_id, export_id, str(e)))
        print(f"❌ Excepción DB {db_id} Export {export_id}: {e}")
        return None


# ==========================
# MAIN
# ==========================
def main():
    print("🔎 Buscando exports que contengan custom labels y comiencen con un único * ...")
    df_exports = detectar_exports_con_custom_labels()

    if df_exports.empty:
        print("⚠️ No se encontraron exports válidos con custom labels.")
        return

    save_to_google_sheet(df_exports, WORKSHEET_MAPPING)
    print(f"✅ Exports detectados: {len(df_exports)}")

    rows = [row for _, row in df_exports.iterrows()]
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(procesar_export, row) for row in rows]
        for future in as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, pd.DataFrame) and not result.empty:
                    results.append(result)
            except Exception as e:
                errores.append(("", "", f"Error en future: {e}"))
                print(f"❌ Error en future: {e}")

    if not results:
        print("⚠️ No se pudo descargar ningún export válido.")
        if errores:
            df_errors = pd.DataFrame(errores, columns=["db_id", "export_id", "error"])
            save_to_google_sheet(df_errors, WORKSHEET_ERRORS)
        return

    combined_df = pd.concat(results, ignore_index=True)
    combined_df = combined_df[FINAL_ORDER]

    save_to_google_sheet(combined_df, WORKSHEET_COMBINED)

    if errores:
        df_errors = pd.DataFrame(errores, columns=["db_id", "export_id", "error"])
        save_to_google_sheet(df_errors, WORKSHEET_ERRORS)

    print(f"✅ Google Sheets actualizado: {WORKSHEET_COMBINED}")
    print(f"✅ Google Sheets actualizado: {WORKSHEET_MAPPING}")


if __name__ == "__main__":
    main()

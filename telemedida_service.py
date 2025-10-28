#!/usr/bin/env python
# coding: utf-8

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1
from sqlalchemy import create_engine, text

# Evita depender de gspread.models (no existe en versiones actuales)
Worksheet = Any

class TelemedidaService:
    def __init__(self):
        """Inicializa el servicio con configuración desde variables de entorno"""
        try:
            self.setup_logging()
            self.load_config()
            self.setup_database_connections()
            self.setup_google_sheets()
            self.logger.info("Servicio de telemedida inicializado correctamente")
        except Exception as e:
            # Si hay error en la inicialización, configurar logging básico
            logging.basicConfig(level=logging.ERROR)
            logger = logging.getLogger(__name__)
            logger.error(f"Error crítico durante la inicialización: {str(e)}")
            raise
        
    def setup_logging(self):
        """Configura el logging"""
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def load_config(self):
        """Carga la configuración desde variables de entorno"""
        # Base de datos
        self.db_username = os.getenv('DB_USERNAME', 'data')
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = int(os.getenv('DB_PORT', '5432'))
        self.db_metersight = os.getenv('DB_METERSIGHT')
        self.db_app_ops = os.getenv('DB_APP_OPS')
        
        # Google Sheets
        self.google_sheets_id = os.getenv('GOOGLE_SHEETS_ID')
        self.google_sheets_worksheet_name = os.getenv('GOOGLE_SHEETS_WORKSHEET_NAME', 'BD_Telemedida')
        
        # Fecha de filtro (siempre usa fecha actual)
        self.fecha_filtro = datetime.now().replace(tzinfo=timezone.utc)
        self.logger.info(f"Usando fecha de filtro: {self.fecha_filtro.strftime('%Y-%m-%d')}")
        
        # Validar configuración requerida
        required_vars = [
            'DB_PASSWORD', 'DB_HOST', 'DB_METERSIGHT', 'DB_APP_OPS',
            'GOOGLE_SHEETS_ID', 'GOOGLE_SERVICE_ACCOUNT_JSON'
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Faltan variables de entorno requeridas: {missing_vars}")
            
    def setup_database_connections(self):
        """Configura las conexiones a las bases de datos"""
        password_encoded = quote_plus(self.db_password)
        
        url_metersight = f"postgresql://{self.db_username}:{password_encoded}@{self.db_host}:{self.db_port}/{self.db_metersight}"
        url_app_ops = f"postgresql://{self.db_username}:{password_encoded}@{self.db_host}:{self.db_port}/{self.db_app_ops}"
        
        self.engine_metersight = self.conexion_db(url_metersight)
        self.engine_app_ops = self.conexion_db(url_app_ops)
        
    def setup_google_sheets(self):
        """Configura la conexión a Google Sheets"""
        # Parsear el JSON de service account desde variable de entorno
        service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not service_account_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON no está definida")
            
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error al parsear GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
            
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(self.google_sheets_id)
        self.worksheet = self.sh.worksheet(self.google_sheets_worksheet_name)
        
    def conexion_db(self, conn_string):
        """Función de conexión a base de datos"""
        try:
            engine = create_engine(conn_string)
            with engine.connect() as connection:
                self.logger.info("Conexión a la base de datos establecida y probada exitosamente.")
            return engine
        except Exception as e:
            self.logger.error(f"Error al crear o probar conexión a la base de datos: {e}")
            raise
            
    def extract_data_from_databases(self):
        """Extrae datos de las bases de datos"""
        query_metersight = text("""
            SELECT
                read_timestamp - interval '5 hour' AS read_timestamp_local,
                user_email,
                success,
                error,
                client_number,
                meter_factor,
                brand,
                serial,
                ip
            FROM cgm.metersight
            WHERE read_timestamp - interval '5 hour' >= :fecha AND  read_timestamp - interval '5 hour' <= :fecha + interval '1 day'
        """)

        query_app_ops = text("""
            WITH info_visit AS (
                SELECT * FROM visits
            ),
            meter_reading AS (
                SELECT *
                FROM dblink(
                    'dbname=bia-bi password=SlKJOH602q87f7enwyCAGRra user=data',
                    $$
                        SELECT
                            visit_id,
                            read_timestamp,
                            user_id,
                            success,
                            error,
                            meter_factor,
                            brand,
                            serial,
                            ip
                        FROM telemetry.meter_readings
                    $$
                ) AS t (
                    visit_id      TEXT,
                    read_timestamp TIMESTAMP,
                    user_id        TEXT,
                    success        BOOLEAN,
                    error          TEXT,
                    meter_factor   INTEGER,
                    brand          TEXT,
                    serial         TEXT,
                    ip             TEXT
                )
            )
            SELECT
                read_timestamp - interval '5 hour' AS read_timestamp_local,
                user_id,
                success,
                error,
                internal_bia_code,
                meter_factor,
                brand,
                serial,
                ip
            FROM meter_reading mr
            LEFT JOIN info_visit iv
                ON iv.id::TEXT = mr.visit_id::TEXT
            WHERE read_timestamp - interval '5 hour' >= :fecha AND  read_timestamp - interval '5 hour' <= :fecha + interval '1 day'
            ORDER BY 2 DESC
        """)

        # Ejecutar consultas con manejo de errores
        try:
            self.logger.info("Ejecutando consulta en base de datos metersight...")
            df_metersight = pd.read_sql(query_metersight, con=self.engine_metersight, params={"fecha": self.fecha_filtro})
            self.logger.info(f"Consulta metersight completada: {len(df_metersight)} registros")
        except Exception as e:
            self.logger.error(f"Error en consulta metersight: {str(e)}")
            raise Exception(f"Error extrayendo datos de metersight: {str(e)}")
        
        try:
            self.logger.info("Ejecutando consulta en base de datos app_ops...")
            df_app_ops = pd.read_sql(query_app_ops, con=self.engine_app_ops, params={"fecha": self.fecha_filtro})
            self.logger.info(f"Consulta app_ops completada: {len(df_app_ops)} registros")
        except Exception as e:
            self.logger.error(f"Error en consulta app_ops: {str(e)}")
            raise Exception(f"Error extrayendo datos de app_ops: {str(e)}")
        
        return df_metersight, df_app_ops
        
    def process_data(self, df_metersight, df_app_ops):
        """Procesa y combina los datos de ambas fuentes"""
        # Mapeo y limpieza de datos
        col_map = {
            "read_timestamp": "read_timestamp",
            "user_id":    "user_email",
            "success": "success",
            "error": "error",
            "internal_bia_code": "client_number",
            "meter_factor": "meter_factor",
            "brand": "brand",
            "serial": "serial",
            "ip": "ip"
        }

        df_app_ops_renamed = df_app_ops.rename(columns=col_map)

        # Ambas tablas mismas columnas
        df_total = pd.concat([df_metersight, df_app_ops_renamed], ignore_index=True)

        # Asegurarnos de que la columna de fecha sea tipo datetime
        df_total['read_timestamp_local'] = pd.to_datetime(df_total['read_timestamp_local'])

        # Ordenar descendente por fecha (la más nueva primero)
        df_sorted = df_total.sort_values('read_timestamp_local', ascending=False)

        # Eliminar duplicados conservando la primera aparición (la más nueva)
        df_unique = df_sorted.drop_duplicates(subset='client_number', keep='first')

        # Opcional: resetear el índice para que quede limpio
        df_unique = df_unique.reset_index(drop=True)

        self.logger.info(f"Registros únicos: {df_unique.shape[0]}")
        return df_unique
        
    def get_google_sheets_data(self):
        """Obtiene datos actuales de Google Sheets"""
        try:
            self.logger.info("Obteniendo datos de Google Sheets...")
            data = self.worksheet.get_all_records()
            df_sheet = pd.DataFrame(data)
            self.logger.info(f"Datos obtenidos de Google Sheets: {len(df_sheet)} filas")
            return df_sheet
        except Exception as e:
            self.logger.error(f"Error obteniendo datos de Google Sheets: {str(e)}")
            raise Exception(f"Error accediendo a Google Sheets: {str(e)}")
        
    # Helper functions (mantenidas del código original)
    def _col_to_index(self, col_letters: str) -> int:
        """Convierte letras de columna (A, AB…) a índice 0‑based."""
        idx = 0
        for ch in col_letters.upper():
            idx = idx * 26 + (ord(ch) - ord('A') + 1)
        return idx - 1

    def _a1_to_grid(self, sheet_id: int, a1_range: str) -> dict:
        """Convierte un rango A1 a un dict GridRange que entiende la API de Google Sheets."""
        if ":" in a1_range:
            start_a1, end_a1 = a1_range.split(":")
        else:
            start_a1 = end_a1 = a1_range

        col_start = "".join(filter(str.isalpha, start_a1))
        col_end   = "".join(filter(str.isalpha, end_a1))
        row_start = int("".join(filter(str.isdigit, start_a1))) - 1
        row_end   = int("".join(filter(str.isdigit, end_a1))) - 1

        return {
            "sheetId": sheet_id,
            "startRowIndex": row_start,
            "endRowIndex":   row_end + 1,
            "startColumnIndex": self._col_to_index(col_start),
            "endColumnIndex":   self._col_to_index(col_end) + 1,
        }

    def _format_date_mmddyyyy(self, dt: pd.Timestamp | datetime) -> str:
        """Formatea una fecha al estilo MM/DD/YYYY."""
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        return dt.strftime("%m/%d/%Y")

    def buscar_fila_por_codigo(self, worksheet: Worksheet, codigo: str, col_index: dict) -> int | None:
        """Busca código en la columna ID Interno."""
        try:
            cell = worksheet.find(codigo, in_column=col_index["ID Interno"])
            self.logger.info(f"Código '{codigo}' encontrado en la fila {cell.row}.")
            return cell.row
        except Exception:
            self.logger.warning(f"Código '{codigo}' NO encontrado.")
            return None

    def actualizar_fila_existente(self, worksheet: Worksheet, fila: int, col_index: dict, 
                                 serial_val: str, ip_val: str, factor_val: str, brand_val: str) -> None:
        """Actualiza solo las columnas que nos interesan en la fila ya existente."""
        # Medidor Principal
        worksheet.update(
            rowcol_to_a1(fila, col_index["Medidor Principal"]),
            [[serial_val]],
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"Medidor actualizado (fila {fila}) → {serial_val}")

        # IP Principal
        worksheet.update(
            rowcol_to_a1(fila, col_index["IP Principal"]),
            [[ip_val]],
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"IP actualizado (fila {fila}) → {ip_val}")

        # Factor Fx
        worksheet.update(
            rowcol_to_a1(fila, col_index["Factor \nFx"]),
            [[factor_val]],
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"Factor actualizado (fila {fila}) → {factor_val}")

        # Marca Medidor Activo (brand) – **SIEMPRE**
        worksheet.update(
            rowcol_to_a1(fila, col_index["Marca Medidor Activo"]),
            [[brand_val]],
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"Brand actualizado (fila {fila}) → {brand_val}")

    def obtener_ultima_fila_con_datos(self, worksheet: gspread.Worksheet, col_index: dict) -> int:
        """Devuelve el número de la última fila que contiene datos en la columna ID Interno."""
        id_vals = worksheet.col_values(col_index["ID Interno"])
        ultima = len(id_vals)
        while ultima > 1 and not id_vals[ultima - 1].strip():
            ultima -= 1
        return ultima

    def copiar_pegar_de_fila_anterior(self, worksheet: Worksheet, fila_nueva: int) -> None:
        """Copia los rangos de la fila anterior a la fila recién insertada."""
        sheet_id = worksheet.id

        pares = [
            (f"B{fila_nueva - 1}:V{fila_nueva - 1}", f"B{fila_nueva}:V{fila_nueva}"),
            (f"AE{fila_nueva - 1}", f"AE{fila_nueva}"),
            (f"AG{fila_nueva - 1}", f"AG{fila_nueva}"),
        ]

        peticiones = []
        for origen, destino in pares:
            peticiones.append({
                "copyPaste": {
                    "source": self._a1_to_grid(sheet_id, origen),
                    "destination": self._a1_to_grid(sheet_id, destino),
                    "pasteType": "PASTE_NORMAL",
                    "pasteOrientation": "NORMAL",
                }
            })

        worksheet.spreadsheet.batch_update({"requests": peticiones})
        self.logger.info(f"Copiado de rangos anteriores a la fila {fila_nueva}.")

    def insertar_fila_y_copiar_anteriores(self, worksheet: Worksheet, nueva_fila_idx: int, 
                                        encabezados: list, col_index: dict, codigo: str,
                                        serial_val: str, ip_val: str, factor_val: str, 
                                        brand_val: str, read_timestamp_local) -> None:
        """Inserta una fila nueva y copia datos de la fila anterior."""
        # Insertar fila vacía
        worksheet.insert_row(
            [""] * len(encabezados),
            index=nueva_fila_idx,
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"Fila insertada en la posición {nueva_fila_idx}")

        # Copiar rangos de la fila anterior
        self.copiar_pegar_de_fila_anterior(worksheet, nueva_fila_idx)

        # Rellenar los campos obligatorios
        worksheet.update(
            rowcol_to_a1(nueva_fila_idx, col_index["ID Interno"]),
            [[codigo]],
            value_input_option="USER_ENTERED",
        )
        worksheet.update(
            rowcol_to_a1(nueva_fila_idx, col_index["Medidor Principal"]),
            [[serial_val]],
            value_input_option="USER_ENTERED",
        )
        worksheet.update(
            rowcol_to_a1(nueva_fila_idx, col_index["IP Principal"]),
            [[ip_val]],
            value_input_option="USER_ENTERED",
        )
        worksheet.update(
            rowcol_to_a1(nueva_fila_idx, col_index["Factor \nFx"]),
            [[factor_val]],
            value_input_option="USER_ENTERED",
        )
        worksheet.update(
            rowcol_to_a1(nueva_fila_idx, col_index["Marca Medidor Activo"]),
            [[brand_val]],
            value_input_option="USER_ENTERED",
        )
        self.logger.info(f"Brand escrito en la fila {nueva_fila_idx} → {brand_val}")

        # Fecha Instalación (solo en inserción)
        if "Fecha Instalación\n(MM/DD/YYYY)" in col_index:
            fecha_formateada = self._format_date_mmddyyyy(read_timestamp_local)
            worksheet.update(
                rowcol_to_a1(nueva_fila_idx, col_index["Fecha Instalación\n(MM/DD/YYYY)"]),
                [[fecha_formateada]],
                value_input_option="USER_ENTERED",
            )
            self.logger.info(f"Fecha instalación escrita en la fila {nueva_fila_idx} → {fecha_formateada}")

    def colorear_fila_completa(self, worksheet: Worksheet, fila: int, hex_color: str = "FFFF00") -> None:
        """Aplica un fondo de color a toda la fila indicada."""
        encabezados = worksheet.row_values(1)
        total_cols = len(encabezados)

        request = {
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": fila - 1,
                    "endRowIndex":   fila,
                    "startColumnIndex": 0,
                    "endColumnIndex":   total_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red":   int(hex_color[0:2], 16) / 255,
                            "green": int(hex_color[2:4], 16) / 255,
                            "blue":  int(hex_color[4:6], 16) / 255,
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        }

        worksheet.spreadsheet.batch_update({"requests": [request]})
        self.logger.info(f"Fila {fila} coloreada con {hex_color}.")

    def procesar_codigo(self, worksheet: Worksheet, df_sheet: pd.DataFrame, 
                       df_unique: pd.DataFrame, codigo: str) -> bool:
        """Procesa un código individual (actualizar o insertar)"""
        if "ID Interno" not in df_sheet.columns:
            raise KeyError("La hoja no contiene la columna 'ID Interno'")

        # Obtener datos de df_unique
        fila_info = df_unique.loc[df_unique["client_number"] == codigo]
        if fila_info.empty:
            raise ValueError(f"El código {codigo} no está presente en df_unique")

        serial_val = fila_info.iloc[0]["serial"]
        ip_val = fila_info.iloc[0]["ip"]
        factor_val = str(fila_info.iloc[0]["meter_factor"])
        brand_val = fila_info.iloc[0]["brand"]
        read_timestamp = fila_info.iloc[0]["read_timestamp_local"]

        # Mapeo de encabezados
        encabezados = worksheet.row_values(1)
        col_index = {nombre: idx + 1 for idx, nombre in enumerate(encabezados)}

        # Verificar columnas obligatorias
        columnas_obligatorias = [
            "ID Interno", "Medidor Principal", "IP Principal", 
            "Factor \nFx", "Marca Medidor Activo"
        ]
        for col in columnas_obligatorias:
            if col not in col_index:
                raise KeyError(f"La hoja no contiene la columna '{col}'")

        # Buscar código
        fila_en_hoja = self.buscar_fila_por_codigo(worksheet, codigo, col_index)

        if fila_en_hoja is not None:
            # Actualizar fila existente
            self.actualizar_fila_existente(
                worksheet, fila_en_hoja, col_index, 
                serial_val, ip_val, factor_val, brand_val
            )
            self.colorear_fila_completa(worksheet, fila_en_hoja)
            return True
        else:
            # Insertar nueva fila
            ultima_fila_con_dato = self.obtener_ultima_fila_con_datos(worksheet, col_index)
            nueva_fila_idx = ultima_fila_con_dato + 1

            self.insertar_fila_y_copiar_anteriores(
                worksheet, nueva_fila_idx, encabezados, col_index, codigo,
                serial_val, ip_val, factor_val, brand_val, read_timestamp
            )
            self.colorear_fila_completa(worksheet, nueva_fila_idx)
            return False

    def process_all_codes(self):
        """Procesa todos los códigos únicos encontrados en la base de datos"""
        try:
            # Extraer datos de las bases de datos
            df_metersight, df_app_ops = self.extract_data_from_databases()
            
            # Procesar y combinar datos
            df_unique = self.process_data(df_metersight, df_app_ops)
            
            # Obtener datos actuales de Google Sheets
            df_sheet = self.get_google_sheets_data()
            
            # Procesar cada código
            results = {
                'updated': [],
                'inserted': [],
                'errors': []
            }
            
            for client in df_unique["client_number"]:
                try:
                    existe = self.procesar_codigo(self.worksheet, df_sheet, df_unique, client)
                    
                    if existe:
                        results['updated'].append(client)
                        self.logger.info(f"✅ El código {client} YA ESTABA EN LA HOJA → se actualizaron los campos.")
                    else:
                        results['inserted'].append(client)
                        self.logger.info(f"🆕 El código {client} NO ESTABA → se insertó una fila nueva.")
                        
                except Exception as e:
                    error_msg = f"Error procesando código {client}: {str(e)}"
                    results['errors'].append(error_msg)
                    self.logger.error(error_msg)
            
            return {
                'success': True,
                'total_processed': len(df_unique),
                'updated_count': len(results['updated']),
                'inserted_count': len(results['inserted']),
                'error_count': len(results['errors']),
                'results': results
            }
            
        except Exception as e:
            self.logger.error(f"Error en process_all_codes: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Función principal del microservicio"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("INICIANDO MICROSERVICIO DE TELEMEDIDA")
        logger.info("=" * 60)
        
        # Inicializar el servicio
        logger.info("Inicializando servicio de telemedida...")
        service = TelemedidaService()
        
        # Procesar todos los códigos
        logger.info("Iniciando procesamiento de códigos...")
        result = service.process_all_codes()
        
        if result['success']:
            logger.info("=" * 60)
            logger.info("PROCESAMIENTO COMPLETADO EXITOSAMENTE")
            logger.info(f"Total procesados: {result['total_processed']}")
            logger.info(f"Actualizados: {result['updated_count']}")
            logger.info(f"Insertados: {result['inserted_count']}")
            logger.info(f"Errores: {result['error_count']}")
            logger.info("=" * 60)
            
            # Log de códigos procesados si hay errores
            if result['error_count'] > 0:
                logger.warning("Códigos con errores:")
                for error in result['results']['errors']:
                    logger.warning(f"  - {error}")
        else:
            logger.error("=" * 60)
            logger.error("ERROR EN EL PROCESAMIENTO")
            logger.error(f"Error: {result['error']}")
            logger.error("=" * 60)
            return False
            
    except Exception as e:
        logger.error("=" * 60)
        logger.error("ERROR CRÍTICO EN EL MICROSERVICIO")
        logger.error(f"Error inesperado: {str(e)}")
        logger.error("=" * 60)
        return False
    
    return True


if __name__ == "__main__":
    # Configurar logging básico para el punto de entrada
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('telemedida_service.log', mode='a')
        ]
    )
    
    # Ejecutar el servicio
    success = main()
    
    if success:
        print("Servicio ejecutado exitosamente")
        exit(0)
    else:
        print("Servicio terminó con errores")
        exit(1)

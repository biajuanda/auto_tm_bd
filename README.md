# Microservicio de Sincronización de Datos de Telemedida

Este microservicio sincroniza datos de telemedida desde bases de datos PostgreSQL hacia Google Sheets, procesando lecturas de medidores y actualizando información de clientes. El servicio se ejecuta automáticamente usando la fecha actual como filtro.

## Descripción del Funcionamiento

El sistema extrae datos de dos fuentes principales:
- **Base de datos `bia-bi`**: Tabla `cgm.metersight` con lecturas de medidores
- **Base de datos `electrician-visits`**: Tabla `visits` y `telemetry.meter_readings`

Los datos se procesan, combinan y sincronizan con una hoja de Google Sheets, actualizando registros existentes o insertando nuevos según corresponda.

## Estructura del Código

```
/auto_tm_bd/
├── telemedida_service.py         # Microservicio principal con punto de entrada
├── requirements.txt              # Dependencias Python
├── Dockerfile                    # Configuración Docker
└── README.md                     # Este archivo
```

## Arquitectura del Sistema

### 1. Clase `TelemedidaService`

La clase principal que maneja toda la lógica del sistema:

#### Inicialización
- **`__init__()`**: Configura logging, carga configuración, establece conexiones a bases de datos y Google Sheets con manejo robusto de errores
- **`setup_logging()`**: Configura el sistema de logging con nivel configurable
- **`load_config()`**: Carga variables de entorno y valida configuración requerida, siempre usa fecha actual
- **`setup_database_connections()`**: Establece conexiones a ambas bases de datos PostgreSQL
- **`setup_google_sheets()`**: Configura autenticación y conexión a Google Sheets

#### Extracción de Datos
- **`extract_data_from_databases()`**: Ejecuta consultas SQL en ambas bases de datos con manejo robusto de errores
  - Consulta `cgm.metersight` para datos de medidores
  - Consulta `visits` y `telemetry.meter_readings` usando `dblink` para datos de visitas
  - Aplica filtro de fecha actual automáticamente

#### Procesamiento de Datos
- **`process_data()`**: Combina y limpia datos de ambas fuentes
  - Mapea columnas para unificar estructura
  - Elimina duplicados conservando el registro más reciente
  - Ordena por fecha de lectura (más reciente primero)

#### Sincronización con Google Sheets
- **`get_google_sheets_data()`**: Obtiene datos actuales de la hoja con manejo de errores
- **`procesar_codigo()`**: Procesa cada código individual:
  - Busca si el código ya existe en la hoja
  - Si existe: actualiza campos específicos
  - Si no existe: inserta nueva fila copiando datos de la fila anterior

### 2. Funciones de Utilidad

#### Búsqueda y Localización
- **`buscar_fila_por_codigo()`**: Busca un código específico en la columna "ID Interno"
- **`obtener_ultima_fila_con_datos()`**: Encuentra la última fila con datos para insertar nuevas filas

#### Actualización de Datos
- **`actualizar_fila_existente()`**: Actualiza campos específicos en fila existente:
  - Medidor Principal (serial)
  - IP Principal
  - Factor Fx
  - Marca Medidor Activo
- **`insertar_fila_y_copiar_anteriores()`**: Inserta nueva fila y copia datos de la fila anterior

#### Formateo y Presentación
- **`colorear_fila_completa()`**: Aplica color de fondo a filas procesadas
- **`_format_date_mmddyyyy()`**: Formatea fechas al estilo MM/DD/YYYY
- **`_a1_to_grid()`**: Convierte rangos A1 a formato GridRange de Google Sheets API

### 3. Punto de Entrada Principal

- **`main()`**: Función principal del microservicio que:
  - Inicializa el servicio con manejo robusto de errores
  - Ejecuta el procesamiento completo
  - Genera logs detallados de inicio, progreso y resultado
  - Maneja errores críticos para evitar loops infinitos
  - Retorna códigos de salida apropiados (0 = éxito, 1 = error)

### 4. Flujo de Procesamiento

1. **Inicialización**: Se cargan configuraciones y se establecen conexiones
2. **Extracción**: Se consultan ambas bases de datos con filtro de fecha actual
3. **Procesamiento**: Se combinan y limpian los datos, eliminando duplicados
4. **Sincronización**: Para cada código único:
   - Se verifica si existe en Google Sheets
   - Se actualiza o inserta según corresponda
   - Se aplica formato visual (coloreado)
5. **Resultado**: Se genera resumen detallado y logs de la ejecución

## Configuración Requerida

### Variables de Entorno

```bash
# Base de datos PostgreSQL
DB_USERNAME=usuario_bd
DB_PASSWORD=password_bd
DB_HOST=host_bd
DB_PORT=5432
DB_METERSIGHT=nombre_bd_metersight
DB_APP_OPS=nombre_bd_app_ops

# Google Sheets
GOOGLE_SHEETS_ID=id_de_la_hoja
GOOGLE_SHEETS_WORKSHEET_NAME=nombre_de_la_pestaña

# Google Service Account (JSON como string)
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}

# Configuración opcional
LOG_LEVEL=INFO          # Nivel de logging (INFO, DEBUG, WARNING, ERROR)
```

## Uso del Sistema

### Ejecución del Microservicio

```bash
# Ejecutar directamente
python telemedida_service.py

# O usando Docker
docker run --env-file .env telemedida-service:latest
```

### Ejecución Programática

```python
from telemedida_service import TelemedidaService

# Crear instancia del servicio
service = TelemedidaService()

# Procesar todos los códigos
result = service.process_all_codes()

# Verificar resultado
if result['success']:
    print(f"Procesados: {result['total_processed']}")
    print(f"Actualizados: {result['updated_count']}")
    print(f"Insertados: {result['inserted_count']}")
    print(f"Errores: {result['error_count']}")
else:
    print(f"Error: {result['error']}")
```

### Estructura de Respuesta

```python
{
    'success': True,
    'total_processed': 150,
    'updated_count': 120,
    'inserted_count': 30,
    'error_count': 0,
    'results': {
        'updated': ['CO0100005723', 'CO0100005724', ...],
        'inserted': ['CO0100005800', 'CO0100005801', ...],
        'errors': []
    }
}
```

## Características Técnicas

### Manejo de Duplicados
- Se eliminan duplicados basándose en `client_number`
- Se conserva el registro más reciente según `read_timestamp_local`

### Copia de Datos Anteriores
- Al insertar nuevas filas, se copian automáticamente rangos de la fila anterior
- Esto mantiene consistencia en el formato y datos históricos

### Formateo Visual
- Las filas procesadas se colorean para indicar que fueron actualizadas
- Se aplica formato de fecha consistente (MM/DD/YYYY)

### Manejo de Errores
- Logging detallado de todas las operaciones con timestamps
- Logs guardados tanto en consola como en archivo (`telemedida_service.log`)
- Manejo robusto de errores en cada etapa del proceso
- Continuación del procesamiento aunque fallen códigos individuales
- Retorno de códigos de salida apropiados (0 = éxito, 1 = error)
- Prevención de loops infinitos mediante try-catch globales

### Logging
- **Nivel configurable**: INFO, DEBUG, WARNING, ERROR
- **Doble salida**: Consola y archivo de log
- **Formato consistente**: Timestamp, nivel, mensaje
- **Logs de progreso**: Inicio, etapas intermedias, resultado final
- **Logs de error**: Detalles específicos de cada fallo

---


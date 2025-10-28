import json
import os
import logging
from telemedida_service import TelemedidaService

def lambda_handler(event, context):
    """
    Handler para AWS Lambda que procesa datos de telemedida y los sincroniza con Google Sheets.
    
    El evento puede contener parámetros opcionales:
    - filter_date: Fecha de filtro en formato YYYY-MM-DD (opcional, usa variable de entorno por defecto)
    - force_update: Si es true, fuerza la actualización de todos los registros (opcional)
    
    Ejemplo de event:
    {
        "filter_date": "2025-10-26",
        "force_update": false
    }
    """
    try:
        # Configurar logging
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # Procesar parámetros del evento
        filter_date = event.get('filter_date')
        force_update = event.get('force_update', False)
        
        # Si se proporciona una fecha de filtro, actualizar la variable de entorno
        if filter_date:
            os.environ['FILTER_DATE'] = filter_date
            logger.info(f"Usando fecha de filtro del evento: {filter_date}")
        
        # Inicializar el servicio de telemedida
        logger.info("Inicializando servicio de telemedida...")
        service = TelemedidaService()
        
        # Procesar todos los códigos
        logger.info("Iniciando procesamiento de códigos...")
        result = service.process_all_codes()
        
        if result['success']:
            logger.info(f"Procesamiento completado exitosamente. "
                       f"Total: {result['total_processed']}, "
                       f"Actualizados: {result['updated_count']}, "
                       f"Insertados: {result['inserted_count']}, "
                       f"Errores: {result['error_count']}")
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Procesamiento completado exitosamente",
                    "summary": {
                        "total_processed": result['total_processed'],
                        "updated_count": result['updated_count'],
                        "inserted_count": result['inserted_count'],
                        "error_count": result['error_count']
                    },
                    "details": result['results']
                })
            }
        else:
            logger.error(f"Error en el procesamiento: {result['error']}")
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": "Error en el procesamiento",
                    "details": result['error']
                })
            }
            
    except Exception as e:
        logger.error(f"Error inesperado en lambda_handler: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Error interno del servidor",
                "details": str(e)
            })
        } 
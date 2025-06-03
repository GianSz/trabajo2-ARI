import base64
import json
import boto3
import decimal

# Crear cliente SNS
sns = boto3.client('sns')
# Reemplaza con el ARN de tu tópico SNS
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:250508015438:trabajo2notificacion'

def lambda_handler(event, context):
    message = f"¡Alerta de inventario bajo! ¡Revisa el stock antes de que se agote!\n\n"

    for record in event['Records']:
        print(record)
 
        payload_bytes = base64.b64decode(record['kinesis']['data'])
        payload_str = payload_bytes.decode('utf-8')

        payload = json.loads(payload_str)
        stock_code = payload.get('stockcode')
        total_quantity = payload.get('total_quantity')

        # Guardar en dynomdb
        save_to_db(stock_code, total_quantity)
        message += f"Producto: {stock_code} - Total vendido: {total_quantity}\n"

        # Publicar mensaje en SNS
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='⚠️ Inventario bajo detectado',
            Message=message
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Notificación enviada con éxito.')
    }


def save_to_db(stock_code, total_quantity):
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('acmecoOrders-flink')

    with table.batch_writer() as batch_writer:
        batch_writer.put_item(                        
            Item = {
                'StockCode': stock_code,
                'TotalQuantity': decimal.Decimal(total_quantity)
            }
        )
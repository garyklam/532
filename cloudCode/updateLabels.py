import json
import boto3

db = boto3.resource('dynamodb', endpoint_url = 'https://dynamodb.us-west-2.amazonaws.com')
table = db.Table('532data')

def lambda_handler(event, context):
    count = event['count']
    label = event['flag']
    if label == 1:
        prev_count = int(count) - 1
        prev = table.get_item(
            Key={
                'count': f'{prev_count}'
            })
        if prev['Item']['flag'] == 0:
            for i in range(21):
                prev_count = int(count) - i
                table.update_item(
                    Key={
                        'count': f'{prev_count}'
                    },
                    UpdateExpression="set flag=:f",
                    ExpressionAttributeValues={
                        ':f': 1
                    },
                    ReturnValues="UPDATED_NEW"
                )
    return {
        'statusCode': 200
    }

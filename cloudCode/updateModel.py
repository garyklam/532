import json
import boto3
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import pickle


s3 = boto3.client('s3')
db = boto3.resource('dynamodb')

def lambda_handler(event, context):
    count = event['count']
    table = db.Table('532data')
    scan_kwargs = {
        'ProjectionExpression': "#avg, delta, #total, total_time, #time, flag",
        'ExpressionAttributeNames': {"#avg": "avg", '#total': 'total', '#time': 'time'}
    }
    data = pd.DataFrame(columns=['avg', 'delta', 'time', 'total', 'total_time', 'flag'])
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        data = data.append(response.get('Items', []), ignore_index=True)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    x = data[['avg', 'delta', 'total', 'total_time']]
    y = data['flag'].astype(int)
    time = data['time'].str[6:8]
    time_x = x.assign(time=time.values)
    time_x['time'] = time_x['time'].astype(int)
    model = RandomForestClassifier().fit(time_x, y)
    
    
    filename = f'V{int(count)//1000}'
    filepath = f'/tmp/{filename}'
    file = open(filepath, 'wb')
    pickle.dump(model, file)
    file.close()
    s3.put_object(Bucket='532models', Key=filename, Body=open(filepath, 'rb'))


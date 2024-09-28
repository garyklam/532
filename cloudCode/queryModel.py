import json
import boto3
import pickle
import pandas as pd
import sklearn


s3 = boto3.resource('s3')
iot = boto3.client('iot-data', region_name='us-west-2')

def lambda_handler(event, context):
    count = int(event['count'])
    filename = f'V{(count-4)//1000}'
    
    response = s3.Bucket('532models').Object(filename).get()
    
    model = pickle.loads(response['Body'].read())
    time = event['time'][6:8]
    sample = [[event['avg'], event['total_time'], event['delta'], event['total'], int(time)]]
    test = pd.DataFrame(sample, columns=['avg', 'total_time', 'delta', 'total', 'time'])
    flag = model.predict(test)
    
    if flag[0] == 1:
        iot.publish(
                topic='532/prediction',
                qos=1,
                payload=json.dumps({'prediction': f'{flag[0]}', "count": count, "model": filename})
            )
        
    
    if count % 1000 == 0:
        child = boto3.client('lambda')
        params = {'count': f'{count}'}
        child.invoke(
            FunctionName = 'arn:aws:lambda:us-west-2:286648262662:function:updateModel',
            InvocationType = 'RequestResponse',
            Payload = json.dumps(params)
            )
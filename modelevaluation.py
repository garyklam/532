import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import recall_score, precision_score, f1_score, roc_curve, auc
import boto3


def train(data):
    x = data[['avg', 'delta', 'total', 'total_time']]
    y = data['flag'].astype(int)
    model = RandomForestClassifier().fit(x, y)
    return model

def train_with_time(data):
    x = data[['avg', 'delta', 'total', 'total_time']]
    y = data['flag'].astype(int)
    time = data['time'].str[6:8]
    time_x = x.assign(time=time.values)
    time_x['time'] = time_x['time'].astype(int)
    model = RandomForestClassifier().fit(time_x, y)
    return model

def test(x_test, y_test, model):
    predictions = model.predict(x_test)
    fpr, tpr, thresholds = roc_curve(y_test, predictions)
    roc_auc = round(auc(fpr, tpr), 2)
    precision = round(precision_score(predictions, y_test, pos_label=1), 2)
    recall = round(recall_score(predictions, y_test, pos_label=1), 2)
    f1 = round(f1_score(predictions, y_test, pos_label=1), 2)
    return {'Precision': precision, 'Recall': recall, 'F1': f1, 'AUC': roc_auc}


def separate_count(data, high, low=1):
    data['count'] = data['count'].astype(int)
    high_mask = (data['count'] <= high)
    data = data[high_mask]
    low_mask = (data['count'] > low)
    data = data[low_mask]
    return data


def evaluate_improvement(maximum, data):
    high = maximum//500
    scores = {}
    for i in range(1, high):
        train_data = separate_count(data, i*500)
        test_data = separate_count(data, (i+1)*500, 1*500)
        x = test_data[['avg', 'delta', 'total', 'total_time']]
        y = test_data['flag'].astype(int)
        model = train_with_time(train_data)
        time = test_data['time'].str[6:8]
        time_x = x.assign(time=time.values)
        time_x['time'] = time_x['time'].astype(int)
        scores[f'{i*500}'] = test(time_x, y, model)
    return scores



def evaluate_overall(maximum):
    data = separate_count(maximum)
    x = data[['avg', 'delta', 'total', 'total_time']]
    y = data['flag'].astype(int)
    time = data['time'].str[6:8]
    time_x = x.assign(time=time.values)
    time_x['time'] = time_x['time'].astype(int)
    x_train, x_test, y_train, y_test = train_test_split(time_x, y, test_size=0.3, random_state=0)
    model = RandomForestClassifier().fit(x_train, y_train)
    predictions = model.predict(x_test)
    fpr, tpr, thresholds = roc_curve(y_test, predictions)
    roc_auc = round(auc(fpr, tpr), 2)
    precision = round(precision_score(predictions, y_test, pos_label=1), 2)
    recall = round(recall_score(predictions, y_test, pos_label=1), 2)
    f1 = round(f1_score(predictions, y_test, pos_label=1), 2)
    print(f'Precision: {precision} Recall: {recall} F1: {f1} AUC: {roc_auc}')


if __name__ == "__main__":

    key = pd.read_csv('lam_admin_accessKeys.csv')
    access_key = key['Access key ID'][0]
    secret_access_key = key['Secret access key'][0]
    session = boto3.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)
    dynamodb = session.resource('dynamodb')

    table = dynamodb.Table('532data')
    scan_kwargs = {
        'ProjectionExpression': "#count, #avg, delta, #total, total_time, #time, flag",
        'ExpressionAttributeNames': {'#count': 'count', "#avg": "avg", '#total': 'total', '#time': 'time'}
    }
    data = pd.DataFrame(columns=['count', 'avg', 'delta', 'time', 'total', 'total_time', 'flag'])
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        data = data.append(response.get('Items', []), ignore_index=True)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    print(evaluate_improvement(7000, data))











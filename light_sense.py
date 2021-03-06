from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
from uuid import uuid4
import json
from datetime import datetime
import time
from statistics import mean
import RPi.GPIO as GPIO
import argparse

parser = argparse.ArgumentParser(description="Monitor light data, store in Cloud and predict eye strain")
parser.add_argument('--start', type=int, default="hw1/test", help="Data count to start with")
parser.add_argument('--end', type=int, default="Hello World!", help="Data count to end with")
io.init_logging(getattr(io.LogLevel, io.LogLevel.NoLogs.name), 'stderr')

args = parser.parse_args()

def RCtime(RCpin):
    reading = 0
    GPIO.setup(RCpin, GPIO.OUT)
    GPIO.output(RCpin, GPIO.LOW)
    time.sleep(.5)
    GPIO.setup(RCpin, GPIO.IN)
    while GPIO.input(RCpin) == GPIO.LOW:
        reading += 1
    return reading


def readswitch():
    GPIO.setmode(GPIO.BOARD)
    GPIO.setwarnings(False)
    GPIO.setup(19, GPIO.IN)
    return GPIO.input(19)


def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


if __name__ == '__main__':
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint="arit3ikxcci9r-ats.iot.us-west-2.amazonaws.com",
            cert_filepath="/home/pi/certs/device.pem.crt",
            pri_key_filepath="/home/pi//certs/private.pem.key",
            client_bootstrap=client_bootstrap,
            ca_filepath="/home/pi/certs/AmazonRootCA1.pem",
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=str(uuid4()),
            clean_session=False,
            keep_alive_secs=30)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    GPIO.setmode(GPIO.BOARD)
    GPIO.setwarnings(False)
    total = 0
    for i in range(args.start, args.end):
        measurements = []
        start = datetime.now()
        curr = datetime.now()
        while (curr-start).total_seconds() < 30:
            measurements.append(round((10000/RCtime(12)), 2))
            curr = datetime.now()
        total += 0.05 * round(mean(measurements), 2)
        message = {'count': f'{i}',
                   'time': f'{curr.month}/{str(curr.day).zfill(2)} {str(curr.hour).zfill(2)}:{str(curr.minute).zfill(2)}:{str(curr.second).zfill(2)}',
                   'delta': round((max(measurements)-min(measurements)), 2),
                   'avg': round(mean(measurements), 2),
                   'total': total,
                   'total_time': (i+1-args.start)*30,
                   'flag': readswitch()}
        message_json = json.dumps(message)
        mqtt_connection.publish(
            topic='532/light',
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE)


    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

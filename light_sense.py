from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
from uuid import uuid4
import json
from datetime import datetime
import time
from statistics import mean
import RPi.GPIO as GPIO


io.init_logging(getattr(io.LogLevel, io.LogLevel.NoLogs.name), 'stderr')


def RCtime(RCpin):
    reading = 0
    GPIO.setup(RCpin, GPIO.OUT)
    GPIO.output(RCpin, GPIO.LOW)
    time.sleep(.5)
    GPIO.setup(RCpin, GPIO.IN)
    while GPIO.input(RCpin) == GPIO.LOW:
        reading += 1
    return reading


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

    DEBUG = 1
    GPIO.setmode(GPIO.BOARD)
    GPIO.setwarnings(False)
    total = 0
    for i in range(4):
        measurements = []
        start = datetime.now()
        curr = datetime.now()
        while (curr-start).total_seconds() < 5:
            measurements.append(RCtime(12))
            curr = datetime.now()
        total += 5 * mean(measurements) // 1
        message = {'count': i,
                   'time': f'{curr.hour}:{curr.minute}:{curr.second}',
                   'delta': max(measurements)-min(measurements),
                   'avg': mean(measurements) // 1,
                   'total': total,
                   'blink': 0}
        message_json = json.dumps(message)
        mqtt_connection.publish(
            topic='test',
            payload=message_json,
            qos=mqtt.QoS.AT_LEAST_ONCE)
        TOTAL = 0

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

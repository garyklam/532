# CSS 532 HW 1
# Modified from AWS SDK sample files

import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import threading
import time
from uuid import uuid4
import json
from statistics import mean
from random import randint


parser = argparse.ArgumentParser(description="Send and receive messages through and MQTT connection.")
parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name,
    help='Logging level')
parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")

# Using globals to simplify sample code
args = parser.parse_args()

io.init_logging(getattr(io.LogLevel, args.verbosity), 'stderr')

received_count = 0
received_all_event = threading.Event()


# Callback when connection is accidentally lost.
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
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=30)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    # Publish message to server desired number of times.
    # This step is skipped if message is blank.
    # This step loops forever if count was set to 0.
    total = 0
    while i in range(2):
        for j in range(5):
            measurements = []
            sample = randint(100, 200)
            measurements.append(sample)
            time.sleep(1)
        total += sum(measurements)
        message = {'count': i, 'max': max(measurements), 'min': min(measurements), 'avg': mean(measurements)}
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

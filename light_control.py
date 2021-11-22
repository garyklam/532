# CSS 532 HW 1
# Modified from AWS SDK sample files

import argparse
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import sys
import time
from uuid import uuid4
import json


io.init_logging(getattr(io.LogLevel, io.LogLevel.NoLogs.name), 'stderr')


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


def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload):
    print("Received message from topic '{}': {}".format(topic, payload))
    message = payload.decode('UTF-8')
    print(message)
    # if message['prediction'] == "1":
    #     print("alert")


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
    print("Waiting on alert")

    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic="532/prediction",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed to predictions")

    while True:
        time.sleep(10)


    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
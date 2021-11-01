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
from datetime import datetime
from random import randint


parser = argparse.ArgumentParser(description="Send and receive messages through and MQTT connection.")
parser.add_argument('--topic', default="hw1/test", help="Topic to subscribe to, and publish messages to.")
parser.add_argument('--message', default="Hello World!", help="Message to publish. " +
                                                              "Specify empty string to publish nothing.")
parser.add_argument('--count', default=3, type=int, help="Number of messages to publish/receive before exiting. " +
                                                          "Specify 0 to run forever.")
parser.add_argument('--verbosity', choices=[x.name for x in io.LogLevel], default=io.LogLevel.NoLogs.name,
    help='Logging level')
parser.add_argument('--client-id', default="test-" + str(uuid4()), help="Client ID for MQTT connection.")
parser.add_argument('--question5', default=False)

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


def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    if topic == "hw1/q4":
        cur_time = datetime.now()
        mqtt_connection.publish(
            topic="hw1/response",
            payload=json.dumps(cur_time.strftime('%H:%M:%S')),
            qos=mqtt.QoS.AT_LEAST_ONCE)
    if received_count == args.count:
        received_all_event.set()


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

    if args.question5:
        print("Sending time data...")
        for i in range(3):
            delay = randint(1, 3)
            now = datetime.now()
            message = {"count": i, "time": {"hour": now.hour, "minute": now.minute, "second": now.second}}
            message_json = json.dumps(message)
            mqtt_connection.publish(
                topic="hw1/q5",
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE)
            time.sleep(delay)
        print("Finished")

    else:
        # Subscribe
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic="hw1/+",
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=on_message_received)

        subscribe_result = subscribe_future.result()
        print("Subscribed to hw1/+")

        # Publish message to server desired number of times.
        # This step is skipped if message is blank.
        # This step loops forever if count was set to 0.
        if args.message:
            if args.count == 0:
                print("Sending messages until program killed")
            else:
                print("Sending {} message(s)".format(args.count))

            publish_count = 1
            while (publish_count <= args.count) or (args.count == 0):
                message = {"message": "{}".format(args.message), "count": "{}".format(publish_count)}
                message_json = json.dumps(message)
                mqtt_connection.publish(
                    topic=args.topic,
                    payload=message_json,
                    qos=mqtt.QoS.AT_LEAST_ONCE)
                time.sleep(1)
                publish_count += 1

        # Wait for all messages to be received.
        # This waits forever if count was set to 0.
        if args.count != 0 and not received_all_event.is_set():
            print("Waiting for all messages to be received...")

        received_all_event.wait()

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
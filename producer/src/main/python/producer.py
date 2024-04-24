import websocket
import os
from dotenv import load_dotenv
# from kafka_utils import producer, topics
import socket
import time
import json
load_dotenv()

API_KEY=os.getenv("FINNHUB_API")
# topic = topics["topic_0"]

def on_message(ws, message):
    msgs = json.loads(message)["data"]
    for msg in msgs:
        key = str(msg["t"]+int(time.time()))
        print(key, msg)
        # producer.produce("topic_0", key=key, value=json.dumps(msg), callback=ack)
        # producer.poll(.5)


def ack(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (msg))


def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AMD"}')


if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={API_KEY}",
                              on_message = on_message,

                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
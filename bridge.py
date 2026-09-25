import json
import os
import re
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
import serial
from dotenv import load_dotenv

load_dotenv()

# =====================================
# CONFIGURATION
# =====================================

MQTT_BROKER = os.environ.get('MQTT_BROKER_URL')
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
BRANCH_ID = os.environ.get('BRANCH_ID')

SERIAL_PORT_PATH = os.environ.get('SERIAL_PORT_PATH', '/dev/ttyUSB0')
BAUDRATE = int(os.environ.get('BAUDRATE', '9600'))
# Frame terminator sent by the scale. Supports escape sequences (e.g. '\x1b' for
# ESC-terminated protocols like Aczet) since .env values are read as literal text.
SERIAL_DELIMITER = os.environ.get('SERIAL_DELIMITER', '\\n').encode().decode('unicode_escape').encode('latin-1')

# Streaming + heartbeat config
STREAM_INTERVAL_MS = int(os.environ.get('STREAM_INTERVAL_MS', '1500'))
STREAM_CEILING_MS = int(os.environ.get('STREAM_CEILING_MS', '180000'))

# Fallback for single-value frames (e.g. Aczet: "enter.!G            372.37") — matches the
# trailing "<mode letter><spaces><number>" regardless of what junk precedes it on the line.
SINGLE_VALUE_FRAME_PATTERN = re.compile(r'([A-Za-z])\s+(-?\d+(?:\.\d+)?)\s*$')
HEARTBEAT_INTERVAL_MS = 60000

# =====================================
# LOGGING (IST)
# =====================================

IST = ZoneInfo('Asia/Kolkata')


def log(level, message):
    timestamp = datetime.now(IST).strftime('%d/%m/%Y, %H:%M:%S')
    print(f'[{timestamp}] [{level}] {message}')


def iso_now():
    now = datetime.now(timezone.utc)
    return now.strftime('%Y-%m-%dT%H:%M:%S.') + f'{now.microsecond // 1000:03d}Z'


log('INFO', 'Scale Bridge Starting')
log('INFO', f'Branch ID: {BRANCH_ID}')
log('INFO', f'Serial Port: {SERIAL_PORT_PATH}')
log('INFO', f'Serial Delimiter: {SERIAL_DELIMITER!r}')
log('INFO', f'MQTT Broker: {MQTT_BROKER}')

# =====================================
# STATE
# =====================================

gross_weight = 0.0
net_weight = 0.0
tare_weight = 0.0
is_scale_connected = False

# Streaming state
stream_timer = None
stream_ceiling_timer = None

# Vault/shelf identity — captured from the most recent READ cmd topic
current_vault_number = None
current_shelf_number = None
# Recon context (e.g. PRE_PLACEMENT_RECON) — captured from the most recent READ cmd
current_context = None
# packet_id — captured alongside context so REGISTER_WEIGHT samples stay attributable
current_packet_id = None

ser = None
mqtt_client = None


# =====================================
# setInterval()-STYLE REPEATING TIMER
# =====================================

class RepeatingTimer:
    def __init__(self, interval_seconds, function):
        self._interval = interval_seconds
        self._function = function
        self._timer = None
        self._running = False

    def _run(self):
        if not self._running:
            return
        self._function()
        if self._running:
            self._timer = threading.Timer(self._interval, self._run)
            self._timer.daemon = True
            self._timer.start()

    def start(self):
        self._running = True
        self._timer = threading.Timer(self._interval, self._run)
        self._timer.daemon = True
        self._timer.start()
        return self

    def cancel(self):
        self._running = False
        if self._timer:
            self._timer.cancel()


# =====================================
# SERIAL PORT SETUP
# =====================================

def serial_reader_loop():
    global ser, is_scale_connected, gross_weight, net_weight, tare_weight

    try:
        ser = serial.Serial(port=SERIAL_PORT_PATH, baudrate=BAUDRATE, timeout=1)
    except Exception as err:
        is_scale_connected = False
        log('ERROR', f'Serial Error: {err}')
        return

    is_scale_connected = True
    log('INFO', 'Serial Port Connected')

    # Manual delimiter-based framing (not ser.readline(), which only ever stops at '\n') —
    # some indicators (e.g. Aczet) terminate frames with a non-newline byte like ESC (\x1b).
    buffer = bytearray()
    delimiter_len = len(SERIAL_DELIMITER)

    try:
        while True:
            try:
                byte = ser.read(1)
            except Exception as err:
                is_scale_connected = False
                log('ERROR', f'Serial Error: {err}')
                break

            if not byte:
                continue

            is_scale_connected = True
            buffer += byte

            if not buffer.endswith(SERIAL_DELIMITER):
                continue

            try:
                raw_string = bytes(buffer[:-delimiter_len]).decode(errors='replace').strip()
                buffer.clear()

                if raw_string:
                    fields = [f.strip() for f in raw_string.split('\r')]

                    if len(fields) >= 3:
                        try:
                            gross = float(fields[0])
                            net = float(fields[1])
                            tare = float(fields[2])
                            gross_weight, net_weight, tare_weight = gross, net, tare
                        except ValueError:
                            log('WARN', f'Unparseable frame fields: {raw_string}')
                    else:
                        # Single-value protocol (e.g. Aczet): "<junk><mode letter><spaces><number>".
                        match = SINGLE_VALUE_FRAME_PATTERN.search(raw_string)
                        if match:
                            mode, value = match.group(1).upper(), float(match.group(2))
                            if mode == 'G':
                                gross_weight = value
                                net_weight = gross_weight - tare_weight  # indicator doesn't stream Net separately
                            elif mode == 'N':
                                net_weight = value
                            elif mode == 'T':
                                tare_weight = value
                                net_weight = gross_weight - tare_weight
                            else:
                                log('WARN', f'Unrecognized weight mode "{mode}" in frame: {raw_string}')
                        else:
                            # Fallback: unexpected frame shape, log it so it can be investigated
                            log('WARN', f'Unexpected frame (expected 3 fields): {raw_string}')
            except Exception as err:
                log('ERROR', f'Scale Parse Error: {err}')
    finally:
        is_scale_connected = False
        log('WARN', 'Serial Port Closed')
        if ser and ser.is_open:
            ser.close()


# =====================================
# STREAMING + HEARTBEAT
# =====================================

def publish_stream_sample():
    if not is_scale_connected:  # don't emit stale/garbage readings while the serial link is down
        return

    topic = f'vault/{BRANCH_ID}/{current_vault_number}/{current_shelf_number}/data'
    payload = {
        'weight': net_weight,
        'grossWeight': gross_weight,
        'tareWeight': tare_weight,
        'status': 'SUCCESS',
        'timestamp': iso_now(),
        'type': 'STREAM',  # distinguishes an unprompted sample from a READ response for the backend
    }
    if current_context:
        payload['context'] = current_context  # lets the backend attribute this sample without relying on its own cached context
    if current_packet_id:
        payload['packet_id'] = current_packet_id  # REGISTER_WEIGHT needs this on every sample, not just the initial READ_RESPONSE

    mqtt_client.publish(topic, json.dumps(payload), qos=0)  # fine to drop one — the next sample supersedes it
    log('DEBUG', f'Stream Sample Published → {topic} | Weight={net_weight} | Context={current_context}')


def start_streaming():
    global stream_timer, stream_ceiling_timer

    if stream_timer:  # already streaming — a new READ for the same phase doesn't restart it
        return

    stream_timer = RepeatingTimer(STREAM_INTERVAL_MS / 1000, publish_stream_sample).start()
    stream_ceiling_timer = threading.Timer(STREAM_CEILING_MS / 1000, stop_streaming)  # fallback if STOP_STREAM never arrives
    stream_ceiling_timer.daemon = True
    stream_ceiling_timer.start()
    log('INFO', f'Streaming started (interval={STREAM_INTERVAL_MS}ms, ceiling={STREAM_CEILING_MS}ms)')


def stop_streaming():
    global stream_timer, stream_ceiling_timer, current_context, current_packet_id

    if not stream_timer:
        return

    stream_timer.cancel()
    stream_ceiling_timer.cancel()
    stream_timer = None
    stream_ceiling_timer = None
    current_context = None  # this phase is done — don't let a later restart leak a stale context
    current_packet_id = None  # same — don't let a later restart leak a stale packet_id
    log('INFO', 'Streaming stopped')


def publish_heartbeat():
    if not is_scale_connected or not current_vault_number or not current_shelf_number:  # only emit once we know which shelf to publish to
        return

    topic = f'vault/{BRANCH_ID}/{current_vault_number}/{current_shelf_number}/data'
    payload = {
        'weight': net_weight,
        'grossWeight': gross_weight,
        'tareWeight': tare_weight,
        'status': 'SUCCESS',
        'timestamp': iso_now(),
        'type': 'HEARTBEAT',
    }

    mqtt_client.publish(topic, json.dumps(payload), qos=0)
    log('INFO', f'Heartbeat Published → {topic} | Weight={net_weight}')


# =====================================
# MQTT MESSAGE HANDLER
# =====================================

def on_connect(client, userdata, flags, rc):
    if rc != 0:
        log('ERROR', f'MQTT Connect Error: rc={rc}')
        return

    log('INFO', 'Connected to MQTT Broker')

    cmd_topic = f'vault/{BRANCH_ID}/+/+/cmd'
    result, _ = client.subscribe(cmd_topic)
    if result != mqtt.MQTT_ERR_SUCCESS:
        log('ERROR', f'Subscribe Error: rc={result}')
    else:
        log('INFO', f'Subscribed to: {cmd_topic}')


def on_disconnect(client, userdata, rc):
    if rc != 0:
        log('WARN', 'MQTT Client Offline')
        log('WARN', 'Reconnecting to MQTT Broker...')


def on_message(client, userdata, msg):
    global current_vault_number, current_shelf_number, current_context, current_packet_id

    raw_payload = msg.payload.decode(errors='replace')
    log('DEBUG', f'Raw Message Received | Topic={msg.topic} | Payload={raw_payload}')

    try:
        payload = json.loads(raw_payload)

        if payload.get('action') == 'STOP_STREAM':
            stop_streaming()
            return

        parts = msg.topic.split('/')

        if len(parts) < 5:
            log('WARN', f'Invalid topic format: {msg.topic}')
            return

        branch_id = parts[1]
        vault_number = parts[2]
        shelf_number = parts[3]

        if branch_id != BRANCH_ID:
            log('WARN', f'Ignoring message from branch {branch_id}')
            return

        response_topic = f'vault/{branch_id}/{vault_number}/{shelf_number}/data'

        # ---------- READ ----------
        if payload.get('action') == 'READ':
            # Capture vault/shelf from this READ's topic so heartbeat + stream know where to publish.
            current_vault_number = vault_number
            current_shelf_number = shelf_number
            if payload.get('context'):
                current_context = payload['context']
            if payload.get('packet_id'):
                current_packet_id = payload['packet_id']

            log(
                'INFO',
                f'Tracking shelf: vault={current_vault_number}, shelf={current_shelf_number}, '
                f'context={current_context}, packet_id={current_packet_id}',
            )

            # Start (or continue) streaming for this phase.
            start_streaming()

            status = 'SUCCESS' if is_scale_connected else 'SCALE_OFFLINE'

            log(
                'INFO',
                f'Weight Request | ReqID={payload.get("reqId")} | Gross={gross_weight} | '
                f'Net={net_weight} | Tare={tare_weight} | Status={status}',
            )

            response = {
                'reqId': payload.get('reqId'),
                'weight': net_weight,
                'grossWeight': gross_weight,
                'tareWeight': tare_weight,
                'status': status,
                'timestamp': iso_now(),
                'type': 'READ_RESPONSE',  # distinguishes a one-shot response from ambient STREAM/HEARTBEAT samples
            }

            if payload.get('packet_id'):
                response['packet_id'] = payload['packet_id']

            if payload.get('context'):
                response['context'] = payload['context']

            info = client.publish(response_topic, json.dumps(response), qos=1)
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                log('INFO', f'Response Published → {response_topic}')
            else:
                log('ERROR', f'Publish Error: rc={info.rc}')

            return

        # ---------- TARE ----------
        if payload.get('action') == 'TARE':
            try:
                if ser and ser.is_open:
                    ser.write(b'T')
                log('INFO', f'Tare Command Sent to Scale | ReqID={payload.get("reqId")}')
            except Exception as err:
                log('ERROR', f'Serial Write Error (TARE): {err}')

            response = {
                'reqId': payload.get('reqId'),
                'action': 'TARE',
                'grossWeight': gross_weight,
                'tareWeight': tare_weight,
                'weight': net_weight,
                'status': 'SUCCESS' if is_scale_connected else 'SCALE_OFFLINE',
                'note': 'Tare command sent to scale; reflects on next frame if supported by indicator firmware.',
                'timestamp': iso_now(),
            }

            if payload.get('packet_id'):
                response['packet_id'] = payload['packet_id']

            if payload.get('context'):
                response['context'] = payload['context']

            info = client.publish(response_topic, json.dumps(response), qos=1)
            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                log('INFO', f'Tare Response Published → {response_topic}')
            else:
                log('ERROR', f'Publish Error: rc={info.rc}')

            return
    except Exception as err:
        log('ERROR', f'Message Processing Error: {err}')


# =====================================
# HEALTH LOG EVERY 5 MINUTES
# =====================================

def health_log():
    log(
        'HEALTH',
        f'Gross={gross_weight} | Net={net_weight} | Tare={tare_weight} | '
        f'Scale={"CONNECTED" if is_scale_connected else "DISCONNECTED"}',
    )


# =====================================
# MQTT SETUP
# =====================================

def build_mqtt_client():
    parsed = urlparse(MQTT_BROKER)
    scheme = parsed.scheme or 'mqtt'
    host = parsed.hostname
    port = parsed.port or (8883 if scheme in ('mqtts', 'ssl', 'wss') else 1883)

    client = mqtt.Client(clean_session=True)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if scheme in ('mqtts', 'ssl', 'wss'):
        client.tls_set()

    client.reconnect_delay_set(min_delay=5, max_delay=5)  # mirrors reconnectPeriod: 5000
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    return client, host, port


def main():
    global mqtt_client

    threading.Thread(target=serial_reader_loop, daemon=True).start()

    log('INFO', f'Connecting to MQTT Broker: {MQTT_BROKER}')
    mqtt_client, host, port = build_mqtt_client()
    mqtt_client.connect_async(host, port, keepalive=60)

    # Publish a heartbeat every 60 s so the backend can distinguish a slow recon from a dead link.
    # If the full 1–2 s stream is also running, the heartbeat is redundant but harmless (it will
    # still fire, it's just unnecessary — the stream messages already satisfy the liveness check).
    RepeatingTimer(HEARTBEAT_INTERVAL_MS / 1000, publish_heartbeat).start()

    RepeatingTimer(300, health_log).start()

    mqtt_client.loop_forever()


if __name__ == '__main__':
    main()

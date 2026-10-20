
# använder asyncua 2.1.0

import asyncio
import copy
import logging
import threading

from datetime import datetime, timezone

from flask import Flask, jsonify
from asyncua import Client


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# --------------------------------------------------
# Flask REST API
# --------------------------------------------------

app = Flask(__name__)


# --------------------------------------------------
# OPC UA servers
# --------------------------------------------------

SERVER_URLS = {
    "server1": "opc.tcp://SERVER1:4840",
    "server2": "opc.tcp://SERVER2:4840",
    "server3": "opc.tcp://SERVER3:4840",
    "server4": "opc.tcp://SERVER4:4840",
    "server5": "opc.tcp://SERVER5:4840",
    "server6": "opc.tcp://SERVER6:4840",
}


# Local name: actual OPC UA NodeId
NODES = {
    "DeviceId": "ns=2;s=Device.DeviceId",
    "Temperature": "ns=2;s=Device.Value",
    "Humidity": "ns=2;s=Device.Unit",
}


SUBSCRIPTION_INTERVAL_MS = 500
RECONNECT_MAX_DELAY_SECONDS = 10.0
INITIAL_RETRY_DELAY_SECONDS = 5


# --------------------------------------------------
# Shared data
# --------------------------------------------------

latest_values = {}
values_lock = threading.Lock()


def utc_now():
     return datetime.now().strftime("%Y-%m-%d %H:%M:%S");
    # tid format blir t.ex: "2026-10-20 17:41:23"


def create_server_data(server_url):
    return {
        "url": server_url,
        "connected": False,
        "last_update": None,
        "Data": {},
        }


def initialize_server(server_name, server_url):
    with values_lock:
        if server_name not in latest_values:
            latest_values[server_name] = create_server_data(
                server_url
            )


def set_connection_status(server_name, connected):
    with values_lock:
        latest_values[server_name]["connected"] = connected


def update_node_value(server_name, node_name, value):
    with values_lock:
        server_data = latest_values[server_name]

        server_data["Data"][node_name] = value
       

# --------------------------------------------------
# Subscription handler
# --------------------------------------------------

class SubscriptionHandler:

    def __init__(self, server_name, node_names):
        self.server_name = server_name
        self.node_names = node_names

    def datachange_notification(self, node, value, data):
        node_id = node.nodeid.to_string()
        node_name = self.node_names.get(node_id, node_id)

        update_node_value(
            self.server_name,
            node_name,
            value,
        )

        set_connection_status(self.server_name, True)

        log.info(
            "%s: %s = %r",
            self.server_name,
            node_name,
            value,
        )

    def status_change_notification(self, status):
        log.warning(
            "%s: subscription status changed: %s",
            self.server_name,
            status,
        )


# --------------------------------------------------
# OPC UA client for one server
# --------------------------------------------------

async def read_server(server_name, server_url):

    initialize_server(server_name, server_url)

    while True:

        client = Client(url=server_url)

        # Callback when the connection is lost.
        # Keep this callback synchronous.
        def on_connection_lost(exc):
            log.warning(
                "%s: connection lost: %s",
                server_name,
                exc,
            )
            set_connection_status(server_name, False)

        client.connection_lost_callback = on_connection_lost

        # --------------------------------------------------
        # 1. INITIAL CONNECTION
        # --------------------------------------------------

        try:
            log.info(
                "%s: connecting to %s",
                server_name,
                server_url,
            )

            await client.connect(
                auto_reconnect=True,
                reconnect_max_delay=RECONNECT_MAX_DELAY_SECONDS,
            )

            set_connection_status(server_name, True)

            log.info(
                "%s: connected",
                server_name,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            # The initial connection failed.
            # Clean up this client before creating another one.
            log.exception(
                "%s: initial connection failed",
                server_name,
            )

            set_connection_status(server_name, False)

            try:
                await client.disconnect()
            except Exception:
                pass

            log.info(
                "%s: retrying initial connection in %s seconds",
                server_name,
                INITIAL_RETRY_DELAY_SECONDS,
            )

            await asyncio.sleep(INITIAL_RETRY_DELAY_SECONDS)
            continue

        # --------------------------------------------------
        # 2. CONNECTION ESTABLISHED
        # --------------------------------------------------
        # Keep this client alive during ordinary connection
        # interruptions. asyncua handles automatic reconnect.
        #
        # Do not disconnect and create a new client merely
        # because the connection is temporarily interrupted.

        try:

            nodes = {
                name: client.get_node(node_id)
                for name, node_id in NODES.items()
            }

            # --------------------------------------------------
            # 3. READ INITIAL VALUES
            # --------------------------------------------------

            log.info(
                "%s: reading initial node values",
                server_name,
            )

            for node_name, node in nodes.items():

                try:
                    value = await node.read_value()

                    update_node_value(
                        server_name,
                        node_name,
                        value,
                    )

                    log.info(
                        "%s: initial %s = %r",
                        server_name,
                        node_name,
                        value,
                    )

                except asyncio.CancelledError:
                    raise

                except Exception as exc:
                    log.exception(
                        "%s: failed to read initial value for %s",
                        server_name,
                        node_name,
                    )

                   
            node_names = {
                node.nodeid.to_string(): name
                for name, node in nodes.items()
            }

            handler = SubscriptionHandler(
                server_name,
                node_names,
            )

            # --------------------------------------------------
            # 4. CREATE SUBSCRIPTION
            # --------------------------------------------------
            # Retry subscription creation with the SAME client
            # if setup fails. Do not disconnect on ordinary
            # connection interruptions handled by asyncua.

            while True:

                try:
                    async with await client.create_subscription(
                        SUBSCRIPTION_INTERVAL_MS,
                        handler,
                    ) as subscription:

                        await subscription.subscribe_data_change(
                            list(nodes.values())
                        )

                        log.info(
                            "%s: subscription active",
                            server_name,
                        )

                        # Keep the subscription context alive.
                        # asyncua handles reconnecting and restoring
                        # subscriptions for supported scenarios.
                        await asyncio.Event().wait()

                except asyncio.CancelledError:
                    raise

                except Exception:
                    log.exception(
                        "%s: subscription setup failed; "
                        "retrying with the same client",
                        server_name,
                    )

                    # Retry setup without disconnecting the client.
                    await asyncio.sleep(
                        INITIAL_RETRY_DELAY_SECONDS
                    )

        except asyncio.CancelledError:
            # Explicit program shutdown only.
            raise

        except Exception:
            # An unexpected error occurred after connecting.
            # Clean up and start a fresh connection cycle.
            log.exception(
                "%s: unexpected client error",
                server_name,
            )

            set_connection_status(server_name, False)

        finally:
            # This block is reached when the active client
            # task exits, not for ordinary reconnects handled
            # internally by asyncua.
            try:
                await client.disconnect()
            except Exception:
                pass

        # Retry if an unexpected error ended the active client.
        await asyncio.sleep(INITIAL_RETRY_DELAY_SECONDS)


# --------------------------------------------------
# Start all OPC UA servers concurrently
# --------------------------------------------------

async def read_all_servers():
    await asyncio.gather(
        *[
            read_server(name, url)
            for name, url in SERVER_URLS.items()
        ]
    )


def run_opcua_background():
    asyncio.run(read_all_servers())


# --------------------------------------------------
# REST GET: all servers
# --------------------------------------------------

@app.route("/api/servers", methods=["GET"])
def get_all_servers():

    with values_lock:
        snapshot = copy.deepcopy(latest_values)

    return jsonify(snapshot)


# --------------------------------------------------
# REST GET: one server
# --------------------------------------------------

@app.route("/api/servers/<server_name>", methods=["GET"])
def get_one_server(server_name):

    with values_lock:
        server_data = latest_values.get(server_name)

        if server_data is None:
            return jsonify({
                "error": "Server not found",
                "server": server_name,
            }), 404

        snapshot = copy.deepcopy(server_data)

    return jsonify(snapshot)


# --------------------------------------------------
# Home page
# --------------------------------------------------

@app.route("/", methods=["GET"])
def home():
    return (
        "Flask OPC UA API is running. "
        "Use GET /api/servers to retrieve all servers."
    )


# --------------------------------------------------
# Application startup
# --------------------------------------------------

if __name__ == "__main__":

    opcua_thread = threading.Thread(
        target=run_opcua_background,
        daemon=True,
    )

    opcua_thread.start()

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        use_reloader=False,
        threaded=True,
    )



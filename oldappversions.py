
# #App Version 3

# import asyncio
# import copy
# import logging
# import threading

# from datetime import datetime, timezone

# from flask import Flask, jsonify
# from asyncua import Client

# # --------------------------------------------------
# # Logging
# # --------------------------------------------------

# logging.basicConfig(level=logging.INFO)
# log = logging.getLogger(__name__)

# # --------------------------------------------------
# # Flask REST API
# # --------------------------------------------------

# app = Flask(__name__)

# # --------------------------------------------------
# # OPC UA-servrar
# # --------------------------------------------------

# SERVER_URLS = {
#     "server1": "opc.tcp://SERVER1:4840",
#     "server2": "opc.tcp://SERVER2:4840",
#     "server3": "opc.tcp://SERVER3:4840",
#     "server4": "opc.tcp://SERVER4:4840",
#     "server5": "opc.tcp://SERVER5:4840",
#     "server6": "opc.tcp://SERVER6:4840",
# }

# # Lokalt nodnamn: OPC UA NodeId.
# NODES = {
#     "DeviceId": "ns=2;s=Device.DeviceId",
#     "Temperature": "ns=2;s=Device.Value",
#     "Humidity": "ns=2;s=Device.Unit",
# }

# SUBSCRIPTION_INTERVAL_MS = 500
# RECONNECT_MAX_DELAY_SECONDS = 10.0
# INITIAL_RETRY_DELAY_SECONDS = 5

# # --------------------------------------------------
# # Delad lagring
# # --------------------------------------------------

# latest_values = {}
# values_lock = threading.Lock()


# def utc_now():
#     return datetime.now(timezone.utc).isoformat()


# def create_server_data(server_url):
#     return {
#         "url": server_url,
#         "connected": False,
#         "last_update": None,
#         "Data": {},
#         "Errors": {},
#     }


# def initialize_server(server_name, server_url):
#     with values_lock:
#         if server_name not in latest_values:
#             latest_values[server_name] = create_server_data(
#                 server_url
#             )


# def set_connection_status(server_name, connected):
#     with values_lock:
#         latest_values[server_name]["connected"] = connected


# def update_node_value(server_name, node_name, value):
#     with values_lock:
#         server_data = latest_values[server_name]

#         server_data["Data"][node_name] = value
#         server_data["last_update"] = utc_now()
#         server_data["Errors"].pop(node_name, None)


# def set_node_error(server_name, node_name, error):
#     with values_lock:
#         latest_values[server_name]["Errors"][node_name] = str(
#             error
#         )


# # --------------------------------------------------
# # OPC UA callback
# # --------------------------------------------------

# class SubscriptionHandler:
#     def __init__(self, server_name, node_names):
#         self.server_name = server_name
#         self.node_names = node_names

#     def datachange_notification(self, node, value, data):
#         node_id = node.nodeid.to_string()
#         node_name = self.node_names.get(node_id, node_id)

#         update_node_value(
#             self.server_name,
#             node_name,
#             value,
#         )

#         set_connection_status(self.server_name, True)

#         log.info(
#             "%s: %s = %r",
#             self.server_name,
#             node_name,
#             value,
#         )

#     def status_change_notification(self, status):
#         log.warning(
#             "%s: subscription status changed: %s",
#             self.server_name,
#             status,
#         )


# # --------------------------------------------------
# # OPC UA-klient för en server
# # --------------------------------------------------

# async def read_server(server_name, server_url):
#     initialize_server(server_name, server_url)

#     while True:
#         client = Client(url=server_url)

#         async def on_connection_lost(exc):
#             log.warning(
#                 "%s: anslutningen förlorades: %s",
#                 server_name,
#                 exc,
#             )
#             set_connection_status(server_name, False)

#         client.connection_lost_callback = on_connection_lost

#         # --------------------------------------------------
#         # 1. FÖRSÖK FÖRSTA ANSLUTNINGEN
#         # --------------------------------------------------

#         try:
#             log.info(
#                 "%s: ansluter till %s",
#                 server_name,
#                 server_url,
#             )

#             await client.connect(
#                 auto_reconnect=True,
#                 reconnect_max_delay=RECONNECT_MAX_DELAY_SECONDS,
#             )

#             set_connection_status(server_name, True)

#             log.info("%s: ansluten", server_name)

#         except asyncio.CancelledError:
#             raise

#         except Exception:
#             # Den första anslutningen misslyckades.
#             # Ingen etablerad anslutning behöver bevaras.
#             log.exception(
#                 "%s: första anslutningsförsöket misslyckades",
#                 server_name,
#             )

#             set_connection_status(server_name, False)

#             try:
#                 await client.disconnect()
#             except Exception:
#                 pass

#             log.info(
#                 "%s: nytt anslutningsförsök om %s sekunder",
#                 server_name,
#                 INITIAL_RETRY_DELAY_SECONDS,
#             )

#             await asyncio.sleep(INITIAL_RETRY_DELAY_SECONDS)
#             continue

#         # --------------------------------------------------
#         # 2. ANSLUTNINGEN ÄR ETABLERAD
#         # --------------------------------------------------
#         # Behåll samma klient under vanliga avbrott.
#         # asyncua sköter automatisk återanslutning.
#         # Anropa INTE disconnect() vid vanliga anslutningsavbrott.

#         while True:
#             try:
#                 nodes = {
#                     name: client.get_node(node_id)
#                     for name, node_id in NODES.items()
#                 }

#                 # --------------------------------------------------
#                 # 3. LÄS INITIALA VÄRDEN
#                 # --------------------------------------------------

#                 log.info(
#                     "%s: läser initiala nodvärden",
#                     server_name,
#                 )

#                 for node_name, node in nodes.items():
#                     try:
#                         value = await node.read_value()

#                         update_node_value(
#                             server_name,
#                             node_name,
#                             value,
#                         )

#                         log.info(
#                             "%s: initialt %s = %r",
#                             server_name,
#                             node_name,
#                             value,
#                         )

#                     except asyncio.CancelledError:
#                         raise

#                     except Exception as exc:
#                         log.exception(
#                             "%s: kunde inte läsa noden %s",
#                             server_name,
#                             node_name,
#                         )

#                         set_node_error(
#                             server_name,
#                             node_name,
#                             exc,
#                         )

#                 # --------------------------------------------------
#                 # 4. SKAPA PRENUMERATION
#                 # --------------------------------------------------

#                 node_names = {
#                     node.nodeid.to_string(): name
#                     for name, node in nodes.items()
#                 }

#                 handler = SubscriptionHandler(
#                     server_name,
#                     node_names,
#                 )

#                 async with await client.create_subscription(
#                     SUBSCRIPTION_INTERVAL_MS,
#                     handler,
#                 ) as subscription:

#                     await subscription.subscribe_data_change(
#                         list(nodes.values())
#                     )

#                     log.info(
#                         "%s: prenumeration aktiv",
#                         server_name,
#                     )

#                     # Vänta medan asyncua övervakar anslutningen
#                     # och återställer aktiva prenumerationer.
#                     await asyncio.Event().wait()

#             except asyncio.CancelledError:
#                 # Vid avsiktlig avstängning ska klienten stängas.
#                 try:
#                     await client.disconnect()
#                 except Exception:
#                     pass
#                 raise

#             except Exception:
#                 # Ett fel vid läsning eller skapande av
#                 # prenumerationen. Behåll klienten och låt
#                 # asyncua hantera eventuell återanslutning.
#                 log.exception(
#                     "%s: läsning eller prenumeration misslyckades; "
#                     "försöker igen med samma klient",
#                     server_name,
#                 )

#                 await asyncio.sleep(INITIAL_RETRY_DELAY_SECONDS)


# # --------------------------------------------------
# # Starta alla OPC UA-servrar parallellt
# # --------------------------------------------------

# async def read_all_servers():
#     await asyncio.gather(
#         *[
#             read_server(name, url)
#             for name, url in SERVER_URLS.items()
#         ]
#     )


# def run_opcua_background():
#     asyncio.run(read_all_servers())


# # --------------------------------------------------
# # REST GET: alla servrar
# # --------------------------------------------------

# @app.route("/api/servers", methods=["GET"])
# def get_all_servers():
#     with values_lock:
#         snapshot = copy.deepcopy(latest_values)

#     return jsonify(snapshot)


# # --------------------------------------------------
# # REST GET: en specifik server
# # --------------------------------------------------

# @app.route("/api/servers/<server_name>", methods=["GET"])
# def get_one_server(server_name):
#     with values_lock:
#         server_data = latest_values.get(server_name)

#         if server_data is None:
#             return jsonify({
#                 "error": "Servern finns inte",
#                 "server": server_name,
#             }), 404

#         snapshot = copy.deepcopy(server_data)

#     return jsonify(snapshot)


# # --------------------------------------------------
# # Startsida
# # --------------------------------------------------

# @app.route("/", methods=["GET"])
# def home():
#     return (
#         "Flask OPC UA API är igång. "
#         "Använd GET /api/servers för alla servrar."
#     )


# # --------------------------------------------------
# # Programstart
# # --------------------------------------------------

# if __name__ == "__main__":
#     opcua_thread = threading.Thread(
#         target=run_opcua_background,
#         daemon=True,
#     )
#     opcua_thread.start()

#     app.run(
#         host="127.0.0.1",
#         port=5000,
#         debug=False,
#         use_reloader=False,
#         threaded=True,
#     )


# #App Version 2 
# import asyncio
# import logging
# import threading

# from flask import Flask, jsonify
# from asyncua import Client

# log = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

# app = Flask(__name__)

# SERVER_URLS = {
#     "server1": "opc.tcp://SERVER1:4840",
#     "server2": "opc.tcp://SERVER2:4840",
#     "server3": "opc.tcp://SERVER3:4840",
#     "server4": "opc.tcp://SERVER4:4840",
#     "server5": "opc.tcp://SERVER5:4840",
#     "server6": "opc.tcp://SERVER6:4840",
# }

# # Nyckeln är vårt lokala namn för värdet.
# # Värdet är nodens faktiska OPC UA NodeId.
# NODES = {
#     "DeviceId": "ns=2;s=Device.DeviceId",
#     "Temperature": "ns=2;s=Device.Value",
#     "Humidity": "ns=2;s=Device.Unit",
# }

# # Senaste data från varje server.
# latest_values = {}
# values_lock = threading.Lock()


# class SubscriptionHandler:
#     def __init__(self, server_name, node_names):
#         self.server_name = server_name
#         self.node_names = node_names

#     def datachange_notification(self, node, value, data):
#         node_id = node.nodeid.to_string()
#         node_name = self.node_names.get(node_id, node_id)

#         with values_lock:
#             server_data = latest_values.setdefault(
#                 self.server_name,
#                 {
#                     "url": SERVER_URLS[self.server_name],
#                     "connected": True,
#                     "Data": {},
#                 },
#             )

#             server_data["connected"] = True
#             server_data["Data"][node_name] = value
#         log.info(
#             "%s: %s = %r",
#             self.server_name,
#             node_name,
#             value,
#         )


# async def read_server(server_name, server_url):
#     # Återförsök görs separat för varje server.
#     while True:
#         client = Client(url=server_url)

#         try:
#             with values_lock:
#                 latest_values.setdefault(
#                     server_name,
#                     {
#                         "url": server_url,
#                         "connected": False,
#                         "nodes": {},
#                     },
#                 )

#             log.info("%s: ansluter", server_name)

#             await client.connect(
#                 auto_reconnect=True,
#                 reconnect_max_delay=10.0,
#             )

#             node_names = {
#                 node_id: name
#                 for name, node_id in NODES.items()
#             }

#             handler = SubscriptionHandler(
#                 server_name,
#                 node_names,
#             )

#             nodes = [
#                 client.get_node(node_id)
#                 for node_id in NODES.values()
#             ]

#             async with await client.create_subscription(
#                 500, handler
#             ) as subscription:

#                 await subscription.subscribe_data_change(nodes)

#                 with values_lock:
#                     latest_values[server_name]["connected"] = True

#                 log.info("%s: subscription aktiv", server_name)

#                 # Håll subscriptionen vid liv.
#                 # asyncua hanterar återanslutning vid anslutningsavbrott.
#                 await asyncio.Event().wait()

#         except asyncio.CancelledError:
#             raise

#         except Exception:
#             log.exception(
#                 "%s: anslutning eller subscription misslyckades",
#                 server_name,
#             )

#             with values_lock:
#                 latest_values.setdefault(
#                     server_name,
#                     {"url": server_url, "nodes": {}},
#                 )
#                 latest_values[server_name]["connected"] = False

#         finally:
#             try:
#                 await client.disconnect()
#             except Exception:
#                 pass

#         # Om anslutningen inte kunde upprättas,
#         # eller om funktionen avslutades på grund av ett fel.
#         await asyncio.sleep(5)


# async def read_all_servers():
#     await asyncio.gather(
#         *[
#             read_server(name, url)
#             for name, url in SERVER_URLS.items()
#         ]
#     )


# def run_opcua_background():
#     asyncio.run(read_all_servers())


# @app.route("/api/servers", methods=["GET"])
# def get_all_servers():
#     with values_lock:
#         return jsonify(latest_values.copy())
        

    


# @app.route("/")
# def home():
#     return "Flask OPC UA - alla servrar"


# if __name__ == "__main__":
#     threading.Thread(
#         target=run_opcua_background,
#         daemon=True,
#     ).start()

#     app.run(
#         host="127.0.0.1",
#         port=5000,
#         debug=False,
#         use_reloader=False,
#     )


# version 1 
# import asyncio
# import logging
# from flask import Flask, jsonify
# from asyncua import Client
# from asyncua.common.subscription import DataChangeEvent

# log = logging.getLogger(_name_)

# app = Flask(__name__)

# # Ersätt med de faktiska adresserna till era sex OPC UA-servrar.
# SERVER_URLS = {
#     "server1": "opc.tcp://SERVER1:4840",
#     "server2": "opc.tcp://SERVER2:4840",
#     "server3": "opc.tcp://SERVER3:4840",
#     "server4": "opc.tcp://SERVER4:4840",
#     "server5": "opc.tcp://SERVER5:4840",
#     "server6": "opc.tcp://SERVER6:4840",
# }

# NODE_NAMES = [
#     "Device.DeviceId",
#     "Device.Value",
#     "Device.Unit",
# ]

# NODES = {
#         "DeviceId": "ns=2;s=Device.DeviceId",
#         "Value": "ns=2;s=Device.Value",
#         "Unit": "ns=2;s=Device.Unit",
#     }

# # to do nodeId ??? ns i vad är det? behövs det verkligen ? 
# async def read_server(server_name, server_url):
#     result = {
#         "server": server_name,
#         "url": server_url,
#         "nodes": {},
#     }

#     try:
#          #async with Client(url=server_url) as client:
#          client = Client(url = server_url)

#          await client.connect(auto_reconnect=True)
#             node_deviceId = client.get_node("ns=2;s=Device.DeviceId");
#             value = await node_deviceId.read_value();
            
#             # rätt cast till float görs här automatiskt av asyncua då python inte har double
#             node_measurement = client.get_node("ns=2;s=Device.Value");
#             value = await node_measurement.read_value();

#             node_unit = client.get_node("ns=2;s=Device.Unit");
#             unit = await node_deviceId.read_value();
            
#     except Exception as e:
#         print(f"Fel vid läsning av OPC UA-nod: {e}")
    
#     return result


# async def read_all_servers():
#     tasks = [
#         read_server(name, url)
#         for name, url in SERVER_URLS.items()
#     ]

#     return await asyncio.gather(*tasks)


# @app.route("/api/servers", methods=["GET"])
# def get_all_servers():
#     try:
#         results = asyncio.run(read_all_servers())
#         return jsonify(results)
#     except Exception as error:
#         app.logger.exception("Fel vid läsning av OPC UA-servrar")
#         return jsonify({"error": str(error)}), 500


# @app.route("/")
# def home():
#     return "Flask OPC UA - alla servrar"


# if __name__ == "__main__":
#     app.run(host="127.0.0.1", port=5000, debug=False)

import sys
import time
from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient

SERVER = "rtserve.iris.washington.edu:18000"
STATIONS = [
    ("NM", "NMDM"),  # New Madrid, MO
    ("II", "BORG"),  # Borgarfjordur, Iceland
    ("IU", "ANMO"),  # Albuquerque, NM
    ("IU", "AFI"),   # Afiamalu, Samoa
    ("IU", "MAJO"),  # Matsushiro, Japan
    ("IU", "COLA"),  # College, Alaska
]

packets_seen = []

def on_data(trace):
    packets_seen.append(trace)
    samples = trace.data
    print(f"[{trace.stats.network}.{trace.stats.station}.{trace.stats.channel}] "
          f"{len(samples)} samples, first 5: {samples[:5].tolist()}, "
          f"last-bit parity of first 10: {[int(s) & 1 for s in samples[:10]]}",
          flush=True)

client = EasySeedLinkClient(SERVER, autoconnect=False)
client.on_data = on_data
client.conn.timeout = 30  # obspy bug: defaults to None, which breaks its own is_connected() check
client.connect()
for net, sta in STATIONS:
    try:
        client.select_stream(net, sta, "BHZ")
        print(f"selected {net}.{sta}.BHZ", flush=True)
    except Exception as e:
        print(f"FAILED to select {net}.{sta}.BHZ: {e}", flush=True)

print("connecting + running for 25s...", flush=True)
import threading
t = threading.Thread(target=client.run, daemon=True)
t.start()
time.sleep(25)
seen_stations = sorted(set(f"{tr.stats.network}.{tr.stats.station}" for tr in packets_seen))
print(f"DONE. total packets: {len(packets_seen)}, stations heard from: {seen_stations}", flush=True)
sys.exit(0)

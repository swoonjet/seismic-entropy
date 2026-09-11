"""Live multi-station ingestion from IRIS/EarthScope SeedLink.

Wraps obspy's EasySeedLinkClient (one connection carries all subscribed
stations/channels multiplexed together -- SeedLink is designed for this).
Two things obspy's convenience wrapper gets wrong for a long-running
service, worked around here:

  1. `EasySeedLinkClient.connect()` crashes outright if `conn.timeout` is
     left at its default of None (obspy compares it to a float internally).
     Set an explicit timeout before connecting.
  2. `client.run()` returns (thread exits) on any connection drop -- there
     is no built-in reconnect. A seismic-data feed that quietly goes dead
     defeats the entire point of this project, so this wraps `run()` in a
     supervising loop that reconnects with backoff.
"""

import logging
import threading
import time

from obspy.clients.seedlink.easyseedlink import EasySeedLinkClient

log = logging.getLogger("seedlink_source")

SERVER = "rtserve.iris.washington.edu:18000"

# (network, station, channel, human label) -- all BHZ, all verified live
# against the real IRIS ring server (not assumed from FDSN metadata alone).
# Only one US station (College, Alaska) by design -- the rest is deliberately
# spread across as many distinct parts of the world as are actually live.
STATIONS = [
    ("II", "BORG", "BHZ", "Borgarfjörður, Iceland"),
    ("IU", "AFI", "BHZ", "Afiamalu, Samoa"),
    ("IU", "MAJO", "BHZ", "Matsushiro, Japan"),
    ("IU", "COLA", "BHZ", "College, Alaska"),
    ("IU", "PMSA", "BHZ", "Palmer Station, Antarctica"),
    ("IU", "SNZO", "BHZ", "South Karori, New Zealand"),
    ("IU", "PAYG", "BHZ", "Galápagos, Ecuador"),
    ("IU", "KONO", "BHZ", "Kongsberg, Norway"),
    ("IU", "TSUM", "BHZ", "Tsumeb, Namibia"),
]


class SeismicFeed:
    """Runs the SeedLink connection on a background thread and calls
    `on_samples(station_id, station_label, samples)` for every packet."""

    def __init__(self, on_samples, stations=STATIONS, server=SERVER):
        self.on_samples = on_samples
        self.stations = stations
        self.server = server
        self._labels = {f"{n}.{s}": label for n, s, _ch, label in stations}
        self._stop = threading.Event()
        self._thread = None
        self.packets_received = 0
        self.last_packet_ts = None
        self.connected = False

    def start(self):
        self._thread = threading.Thread(target=self._supervise, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _handle_trace(self, trace):
        station_id = f"{trace.stats.network}.{trace.stats.station}"
        label = self._labels.get(station_id, station_id)
        self.packets_received += 1
        self.last_packet_ts = time.time()
        self.on_samples(station_id, label, trace.data.tolist())

    def _connect_once(self):
        client = EasySeedLinkClient(self.server, autoconnect=False)
        client.on_data = self._handle_trace
        client.conn.timeout = 30
        client.connect()
        for net, sta, chan, _label in self.stations:
            try:
                client.select_stream(net, sta, chan)
            except Exception:
                log.exception("failed to select %s.%s.%s", net, sta, chan)
        self.connected = True
        try:
            client.run()  # blocks until the connection drops
        finally:
            self.connected = False

    def _supervise(self):
        backoff = 2
        while not self._stop.is_set():
            try:
                log.info("connecting to %s", self.server)
                self._connect_once()
                backoff = 2  # reset after any period of successful streaming
            except Exception:
                log.exception("seedlink connection failed")
            if self._stop.is_set():
                return
            log.warning("seedlink connection ended, reconnecting in %ss", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

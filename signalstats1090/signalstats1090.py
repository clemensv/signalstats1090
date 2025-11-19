# File: main.py
# Purpose: Provide a FastAPI-based webapp that reads dump1090 BEAST messages,
#          computes message rate statistics (msg/sec at 5s, 15s, 30s, 60s, 300s intervals),
#          computes signal strength statistics (min, max, average over 30s),
#          and serves two live line charts via WebSocket.
#
# In this version, the WebSocket endpoint is guaranteed to be called
# once the frontend attempts to connect. To confirm, we log when a new
# client connects or disconnects, and we send a "ping" from the client.

import argparse
import asyncio
import contextlib
import json
import logging
import math
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid
import datetime
from collections import defaultdict, deque
from math import asin, atan2, cos, degrees, radians, sin, sqrt
from typing import AsyncGenerator, Deque, Tuple

import pyModeS as pms
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pyModeS.extra.tcpclient import TcpClient
import psutil

proc = psutil.Process()

# Global deque for timestamps within the last 300s
message_timestamps: Deque[float] = deque()

# Global deques for other rolling windows
SIGNAL_LEVELS_30S: Deque[Tuple[float, float]] = deque()
DISTANCES_30S: Deque[Tuple[float, float]] = deque()
MIN_RSSI_TIMESTAMPS: Deque[Tuple[float, int, float]] = deque()
DISTANCE_RSSI_RATIO_30S: Deque[Tuple[float, float, float, float]] = deque()  # (timestamp, ratio, lat, lon)

ref_lat: float = 0
ref_lon: float = 0

dump1090_host: str = "localhost"
dump1090_port: int = 30005

# Global set of connected WebSockets for broadcast.
CONNECTED_WEBSOCKETS = set()

# Synchronization lock to protect shared data.
DATA_LOCK = threading.Lock()

BEARING_SEGMENTS = 16
RING_DISTANCE = 80
MAX_DISTANCE = 480
BEARING_LABELS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE",
                  "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
COVERAGE_60S = defaultdict(int)
MIN_RSSI_BY_BEARING = [0] * BEARING_SEGMENTS
MIN_RATIO_BY_BEARING = [float(0)] * BEARING_SEGMENTS
MAX_RATIO_BY_BEARING = [float(0)] * BEARING_SEGMENTS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LONG_TERM_MSG_RATES: Deque[float] = deque()
LONG_TERM_MSG_TIMESTAMPS: Deque[float] = deque()

# Cache for the last 120 minutes of long-term message rates
LONG_TERM_CACHE: Deque[dict] = deque(maxlen=120)

# Global variable to store the maximum message rate observed
MAX_MESSAGE_RATE = 0.0


def get_current_time() -> float:
    """
    Returns the current time in seconds.
    """
    return time.time()


def update_signal_sliding_window(now: float) -> None:
    """
    Removes signal-level entries older than 30 seconds.
    """
    while SIGNAL_LEVELS_30S and SIGNAL_LEVELS_30S[0][0] < now - 30:
        SIGNAL_LEVELS_30S.popleft()


def update_distance_sliding_window(now: float) -> None:
    """Removes distance entries older than 30s."""
    while DISTANCES_30S and DISTANCES_30S[0][0] < now - 30:
        DISTANCES_30S.popleft()


def update_coverage_sliding_window(now: float) -> None:
    """Removes coverage entries older than 60s."""
    keys_to_remove = [key for key,
                      (ts, _) in COVERAGE_60S.items() if ts < now - 60]
    for key in keys_to_remove:
        del COVERAGE_60S[key]


def update_min_rssi_by_bearing(bearing: int, rssi: float, now: float) -> None:
    """
    Updates the minimum RSSI for a given bearing segment and records the timestamp.
    """
    MIN_RSSI_TIMESTAMPS.append((now, bearing, rssi))
    if rssi < MIN_RSSI_BY_BEARING[bearing]:
        MIN_RSSI_BY_BEARING[bearing] = rssi


def expire_min_rssi(now: float) -> None:
    """
    Expires minimum RSSI values older than 30 seconds.
    """
    while MIN_RSSI_TIMESTAMPS and MIN_RSSI_TIMESTAMPS[0][0] < now - 30:
        _, bearing, rssi = MIN_RSSI_TIMESTAMPS.popleft()
        if MIN_RSSI_BY_BEARING[bearing] == rssi:
            # Recompute the minimum RSSI for this bearing
            # Create a snapshot to avoid "deque mutated during iteration" error
            MIN_RSSI_BY_BEARING[bearing] = min(
                (r for t, b, r in list(MIN_RSSI_TIMESTAMPS) if b == bearing),
                default=float(0)
            )


def expire_distance_rssi_ratio(now: float) -> None:
    while DISTANCE_RSSI_RATIO_30S and DISTANCE_RSSI_RATIO_30S[0][0] < now - 30:
        DISTANCE_RSSI_RATIO_30S.popleft()


def compute_message_rates() -> Tuple[float, float, float, float, float]:
    """
    Computes average message rates for 5s, 15s, 30s, 60s, and 300s windows
    """

    global message_timestamps

    now = get_current_time()
    new_timestamps = Deque[float]()

    count_5s = 0
    count_15s = 0
    count_30s = 0
    count_60s = 0
    count_300s = 0

    # Single pass to count how many timestamps fall into each age window
    for t in reversed(message_timestamps):
        age = now - t
        if age <= 5:
            count_5s += 1
        if age <= 15:
            count_15s += 1
        if age <= 30:
            count_30s += 1
        if age <= 60:
            count_60s += 1
        if age <= 300:
            count_300s += 1
            new_timestamps.appendleft(t)
  
    # Reset timestamps to the new list of valid ones
    message_timestamps.clear()
    message_timestamps.extend(new_timestamps)

    rate_5s = count_5s / 5.0
    rate_15s = count_15s / 15.0
    rate_30s = count_30s / 30.0
    rate_60s = count_60s / 60.0
    rate_300s = count_300s / 300.0

    return (rate_5s, rate_15s, rate_30s, rate_60s, rate_300s)


def compute_signal_stats() -> Tuple[float, float, float]:
    """
    Computes min, max, and average signal level over the last 30s.
    """
    if not SIGNAL_LEVELS_30S:
        return (0.0, 0.0, 0.0)
    values = [v[1] for v in SIGNAL_LEVELS_30S]
    return (min(values), max(values), sum(values) / len(values))


def compute_signal_percentiles() -> dict:
    """
    Computes selected percentiles of signal level over the last 30s.
    """
    if not SIGNAL_LEVELS_30S:
        return {"25": 0, "50": 0, "75": 0, "90": 0, "95": 0, "99": 0}
    values = sorted([v[1] for v in SIGNAL_LEVELS_30S])

    def pct(p):
        return statistics.quantiles(values, n=100, method='inclusive')[p - 1]

    return {
        "25": pct(25),
        "50": pct(50),
        "75": pct(75),
        "90": pct(90),
        "95": pct(95),
        "99": pct(99)
    }


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Returns distance in km between two lat/lon points using the Haversine formula.
    """
    r = 6371
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * \
        cos(radians(lat2)) * sin(d_lon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def compute_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Returns the bearing in degrees from point (lat1, lon1) to point (lat2, lon2).
    """
    d_lon = radians(lon2 - lon1)
    y = sin(d_lon) * cos(radians(lat2))
    x = cos(radians(lat1)) * sin(radians(lat2)) - \
        sin(radians(lat1)) * cos(radians(lat2)) * cos(d_lon)
    return (degrees(atan2(y, x)) + 360) % 360


def compute_distance_stats() -> dict:
    """
    Computes min, max, 25%, 50%, 75%, 90%, 95% distance over the last 30s.
    """
    if not DISTANCES_30S:
        return {"min": 0, "25": 0, "50": 0, "75": 0, "90": 0, "95": 0, "max": 0}
    values = sorted([d[1] for d in DISTANCES_30S])

    def pct(p):
        return statistics.quantiles(values, n=100, method='inclusive')[p - 1]

    return {
        "min": values[0],
        "25": pct(25),
        "50": pct(50),
        "75": pct(75),
        "90": pct(90),
        "95": pct(95),
        "max": values[-1]
    }


def compute_distance_buckets() -> list:
    """
    Computes the rolling 30s count of position readings in 30km buckets (up to 300km).
    """
    buckets = [0] * 10
    for _, dist in DISTANCES_30S:
        index = int(dist // 30)
        if index >= len(buckets):
            index = len(buckets) - 1
        buckets[index] += 1
    return buckets


def compute_coverage_stats() -> list:
    """
    Computes the maximum distance read and count of readings in each sector over the last 60s.
    """
    coverage = [
        [(0, 0)] * BEARING_SEGMENTS for _ in range(MAX_DISTANCE // RING_DISTANCE)]
    for (ring, segment), (ts, dist) in COVERAGE_60S.items():
        max_dist, count = coverage[ring][segment]
        coverage[ring][segment] = (max(max_dist, dist), count + 1)
    return coverage


def update_long_term_msg_rates(rate_60s: float, now: float) -> None:
    """
    Updates the long-term message rates with the latest 60s average.
    """
    LONG_TERM_MSG_RATES.append(rate_60s)
    LONG_TERM_MSG_TIMESTAMPS.append(now)
    # Keep only the last 12 hours of data (720 minutes)
    if len(LONG_TERM_MSG_RATES) > 720:
        LONG_TERM_MSG_RATES.popleft()
        LONG_TERM_MSG_TIMESTAMPS.popleft()


def compute_long_term_averages() -> Tuple[float, float, float]:
    """
    Computes 5m, 15m, and 60m averages from the long-term message rates.
    """

    global message_timestamps

    if not LONG_TERM_MSG_RATES:
        # Fallback if there's no long-term data yet
        now = get_current_time()
        count_5s = sum(1 for t in message_timestamps if now - t <= 5)
        rate_5s = count_5s / 5.0
        return (rate_5s, rate_5s, rate_5s)

    now = get_current_time()
    rates_5m = [
        rate for ts, rate in zip(LONG_TERM_MSG_TIMESTAMPS, LONG_TERM_MSG_RATES)
        if ts >= now - 300
    ]
    rates_15m = [
        rate for ts, rate in zip(LONG_TERM_MSG_TIMESTAMPS, LONG_TERM_MSG_RATES)
        if ts >= now - 900
    ]
    rates_60m = [
        rate for ts, rate in zip(LONG_TERM_MSG_TIMESTAMPS, LONG_TERM_MSG_RATES)
        if ts >= now - 3600
    ]

    avg_5m = sum(rates_5m) / len(rates_5m) if rates_5m else 0.0
    avg_15m = sum(rates_15m) / len(rates_15m) if rates_15m else 0.0
    avg_60m = sum(rates_60m) / len(rates_60m) if rates_60m else 0.0

    return (avg_5m, avg_15m, avg_60m)


def compute_rssi_distance_ratio() -> Tuple[list, list, list]:
    """
    Computes the ratio of RSSI to distance for each segment and bearing from the last 30s.
    """
    ratio_data = [0] * BEARING_SEGMENTS
    count_data = [0] * BEARING_SEGMENTS

    for ts, ratio, lat, lon in DISTANCE_RSSI_RATIO_30S:
        segment = int(
            compute_bearing(ref_lat, ref_lon, lat, lon) // (360 / BEARING_SEGMENTS)
        )
        ratio_data[segment] += ratio
        count_data[segment] += 1

    # Average the ratios
    for segment in range(BEARING_SEGMENTS):
        if count_data[segment] > 0:
            ratio_data[segment] /= count_data[segment]

    return ratio_data, MIN_RATIO_BY_BEARING, MAX_RATIO_BY_BEARING


class ADSBClient(TcpClient):
    """
    Custom ADS-B client that extends TcpClient to handle Mode-S messages.
    """

    def __init__(self, host, port, rawtype='beast'):
        super(ADSBClient, self).__init__(host, port, rawtype)

    def handle_messages(self, messages):

        global message_timestamps

        try:
            for msg, dbfs_rssi, ts in messages:
                if len(msg) < 2:
                    continue
                with DATA_LOCK:
                    # Only store message timestamps in the 300s buffer
                    message_timestamps.append(ts)

                    if dbfs_rssi is not None and dbfs_rssi > -100:
                        SIGNAL_LEVELS_30S.append((ts, dbfs_rssi))

                    df_val = pms.df(msg)
                    if df_val in (17, 18) and pms.crc(msg) == 0:
                        tc = pms.typecode(msg)
                        lat, lon = None, None
                        if 5 <= tc <= 8:
                            lat, lon = pms.adsb.surface_position_with_ref(
                                msg, ref_lat, ref_lon)
                        elif 9 <= tc <= 18 or 20 <= tc <= 22:
                            lat, lon = pms.adsb.airborne_position_with_ref(
                                msg, ref_lat, ref_lon)
                        if lat is not None and lon is not None:
                            dist_km = haversine_distance(
                                ref_lat, ref_lon, lat, lon)
                            DISTANCES_30S.append((ts, dist_km))
                            if dist_km <= MAX_DISTANCE:
                                ring = int(dist_km // RING_DISTANCE)
                                bearing = compute_bearing(
                                    ref_lat, ref_lon, lat, lon)
                                segment = int(
                                    bearing // (360 / BEARING_SEGMENTS))
                                key = (ring, segment)
                                if key in COVERAGE_60S:
                                    COVERAGE_60S[key] = (
                                        ts, max(COVERAGE_60S[key][1], dist_km))
                                else:
                                    COVERAGE_60S[key] = (ts, dist_km)
                                update_min_rssi_by_bearing(
                                    segment, dbfs_rssi, ts)

                                # Compute and store the RSSI/distance ratio along with lat and lon
                                ratio = abs(dbfs_rssi) / (dist_km if dist_km > 0 else 0.0001)
                                DISTANCE_RSSI_RATIO_30S.append((ts, ratio, lat, lon))

                                # Update running min and max ratios
                                MIN_RATIO_BY_BEARING[segment] = min(
                                    MIN_RATIO_BY_BEARING[segment], ratio
                                )
                                MAX_RATIO_BY_BEARING[segment] = max(
                                    MAX_RATIO_BY_BEARING[segment], ratio
                                )
        except Exception as e:
            logger.error("Error handling messages", exc_info=True)

    def run(self, raw_pipe_in=None, stop_flag=None, exception_queue=None):
        self.raw_pipe_in = raw_pipe_in
        self.exception_queue = exception_queue
        self.stop_flag = stop_flag
        self.connect()

        while True:
            try:
                received = [i for i in self.socket.recv(4096)]
                self.buffer.extend(received)
                messages = self.read_beast_buffer_rssi_piaware()
                if not messages:
                    continue
                else:
                    self.handle_messages(messages)
            except Exception as e:
                logger.error("Error in ADSBClient run loop", exc_info=True)

    def read_beast_buffer_rssi_piaware(self):
        """Handle mode-s beast data type.

        <esc> "1" : 6 byte MLAT timestamp, 1 byte signal level,
            2 byte Mode-AC
        <esc> "2" : 6 byte MLAT timestamp, 1 byte signal level,
            7 byte Mode-S short frame
        <esc> "3" : 6 byte MLAT timestamp, 1 byte signal level,
            14 byte Mode-S long frame
        <esc> "4" : 6 byte MLAT timestamp, status data, DIP switch
            configuration settings (not on Mode-S Beast classic)
        <esc><esc>: true 0x1a
        <esc> is 0x1a, and "1", "2" and "3" are 0x31, 0x32 and 0x33

        timestamp:
        wiki.modesbeast.com/Radarcape:Firmware_Versions#The_GPS_timestamp
        """
        messages_mlat = []
        msg = []
        i = 0

        try:
            # process the buffer until the last divider <esc> 0x1a
            # then reset self.buffer with the remainder
            while i < len(self.buffer):
                if self.buffer[i: i + 2] == [0x1A, 0x1A]:
                    msg.append(0x1A)
                    i += 1
                elif (i == len(self.buffer) - 1) and (self.buffer[i] == 0x1A):
                    # special case where the last bit is 0x1a
                    msg.append(0x1A)
                elif self.buffer[i] == 0x1A:
                    if i == len(self.buffer) - 1:
                        # special case where the last bit is 0x1a
                        msg.append(0x1A)
                    elif len(msg) > 0:
                        messages_mlat.append(msg)
                        msg = []
                else:
                    msg.append(self.buffer[i])
                i += 1

            # save the reminder for next reading cycle, if not empty
            if len(msg) > 0:
                reminder = []
                for i, m in enumerate(msg):
                    if (m == 0x1A) and (i < len(msg) - 1):
                        # rewind 0x1a, except when it is at the last bit
                        reminder.extend([m, m])
                    else:
                        reminder.append(m)
                self.buffer = [0x1A] + msg
            else:
                self.buffer = []

            # extract messages
            messages = []
            for mm in messages_mlat:
                ts = time.time()

                msgtype = mm[0]
                if msgtype == 0x32:
                    # Mode-S Short Message, 7 byte, 14-len hexstr
                    msg_str = "".join("%02X" % i for i in mm[8:15])
                elif msgtype == 0x33:
                    # Mode-S Long Message, 14 byte, 28-len hexstr
                    msg_str = "".join("%02X" % i for i in mm[8:22])
                else:
                    # Other message type
                    continue

                if len(msg_str) not in [14, 28]:
                    continue

                '''
                    we get the raw 0-255 byte value (raw_rssi = mm[7])
                    we scale it to 0.0 - 1.0 (voltage = raw_rssi / 255)
                    we convert it to a dBFS power value (rolling the squaring of the voltage into the dB calculation)
                '''
                try:
                    df = pms.df(msg_str)
                    raw_rssi = mm[7]
                    if raw_rssi == 0:
                        dbfs_rssi = -100
                    else:
                        rssi_ratio = raw_rssi / 255
                        signalLevel = rssi_ratio ** 2
                        dbfs_rssi = 10 * math.log10(signalLevel)
                except Exception:
                    logger.error("Error processing RSSI", exc_info=True)
                    continue

                # skip incomplete message
                if df in [0, 4, 5, 11] and len(msg_str) != 14:
                    continue
                if df in [16, 17, 18, 19, 20, 21, 24] and len(msg_str) != 28:
                    continue

                messages.append([msg_str, dbfs_rssi, ts])
            return messages
        except Exception as e:
            logger.error("Error reading beast buffer", exc_info=True)
            return []


async def broadcast_stats_task() -> None:
    """
    Async background task that periodically computes and broadcasts stats.
    """
    last_minute_update_time = 0.0
    last_minute_update_time = get_current_time() // 60 * 60
    global MAX_MESSAGE_RATE
    global proc

    # delay to allow collection of initial data
    await asyncio.sleep(5.0) 

    while True:
        await asyncio.sleep(1.0)
        try:
            now = get_current_time()
            # Update other windows to evict old data
            update_signal_sliding_window(now)
            update_distance_sliding_window(now)
            update_coverage_sliding_window(now)
            expire_min_rssi(now)
            expire_distance_rssi_ratio(now)

            with DATA_LOCK:
                # Compute message rates in a single pass
                rate_5s, rate_15s, rate_30s, rate_60s, rate_300s = compute_message_rates()

            if now - last_minute_update_time >= 60.0:
                # Minute update
                last_minute_update_time = now
                with DATA_LOCK:
                    update_long_term_msg_rates(rate_60s, get_current_time())
                    avg_5m, avg_15m, avg_60m = compute_long_term_averages()

                payload = {
                    "msg5mAvg": avg_5m,
                    "msg15mAvg": avg_15m,
                    "msg60mAvg": avg_60m
                }

                cloudevent = {
                    "specversion": "1.0",
                    "type": "com.vasters.signalstats1090.minuteUpdate",
                    "source": "signalstats1090",
                    "id": str(uuid.uuid4()),
                    "time": datetime.datetime.utcnow().isoformat(),
                    "data": payload
                }

                # Cache the long-term message rates
                LONG_TERM_CACHE.append(cloudevent)

                # Broadcast to all connected clients
                for ws_conn in list(CONNECTED_WEBSOCKETS):
                    try:
                        await ws_conn.send_text(json.dumps(cloudevent))
                    except Exception:
                        logger.error("Error broadcasting stats", exc_info=True)

            # Second update (every second)
            with DATA_LOCK:
                sig_min, sig_max, sig_avg = compute_signal_stats()
                sig_p = compute_signal_percentiles()
                dist_stats = compute_distance_stats()
                dist_hist = compute_distance_buckets()
                coverage_stats = compute_coverage_stats()
                min_rssi_by_bearing = MIN_RSSI_BY_BEARING.copy()
                ratio_data, min_ratio_data, max_ratio_data = compute_rssi_distance_ratio()
                MAX_MESSAGE_RATE = max(
                    MAX_MESSAGE_RATE,
                    rate_5s, rate_15s, rate_30s, rate_60s, rate_300s
                )

                # Obtain memory and CPU stats
                memory_info = proc.memory_percent()
                cpu_percent = proc.cpu_percent(interval=None)
                

            payload = {
                "msg5s": rate_5s,
                "msg15s": rate_15s,
                "msg30s": rate_30s,
                "msg60s": rate_60s,
                "msg300s": rate_300s,
                "sigMin": sig_min,
                "sigMax": sig_max,
                "sigAvg": sig_avg,
                "sig25": sig_p["25"],
                "sig50": sig_p["50"],
                "sig75": sig_p["75"],
                "sig90": sig_p["90"],
                "sig95": sig_p["95"],
                "sig99": sig_p["99"],
                "distMin": dist_stats["min"],
                "dist25": dist_stats["25"],
                "dist50": dist_stats["50"],
                "dist75": dist_stats["75"],
                "dist90": dist_stats["90"],
                "dist95": dist_stats["95"],
                "distMax": dist_stats["max"],
                "distHistogram": dist_hist,
                "coverageStats": coverage_stats,
                "minRssiByBearing": min_rssi_by_bearing,
                "ratioData": ratio_data,
                "minRatioData": min_ratio_data,
                "maxRatioData": max_ratio_data,
                "maxMsgRate": MAX_MESSAGE_RATE,
                "memoryUsage": memory_info,
                "cpuUsage": cpu_percent
            }

            cloudevent = {
                "specversion": "1.0",
                "type": "com.vasters.signalstats1090.secondUpdate",
                "source": "signalstats1090",
                "id": str(uuid.uuid4()),
                "time": datetime.datetime.utcnow().isoformat(),
                "data": payload
            }

            # Broadcast to all connected clients
            for ws_conn in list(CONNECTED_WEBSOCKETS):
                try:
                    await ws_conn.send_text(json.dumps(cloudevent))
                except Exception:
                    logger.error("Error broadcasting stats", exc_info=True)

        except Exception:
            logger.error("Error in broadcast_stats_task", exc_info=True)


@contextlib.asynccontextmanager
async def app_lifespan(app_ref: FastAPI):
    """
    Lifespan context to replace deprecated @app.on_event usage.
    Starts the BEAST listener and stats broadcaster at startup.
    """
    # Start ADSB client
    client = ADSBClient(host=dump1090_host, port=dump1090_port, rawtype='beast')
    threading.Thread(target=client.run, daemon=True).start()

    asyncio.create_task(broadcast_stats_task())

    yield  # Wait for shutdown if needed.


app = FastAPI(lifespan=app_lifespan)


@app.get("/", response_class=HTMLResponse)
def get_index() -> str:
    """
    Serves the external index.html file.
    """
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.isfile(index_path):
        with open(index_path, "r", encoding="utf-8") as file:
            return file.read()

    # Fallback if not found next to this file
    with open("index.html", "r", encoding="utf-8") as file:
        return file.read()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    WebSocket endpoint that registers the connection and handles incoming messages.
    Periodic updates are broadcast from broadcast_stats_task().
    """
    print("New WebSocket connection")
    await websocket.accept()
    CONNECTED_WEBSOCKETS.add(websocket)
    try:
        # Send cached long-term message rates to the newly connected client
        for cached_event in LONG_TERM_CACHE:
            await websocket.send_text(json.dumps(cached_event))

        while True:
            # Wait for a message to confirm connection.
            msg = await websocket.receive_text()
            print("Received from client:", msg)
    except WebSocketDisconnect:
        CONNECTED_WEBSOCKETS.remove(websocket)
        print("WebSocket disconnected")


def add_common_arguments(parser):
    parser.add_argument("--host", type=str,
                        help="Host to run the web server on.")
    parser.add_argument("--port", type=int,
                        help="Port to run the web server on.")
    parser.add_argument("--antenna-lat", type=float, help="Antenna latitude.")
    parser.add_argument("--antenna-lon", type=float, help="Antenna longitude.")
    parser.add_argument("--dump1090-host", type=str,
                        help="Host running dump1090.")
    parser.add_argument("--dump1090-port", type=int, help="Port for dump1090.")


def main():
    parser = argparse.ArgumentParser(description="signalstats1090.")
    parser.add_argument("-c", "--config-file", type=str, default="~/.signalstats1090.config",
                        help="Path to the config file. Defaults to '~/.signalstats1090.config'.")
    subparsers = parser.add_subparsers(
        dest="command", help="Subcommands: run, config, install, or uninstall")

    run_parser = subparsers.add_parser("run", help="Run the web server")
    config_parser = subparsers.add_parser(
        "config", help="Write config to file")

    defaults = {
        "host": "0.0.0.0",
        "port": 8000,
        "antenna_lat": None,
        "antenna_lon": None,
        "dump1090_host": "localhost",
        "dump1090_port": 30005
    }

    add_common_arguments(run_parser)
    add_common_arguments(config_parser)

    args = parser.parse_args()
    config_file = os.path.expanduser(args.config_file)

    # Load existing config if present
    if os.path.isfile(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as cf:
                stored = json.load(cf)
                for k, v in stored.items():
                    if k in defaults and getattr(args, k) is None:
                        setattr(args, k, v)
        except Exception:
            logger.error("Failed to load config file", exc_info=True)

    # Set defaults if not provided
    for k, v in defaults.items():
        if not hasattr(args, k) or getattr(args, k) is None:
            setattr(args, k, v)

    if args.command == "config":
        new_conf = {
            "host": args.host,
            "port": args.port,
            "antenna_lat": args.antenna_lat,
            "antenna_lon": args.antenna_lon,
            "dump1090_host": args.dump1090_host,
            "dump1090_port": args.dump1090_port
        }
        try:
            with open(config_file, "w", encoding="utf-8") as cf:
                json.dump(new_conf, cf)
            logger.info(f"Configuration written to {config_file}")
        except Exception as e:
            logger.error("Failed to write config", exc_info=True)

    elif args.command == "run":
        global ref_lat, ref_lon
        if args.antenna_lat is None or args.antenna_lon is None:
            parser.error(
                "Must provide --antenna-lat and --antenna-lon or set them via 'config' first.")
        logger.info(f"Starting server on {args.host}:{args.port}")
        logger.info(
            f"Using antenna location: {args.antenna_lat}, {args.antenna_lon}")
        ref_lat = args.antenna_lat
        ref_lon = args.antenna_lon
        global dump1090_host, dump1090_port
        dump1090_host = args.dump1090_host
        dump1090_port = args.dump1090_port
        uvicorn.run(app, host=args.host, port=args.port)

    else:
        parser.print_help()
        logger.info("Printed help message")


if __name__ == "__main__":
    main()

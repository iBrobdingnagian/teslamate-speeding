#!/usr/bin/env python3
"""
TeslaMate Unified Alert System
Monitors MQTT for sentry mode, TPMS pressure, and speed limit violations
Sends Telegram alerts for all events

Requirements:
    pip install paho-mqtt requests python-dotenv psycopg2-binary

Configuration:
    Set environment variables in .env file:
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID
    - MQTT_HOST (default: localhost)
    - MQTT_PORT (default: 1883)
    - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import os
import sys
import time
import threading
import logging
import requests
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt
import signal
import argparse
import json
import pickle
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from contextlib import closing
from pathlib import Path

from alert_threads import ThreadsMixin  # event correlation and routing

# Load the .env in the repository root (alerts/ is one level below it)
load_dotenv(Path(__file__).resolve().parent.parent / '.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# MQTT Configuration
MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME', '')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD', '')
MQTT_BASE_TOPIC = os.getenv('MQTT_BASE_TOPIC', 'teslamate')

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('TLG_API')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID') or os.getenv('TLG_CHAT')
TELEGRAM_API_BASE = os.getenv('TELEGRAM_API_BASE', 'https://api.telegram.org')

# Database Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'teslamate')
DB_USER = os.getenv('DB_USER', 'teslamate')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'secret')

# Alert Parameters
SPEED_THRESHOLD_PERCENT = 3      # % over speed limit
SPEED_ALERT_DURATION = 5         # Minimum seconds before speed alert
SPEED_COOLDOWN = 60              # Seconds between speed alerts

TPMS_THRESHOLD_BAR = 2.0         # Alert if pressure below this
TPMS_COOLDOWN = 300              # 5 minutes between TPMS alerts per tire

SENTRY_COOLDOWN = 300            # 5 minutes between sentry alerts

# Door/Window/Trunk alert parameters
DOOR_ALERT_DELAY = 60            # Alert if door/window open for this many seconds
DOOR_COOLDOWN = 300              # 5 minutes between door/window alerts
UNLOCKED_NO_USER_DELAY = 120     # Alert if unlocked for 2 min after user leaves
UNLOCKED_NO_USER_COOLDOWN = 300  # 5 minutes between unlocked-no-user alerts

# Locked with user present alert
LOCKED_USER_PRESENT_DELAY = 120      # Alert if locked with user for 2 min
LOCKED_USER_PRESENT_COOLDOWN = 300  # 5 minutes between alerts

# Car started alert (heading change detection)
CAR_STARTED_COOLDOWN = 300       # 5 minutes between car started alerts

# Cabin overheat protection alert
CABIN_TEMP_THRESHOLD = 40        # Alert if inside temp > 40°C
CABIN_ALERT_DELAY = 120          # Alert after 2 minutes of high temp
CABIN_COOLDOWN = 600             # 10 minutes between cabin overheat alerts

# Charging alert parameters
CHARGING_COOLDOWN = 300          # 5 minutes between charging alerts
LOW_POWER_THRESHOLD = 1.0        # Alert if charging < 1 kW
CHARGE_LIMIT_78_THRESHOLD = 78   # Alert when reaching 78% charge
CHARGE_LIMIT_98_THRESHOLD = 98   # Alert when reaching 98% charge

# Software update alert parameters
UPDATE_COOLDOWN = 86400          # 24 hours between update available alerts
UPDATE_INSTALL_MAX_DURATION = 7200  # 2 hours - treat as stalled/cancelled if still "installing" past this

# Climate alert parameters
CLIMATE_LEFT_ON_DELAY = 300      # Alert if climate on for 5 min while parked
CLIMATE_COOLDOWN = 600           # 10 minutes between climate alerts

# Range alert parameters
LOW_RANGE_THRESHOLD_KM = 50      # Alert if range below this
RANGE_COOLDOWN = 3600            # 1 hour between range alerts

# Geofence alert parameters
GEOFENCE_COOLDOWN = 300          # 5 minutes between geofence alerts

# Parked weather digest (fires when the car transitions driving -> parked)
PARKED_WEATHER_COOLDOWN = 300    # 5 minutes - guards against state flapping
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_CODE_EMOJI = {
    0: "☀️", 1: "🌤️", 2: "⛅", 3: "☁️",
    45: "🌫️", 48: "🌫️",
    51: "🌦️", 53: "🌦️", 55: "🌦️", 56: "🌦️", 57: "🌦️",
    61: "🌧️", 63: "🌧️", 65: "🌧️", 66: "🌧️", 67: "🌧️",
    71: "🌨️", 73: "🌨️", 75: "🌨️", 77: "🌨️",
    80: "🌦️", 81: "🌧️", 82: "⛈️",
    85: "🌨️", 86: "🌨️",
    95: "⛈️", 96: "⛈️", 99: "⛈️",
}

# Telegram retry parameters
TELEGRAM_MAX_RETRIES = 3         # Maximum retry attempts
TELEGRAM_RETRY_DELAY = 2         # Initial delay in seconds (exponential backoff)

# State persistence
STATE_FILE = os.getenv('STATE_FILE', str(Path(__file__).resolve().parent / 'state/unified_alert.pkl'))

# Alert rate limiting
MAX_ALERTS_PER_HOUR = int(os.getenv('MAX_ALERTS_PER_HOUR', '30'))

# Tire position mapping
TIRE_POSITIONS = {
    'tpms_pressure_fl': 'Front Left',
    'tpms_pressure_fr': 'Front Right',
    'tpms_pressure_rl': 'Rear Left',
    'tpms_pressure_rr': 'Rear Right'
}

# Validate configuration
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.error("❌ Telegram credentials not found in .env file")
    logger.error("   Please configure TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID")
    sys.exit(1)


class UnifiedAlertMonitor(ThreadsMixin):
    def __init__(self, restore_state=True):
        # Use MQTT v2 API - clean_start replaces clean_session
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        # Set connection parameters for stability
        self.client.max_inflight_messages_set(20)
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.car_data = defaultdict(dict)

        # Guards all per-car state dicts below, since on_message() runs on the
        # paho-mqtt network thread while save_state()/get_statistics() run on
        # the main thread (see run()) and read/copy the same dicts.
        self.state_lock = threading.RLock()

        # Statistics tracking
        self.start_time = datetime.now()
        self.alert_counts = defaultdict(int)
        self.alert_history = []  # Track last hour of alerts
        self.telegram_failures = 0
        self.telegram_successes = 0
        self.last_mqtt_message = None
        self.mqtt_message_count = 0
        
        # Speed monitoring
        self.speed_violation_start_times = {}
        self.speed_alert_sent = {}
        self.speed_last_alert_times = {}
        
        # TPMS monitoring
        self.tpms_low_pressure_tires = defaultdict(dict)
        self.tpms_last_alert_times = defaultdict(dict)
        
        # Sentry monitoring
        self.sentry_active = {}
        self.sentry_last_alert_times = {}
        
        # Door/Window/Trunk monitoring
        self.doors_open_start_time = {}
        self.doors_alert_sent = {}
        self.doors_last_alert_time = {}
        self.unlocked_no_user_start_time = {}
        self.unlocked_no_user_alert_sent = {}
        self.unlocked_no_user_last_alert_time = {}
        
        # Locked with user present monitoring
        self.locked_user_present_start_time = {}
        self.locked_user_present_alert_sent = {}
        self.locked_user_present_last_alert = {}
        
        # Car started monitoring (heading change)
        self.last_heading = {}
        self.car_started_last_alert = {}
        
        # Cabin overheat monitoring
        self.cabin_overheat_start_time = {}
        self.cabin_overheat_alert_sent = {}
        self.cabin_overheat_last_alert = {}
        self.recently_driven = {}  # Track if car was recently driven
        
        # Charging monitoring
        self.plugged_in = {}
        self.charging_last_alert = {}
        self.charge_start_time = {}
        self.charge_start_battery = {}
        self.charge_78_alerted = {}
        self.charge_98_alerted = {}
        
        # Software update monitoring
        self.update_available_alerted = {}
        self.update_status_last = {}
        self.update_version_tracking = {}
        self.update_installing = {}
        self.update_install_start_time = {}
        
        # Climate control monitoring
        self.climate_on = {}
        self.climate_left_on_alert_sent = {}
        self.dog_mode_alerted = {}
        
        # Range monitoring
        self.last_range = {}
        self.range_alert_sent = {}
        self.overnight_range_check = {}
        
        # Geofence named location tracking
        self.current_geofence = {}
        self.geofence_last_alert = {}

        # Parked weather digest tracking
        self.last_car_state = {}
        self.parked_weather_last_alert = {}
        
        # Database connection
        self.db_conn = None
        
        # Reconnection tracking
        self.reconnect_attempts = 0
        
        # Set MQTT credentials if provided
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        
        # Set MQTT callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        
        # Connect to database
        self.connect_database()
        
        # Restore previous state if requested
        # correlation state (drive thread, parked incident) must exist before load_state()
        self.threads_init(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MAX_ALERTS_PER_HOUR)
        self.cabin_threshold = CABIN_TEMP_THRESHOLD

        if restore_state:
            self.load_state()
    
    def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.db_conn.autocommit = False
            logger.info(f"✓ Connected to database at {DB_HOST}:{DB_PORT}/{DB_NAME}")
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            logger.warning("⚠️  Continuing without database logging...")
            self.db_conn = None
    
    def save_state(self):
        """Save monitoring state to disk for restart persistence"""
        try:
            with self.state_lock:
                state = {
                    'speed_violation_start_times': dict(self.speed_violation_start_times),
                    'speed_last_alert_times': dict(self.speed_last_alert_times),
                    'tpms_last_alert_times': dict(self.tpms_last_alert_times),
                    'sentry_active': dict(self.sentry_active),
                    'sentry_last_alert_times': dict(self.sentry_last_alert_times),
                    'doors_last_alert_time': dict(self.doors_last_alert_time),
                    'locked_user_present_last_alert': dict(self.locked_user_present_last_alert),
                    'car_started_last_alert': dict(self.car_started_last_alert),
                    'cabin_overheat_last_alert': dict(self.cabin_overheat_last_alert),
                    'charging_last_alert': dict(self.charging_last_alert),
                    'plugged_in': dict(self.plugged_in),
                    'charge_78_alerted': dict(self.charge_78_alerted),
                    'charge_98_alerted': dict(self.charge_98_alerted),
                    'update_available_alerted': dict(self.update_available_alerted),
                    'climate_left_on_alert_sent': dict(self.climate_left_on_alert_sent),
                    'range_alert_sent': dict(self.range_alert_sent),
                    'geofence_last_alert': dict(self.geofence_last_alert),
                    'current_geofence': dict(self.current_geofence),
                    'last_heading': dict(self.last_heading),
                    'last_car_state': dict(self.last_car_state),
                    'parked_weather_last_alert': dict(self.parked_weather_last_alert),
                    'alert_counts': dict(self.alert_counts),
                    'telegram_failures': self.telegram_failures,
                    'telegram_successes': self.telegram_successes,
                    'threads': self.threads_state(),
                    'saved_at': datetime.now().isoformat()
                }

            state_path = Path(STATE_FILE)
            state_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(STATE_FILE, 'wb') as f:
                pickle.dump(state, f)
            
            logger.info(f"✓ State saved to {STATE_FILE}")
        except Exception as e:
            logger.error(f"✗ Failed to save state: {e}")
    
    def load_state(self):
        """Load previous monitoring state from disk"""
        try:
            if not Path(STATE_FILE).exists():
                logger.info("No previous state file found, starting fresh")
                return
            
            with open(STATE_FILE, 'rb') as f:
                state = pickle.load(f)
            
            # Restore state
            self.speed_violation_start_times = defaultdict(dict, state.get('speed_violation_start_times', {}))
            self.speed_last_alert_times = defaultdict(dict, state.get('speed_last_alert_times', {}))
            self.tpms_last_alert_times = defaultdict(dict, state.get('tpms_last_alert_times', {}))
            self.sentry_active = state.get('sentry_active', {})
            self.sentry_last_alert_times = state.get('sentry_last_alert_times', {})
            self.doors_last_alert_time = state.get('doors_last_alert_time', {})
            self.locked_user_present_last_alert = state.get('locked_user_present_last_alert', {})
            self.car_started_last_alert = state.get('car_started_last_alert', {})
            self.cabin_overheat_last_alert = state.get('cabin_overheat_last_alert', {})
            self.charging_last_alert = state.get('charging_last_alert', {})
            self.plugged_in = state.get('plugged_in', {})
            self.charge_78_alerted = state.get('charge_78_alerted', {})
            self.charge_98_alerted = state.get('charge_98_alerted', {})
            self.update_available_alerted = state.get('update_available_alerted', {})
            self.climate_left_on_alert_sent = state.get('climate_left_on_alert_sent', {})
            self.range_alert_sent = state.get('range_alert_sent', {})
            self.geofence_last_alert = state.get('geofence_last_alert', {})
            self.current_geofence = state.get('current_geofence', {})
            self.last_heading = state.get('last_heading', {})
            self.last_car_state = state.get('last_car_state', {})
            self.parked_weather_last_alert = state.get('parked_weather_last_alert', {})
            self.alert_counts = defaultdict(int, state.get('alert_counts', {}))
            self.telegram_failures = state.get('telegram_failures', 0)
            self.telegram_successes = state.get('telegram_successes', 0)
            self.threads_restore(state.get('threads'))
            
            saved_at = state.get('saved_at', 'unknown')
            logger.info(f"✓ Previous state restored from {STATE_FILE}")
            logger.info(f"  Last saved: {saved_at}")
            logger.info(f"  Alert history: {sum(self.alert_counts.values())} total alerts")
        except Exception as e:
            logger.error(f"✗ Failed to load state: {e}")
            logger.info("Starting with fresh state")
    
    def get_statistics(self):
        """Get monitoring statistics"""
        uptime = datetime.now() - self.start_time
        hours = uptime.total_seconds() / 3600

        # Calculate alert rate (last hour)
        one_hour_ago = datetime.now() - timedelta(hours=1)

        with self.state_lock:
            recent_alerts = [a for a in self.alert_history if a > one_hour_ago]
            total_alerts = sum(self.alert_counts.values())
            alert_breakdown = dict(self.alert_counts)
            telegram_successes = self.telegram_successes
            telegram_failures = self.telegram_failures
            monitored_cars = len(self.car_data)

        return {
            'uptime_hours': round(hours, 2),
            'uptime_str': str(uptime).split('.')[0],
            'mqtt_messages': self.mqtt_message_count,
            'last_mqtt_message': self.last_mqtt_message.isoformat() if self.last_mqtt_message else 'None',
            'total_alerts': total_alerts,
            'alerts_last_hour': len(recent_alerts),
            'telegram_success_rate': round(100 * telegram_successes / max(1, telegram_successes + telegram_failures), 1),
            'telegram_successes': telegram_successes,
            'telegram_failures': telegram_failures,
            'alert_breakdown': alert_breakdown,
            'monitored_cars': monitored_cars
        }
    
    def log_statistics(self):
        """Log current statistics"""
        stats = self.get_statistics()
        logger.info("=" * 60)
        logger.info("📊 MONITORING STATISTICS")
        logger.info("=" * 60)
        logger.info(f"⏱️  Uptime: {stats['uptime_str']}")
        logger.info(f"📡 MQTT Messages: {stats['mqtt_messages']}")
        logger.info(f"🚗 Cars Monitored: {stats['monitored_cars']}")
        logger.info(f"🔔 Total Alerts: {stats['total_alerts']}")
        logger.info(f"📈 Alerts (last hour): {stats['alerts_last_hour']}")
        logger.info(f"✅ Telegram Success Rate: {stats['telegram_success_rate']}%")
        if stats['alert_breakdown']:
            logger.info("📋 Alert Breakdown:")
            for alert_type, count in sorted(stats['alert_breakdown'].items(), key=lambda x: x[1], reverse=True):
                logger.info(f"   {alert_type}: {count}")
        logger.info("=" * 60)
    
    def log_sentry_event(self, car_id, event_type, car_data):
        """Log sentry event to database"""
        if not self.db_conn:
            return
        
        try:
            with closing(self.db_conn.cursor()) as cursor:
                car_id_int = int(car_id)

                latitude = float(car_data.get('latitude')) if 'latitude' in car_data else None
                longitude = float(car_data.get('longitude')) if 'longitude' in car_data else None
                battery_level = int(car_data.get('battery_level')) if 'battery_level' in car_data else None
                locked = car_data.get('locked', '').lower() == 'true' if 'locked' in car_data else None
                windows_open = car_data.get('windows_open', '').lower() == 'true' if 'windows_open' in car_data else None
                state = car_data.get('state')
                odometer = float(car_data.get('odometer')) if 'odometer' in car_data else None

                cursor.execute("""
                    INSERT INTO sentry_events
                    (car_id, event_type, event_time, latitude, longitude, battery_level,
                     locked, windows_open, state, odometer, telegram_sent)
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
                """, (car_id_int, event_type, latitude, longitude, battery_level,
                      locked, windows_open, state, odometer, True))

            self.db_conn.commit()
            logger.info(f"  📝 Logged {event_type} event to database")

        except Exception as e:
            logger.error(f"✗ Error logging to database: {e}")
            if self.db_conn:
                self.db_conn.rollback()
    
    def on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback when connected to MQTT broker"""
        # Convert ReasonCode to integer if needed (paho-mqtt v2)
        rc_value = rc if isinstance(rc, int) else (rc.value if hasattr(rc, 'value') else rc)
        
        if rc_value == 0:
            logger.info(f"✓ Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
            self.reconnect_attempts = 0  # Reset counter on successful connection
            
            # Subscribe to all relevant TeslaMate topics
            topics = [
                # Sentry mode
                f"{MQTT_BASE_TOPIC}/cars/+/sentry_mode",
                # Speed monitoring
                f"{MQTT_BASE_TOPIC}/cars/+/speed",
                f"{MQTT_BASE_TOPIC}/cars/+/speed_limit",
                # TPMS
                f"{MQTT_BASE_TOPIC}/cars/+/tpms_pressure_fl",
                f"{MQTT_BASE_TOPIC}/cars/+/tpms_pressure_fr",
                f"{MQTT_BASE_TOPIC}/cars/+/tpms_pressure_rl",
                f"{MQTT_BASE_TOPIC}/cars/+/tpms_pressure_rr",
                # Common data
                f"{MQTT_BASE_TOPIC}/cars/+/display_name",
                f"{MQTT_BASE_TOPIC}/cars/+/latitude",
                f"{MQTT_BASE_TOPIC}/cars/+/longitude",
                f"{MQTT_BASE_TOPIC}/cars/+/state",
                f"{MQTT_BASE_TOPIC}/cars/+/battery_level",
                f"{MQTT_BASE_TOPIC}/cars/+/locked",
                f"{MQTT_BASE_TOPIC}/cars/+/windows_open",
                f"{MQTT_BASE_TOPIC}/cars/+/doors_open",
                f"{MQTT_BASE_TOPIC}/cars/+/trunk_open",
                f"{MQTT_BASE_TOPIC}/cars/+/frunk_open",
                f"{MQTT_BASE_TOPIC}/cars/+/odometer",
                f"{MQTT_BASE_TOPIC}/cars/+/inside_temp",
                f"{MQTT_BASE_TOPIC}/cars/+/outside_temp",
                f"{MQTT_BASE_TOPIC}/cars/+/heading",
                f"{MQTT_BASE_TOPIC}/cars/+/shift_state",
                f"{MQTT_BASE_TOPIC}/cars/+/is_user_present",
                # Charging
                f"{MQTT_BASE_TOPIC}/cars/+/plugged_in",
                f"{MQTT_BASE_TOPIC}/cars/+/charge_energy_added",
                f"{MQTT_BASE_TOPIC}/cars/+/charger_power",
                f"{MQTT_BASE_TOPIC}/cars/+/charging_state",
                f"{MQTT_BASE_TOPIC}/cars/+/time_to_full_charge",
                f"{MQTT_BASE_TOPIC}/cars/+/scheduled_charging_start_time",
                # Updates
                f"{MQTT_BASE_TOPIC}/cars/+/update_available",
                f"{MQTT_BASE_TOPIC}/cars/+/update_version",
                f"{MQTT_BASE_TOPIC}/cars/+/update_status",
                # Climate
                f"{MQTT_BASE_TOPIC}/cars/+/is_climate_on",
                f"{MQTT_BASE_TOPIC}/cars/+/climate_keeper_mode",
                # Range & Geofence
                f"{MQTT_BASE_TOPIC}/cars/+/ideal_battery_range_km",
                f"{MQTT_BASE_TOPIC}/cars/+/rated_battery_range_km",
                f"{MQTT_BASE_TOPIC}/cars/+/geofence",
            ]
            
            for topic in topics:
                client.subscribe(topic)
            
            logger.info("✓ Subscribed to all topics")
            logger.info("📡 Monitoring for:")
            logger.info("   🛡️  Sentry Mode activation/deactivation")
            logger.info(f"   🛞 TPMS pressure < {TPMS_THRESHOLD_BAR} bar")
            logger.info(f"   🚨 Speed violations > {SPEED_THRESHOLD_PERCENT}% over limit")
            logger.info(f"   🚪 Doors/Windows/Trunk open > {DOOR_ALERT_DELAY}s")
            logger.info(f"   🔓 Unlocked without user > {UNLOCKED_NO_USER_DELAY}s")
            logger.info(f"   🔒 Locked with user present > {LOCKED_USER_PRESENT_DELAY}s")
            logger.info(f"   🔋 Charging events (plugged/unplugged, limits at 78%/98%)")
            logger.info(f"   📱 Software updates (available/installing/completed)")
            logger.info(f"   ❄️  Climate control alerts")
            logger.info(f"   📍 Geofence entry/exit (named locations)")
            logger.info(f"   ⚡ Low range warnings")
            logger.info(f"   🚗 Car started (heading change)")
            logger.info(f"   🔥 Cabin overheat > {CABIN_TEMP_THRESHOLD}°C")
            logger.info("")
        else:
            logger.error(f"✗ MQTT connection failed with code: {rc_value}")
    
    def on_disconnect(self, client, userdata, flags, rc, properties=None):
        """Callback when disconnected from MQTT broker"""
        # Convert ReasonCode to integer if needed (paho-mqtt v2)
        rc_value = rc if isinstance(rc, int) else (rc.value if hasattr(rc, 'value') else 0)
        
        if rc_value != 0:
            # Map common return codes
            rc_messages = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized",
                7: "Connection lost"
            }
            reason = rc_messages.get(rc_value, f"Unspecified error (code {rc_value})")
            logger.warning(f"⚠️ Unexpected disconnect from MQTT broker: {reason}")
            logger.info("🔄 Attempting to reconnect...")
        else:
            logger.info("Disconnected from MQTT broker (clean shutdown)")
    
    
    def on_message(self, client, userdata, msg):
        """Callback when MQTT message received"""
        try:
            # Track MQTT activity
            self.mqtt_message_count += 1
            self.last_mqtt_message = datetime.now()

            parts = msg.topic.split('/')
            if len(parts) < 4:
                return

            car_id = parts[2]
            field = parts[3]
            value = msg.payload.decode().strip()

            # Held for the whole dispatch below: this runs on the paho-mqtt
            # network thread and mutates the same per-car state dicts that
            # save_state()/get_statistics() read from the main thread.
            with self.state_lock:
                # Store car data
                self.car_data[car_id][field] = value
                self.telemetry_watchdog.observe(car_id, field, value, msg.retain, self.clock())

                # Route to appropriate handler
                if field == 'sentry_mode':
                    self.handle_sentry_mode(car_id, value)
                elif field in ['speed', 'speed_limit']:
                    self.check_speed_violation(car_id)
                elif field in TIRE_POSITIONS:
                    self.check_tire_pressure(car_id, field)
                elif field in ['doors_open', 'windows_open', 'trunk_open', 'frunk_open']:
                    self.check_doors_windows(car_id)
                elif field == 'locked' or field == 'is_user_present':
                    self.check_locked_with_user(car_id)
                    self.check_unlocked_no_user(car_id)
                elif field == 'heading':
                    self.check_car_started(car_id, value)
                elif field == 'inside_temp' or field == 'state':
                    self.check_cabin_overheat(car_id)
                    if field == 'state':
                        self.check_car_parked(car_id, value)
                # New handlers
                elif field in ['plugged_in', 'charger_power', 'charge_energy_added', 'time_to_full_charge', 'battery_level']:
                    self.handle_charging(car_id, field, value)
                elif field in ['update_available', 'update_version', 'update_status']:
                    self.handle_update(car_id, field, value)
                elif field in ['is_climate_on', 'climate_keeper_mode']:
                    self.handle_climate(car_id, field, value)
                elif field in ['rated_battery_range_km', 'ideal_battery_range_km']:
                    self.handle_range(car_id, field, value)
                elif field == 'geofence':
                    self.handle_geofence(car_id, value)

        except Exception as e:
            logger.error(f"✗ Error processing message: {e}")
    
    # ==================== SENTRY MODE ====================
    
    def handle_sentry_mode(self, car_id, sentry_value):
        """Handle sentry mode state changes"""
        try:
            is_active = sentry_value.lower() in ['true', '1', 'on']
            was_active = self.sentry_active.get(car_id, False)
            
            if is_active and not was_active:
                logger.info(f"🚨 Car {car_id}: Sentry Mode ACTIVATED")
                self.log_sentry_event(car_id, 'activated', self.car_data[car_id])
                self.send_sentry_alert(car_id, activated=True)
                self.sentry_active[car_id] = True
            
            elif not is_active and was_active:
                logger.info(f"✓ Car {car_id}: Sentry Mode deactivated")
                self.log_sentry_event(car_id, 'deactivated', self.car_data[car_id])
                self.send_sentry_alert(car_id, activated=False)
                self.sentry_active[car_id] = False
                
        except Exception as e:
            logger.error(f"✗ Error handling sentry mode: {e}")
    
    def send_sentry_alert(self, car_id, activated=True):
        """Send Telegram alert for sentry mode"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        # Check cooldown
        now = datetime.now()
        last_alert = self.sentry_last_alert_times.get(car_id)
        if last_alert and (now - last_alert).total_seconds() < SENTRY_COOLDOWN:
            logger.info(f"  Skipping sentry alert (cooldown active)")
            return
        
        # Build message
        if activated:
            message = f"🚨 <b>Sentry Mode ACTIVATED!</b>\n\n🛡️ <b>Vehicle:</b> {car_name}\n"
        else:
            message = f"✅ <b>Sentry Mode Deactivated</b>\n\n🔓 <b>Vehicle:</b> {car_name}\n"
        
        # Add location if available
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n"
            message += f"📸 <a href='https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}'>Street View</a>\n\n"
        
        locked = car.get('locked', 'unknown')
        if locked.lower() == 'true':
            message += f"🔒 <b>Locked:</b> Yes\n"
        elif locked.lower() == 'false':
            message += f"⚠️  <b>Locked:</b> NO - UNLOCKED!\n"
        
        windows = car.get('windows_open', 'unknown')
        if windows.lower() == 'true':
            message += f"⚠️  <b>Windows:</b> OPEN!\n"
        elif windows.lower() == 'false':
            message += f"✓ <b>Windows:</b> Closed\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        state = car.get('state', 'unknown')
        message += f"📊 <b>State:</b> {state.title()}\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if activated:
            message += f"\n\n<i>Your vehicle is now monitoring for security events.</i>"
        else:
            message += f"\n\n<i>Sentry monitoring has been disabled.</i>"
        
        if self.send_telegram(message, alert_type='sentry_mode', car_id=car_id):
            self.sentry_last_alert_times[car_id] = datetime.now()
            logger.info(f"✓ Sentry alert sent for car {car_id}")
    
    # ==================== SPEED MONITORING ====================
    
    def check_speed_violation(self, car_id):
        """Check if car is exceeding speed limit"""
        car = self.car_data[car_id]
        
        if 'speed' not in car or 'speed_limit' not in car:
            return
        
        # Skip if not driving
        if car.get('state') != 'driving':
            if car_id in self.speed_violation_start_times:
                del self.speed_violation_start_times[car_id]
                del self.speed_alert_sent[car_id]
            return
        
        try:
            current_speed = int(car['speed'])
            speed_limit = int(car['speed_limit'])
        except (ValueError, TypeError):
            return
        
        if speed_limit <= 0:
            return
        
        threshold = speed_limit * (1 + SPEED_THRESHOLD_PERCENT / 100)
        
        if current_speed > threshold:
            if car_id not in self.speed_violation_start_times:
                self.speed_violation_start_times[car_id] = datetime.now()
                self.speed_alert_sent[car_id] = False
                logger.info(f"⚠️  Car {car_id}: Speeding detected ({current_speed} km/h in {speed_limit} km/h zone)")
            
            violation_duration = (datetime.now() - self.speed_violation_start_times[car_id]).total_seconds()
            
            if (violation_duration >= SPEED_ALERT_DURATION and 
                not self.speed_alert_sent[car_id] and
                self.can_send_speed_alert(car_id)):
                
                self.send_speed_alert(car_id, current_speed, speed_limit, violation_duration)
                self.speed_alert_sent[car_id] = True
                
        else:
            if car_id in self.speed_violation_start_times:
                logger.info(f"✓ Car {car_id}: Speed back to normal ({current_speed} km/h)")
                del self.speed_violation_start_times[car_id]
                del self.speed_alert_sent[car_id]
    
    def can_send_speed_alert(self, car_id):
        """Check speed alert cooldown"""
        if car_id not in self.speed_last_alert_times:
            return True
        time_since = (datetime.now() - self.speed_last_alert_times[car_id]).total_seconds()
        return time_since >= SPEED_COOLDOWN
    
    def send_speed_alert(self, car_id, current_speed, speed_limit, duration):
        """Send Telegram alert for speed violation"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        overspeed = current_speed - speed_limit
        overspeed_percent = (overspeed / speed_limit) * 100
        
        heading = car.get('heading', 'N/A')
        
        def heading_to_direction(h):
            try:
                h = int(h)
                directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
                index = int((h + 22.5) / 45) % 8
                return directions[index]
            except:
                return 'N/A'
        
        direction = heading_to_direction(heading)
        
        message = f"🚨 <b>Speed Limit Violation!</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n"
        message += f"⚡ <b>Current Speed:</b> {current_speed} km/h\n"
        message += f"🚧 <b>Speed Limit:</b> {speed_limit} km/h\n"
        message += f"📈 <b>Over Limit:</b> +{overspeed} km/h ({overspeed_percent:.1f}%)\n"
        message += f"⏱️  <b>Duration:</b> {duration:.1f} seconds\n\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n"
            message += f"📸 <a href='https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}'>Street View</a>\n\n"
        
        message += f"🧭 <b>Heading:</b> {direction} ({heading}°)\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='speed_violation', car_id=car_id):
            self.speed_last_alert_times[car_id] = datetime.now()
            logger.info(f"✓ Speed alert sent for car {car_id} ({current_speed} km/h in {speed_limit} zone)")
    
    # ==================== TPMS MONITORING ====================
    
    def check_tire_pressure(self, car_id, tire_field):
        """Check if tire pressure is below threshold"""
        car = self.car_data[car_id]
        
        if tire_field not in car:
            return
        
        try:
            pressure = float(car[tire_field])
        except (ValueError, TypeError):
            return
        
        if pressure <= 0:
            return
        
        tire_name = TIRE_POSITIONS[tire_field]
        
        if pressure < TPMS_THRESHOLD_BAR:
            if tire_field not in self.tpms_low_pressure_tires[car_id]:
                self.tpms_low_pressure_tires[car_id][tire_field] = datetime.now()
                logger.info(f"⚠️  Car {car_id}: Low pressure in {tire_name} ({pressure:.1f} bar)")
            
            if self.can_send_tpms_alert(car_id, tire_field):
                self.send_tpms_alert(car_id, tire_field, pressure)
                
        else:
            if tire_field in self.tpms_low_pressure_tires[car_id]:
                logger.info(f"✓ Car {car_id}: {tire_name} pressure back to normal ({pressure:.1f} bar)")
                del self.tpms_low_pressure_tires[car_id][tire_field]
    
    def can_send_tpms_alert(self, car_id, tire_field):
        """Check TPMS alert cooldown"""
        if tire_field not in self.tpms_last_alert_times[car_id]:
            return True
        time_since = (datetime.now() - self.tpms_last_alert_times[car_id][tire_field]).total_seconds()
        return time_since >= TPMS_COOLDOWN
    
    def send_tpms_alert(self, car_id, tire_field, pressure):
        """Send Telegram alert for low tire pressure"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        tire_name = TIRE_POSITIONS[tire_field]
        
        # Get all tire pressures
        tire_pressures = {}
        for field, name in TIRE_POSITIONS.items():
            if field in car:
                try:
                    tire_pressures[name] = float(car[field])
                except (ValueError, TypeError):
                    tire_pressures[name] = None
        
        # Determine severity
        if pressure < 1.5:
            severity = "🔴 CRITICAL"
            emoji = "🚨"
        elif pressure < 1.8:
            severity = "🟠 WARNING"
            emoji = "⚠️"
        else:
            severity = "🟡 CAUTION"
            emoji = "⚠️"
        
        message = f"{emoji} <b>Low Tire Pressure Alert!</b>\n\n"
        message += f"{severity}\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n"
        
        state = car.get('state', 'N/A')
        message += f"🚦 <b>State:</b> {state.title()}\n\n"
        
        message += f"🛞 <b>Affected Tire:</b> {tire_name}\n"
        message += f"📉 <b>Current Pressure:</b> {pressure:.1f} bar\n"
        message += f"⚠️  <b>Below Threshold:</b> {TPMS_THRESHOLD_BAR:.1f} bar\n"
        message += f"📊 <b>Deficit:</b> {(TPMS_THRESHOLD_BAR - pressure):.1f} bar ({((TPMS_THRESHOLD_BAR - pressure) / TPMS_THRESHOLD_BAR * 100):.1f}%)\n\n"
        
        message += f"<b>All Tires:</b>\n"
        for name in ['Front Left', 'Front Right', 'Rear Left', 'Rear Right']:
            if name in tire_pressures and tire_pressures[name]:
                p = tire_pressures[name]
                status = "❌" if p < TPMS_THRESHOLD_BAR else "✅"
                message += f"  {status} {name}: {p:.1f} bar\n"
        
        message += f"\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n💡 <b>Recommendation:</b> Check and inflate tire to recommended pressure (typically 2.9-3.0 bar)\n\n"
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='tpms_low_pressure', car_id=car_id):
            self.tpms_last_alert_times[car_id][tire_field] = datetime.now()
            logger.info(f"✓ TPMS alert sent for car {car_id} ({tire_name}: {pressure:.1f} bar)")
    
    # ==================== DOORS/WINDOWS/TRUNK ====================
    
    def check_doors_windows(self, car_id):
        """Check if doors, windows, trunk, or frunk are open"""
        car = self.car_data[car_id]
        
        # Check if any are open
        doors_open = car.get('doors_open', '').lower() == 'true'
        windows_open = car.get('windows_open', '').lower() == 'true'
        trunk_open = car.get('trunk_open', '').lower() == 'true'
        frunk_open = car.get('frunk_open', '').lower() == 'true'
        
        state = car.get('state', '')
        plugged_in = self.plugged_in.get(car_id, False)
        
        # Skip alert if charging and only frunk open (common when plugging in)
        if (state == 'charging' or plugged_in) and frunk_open and not doors_open and not windows_open and not trunk_open:
            if car_id in self.doors_open_start_time:
                del self.doors_open_start_time[car_id]
                del self.doors_alert_sent[car_id]
            return
        
        any_open = doors_open or windows_open or trunk_open or frunk_open
        
        if any_open:
            # Start timer if not already started
            if car_id not in self.doors_open_start_time:
                self.doors_open_start_time[car_id] = datetime.now()
                self.doors_alert_sent[car_id] = False
                
                open_items = []
                if doors_open:
                    open_items.append("doors")
                if windows_open:
                    open_items.append("windows")
                if trunk_open:
                    open_items.append("trunk")
                if frunk_open:
                    open_items.append("frunk")
                
                logger.info(f"⚠️  Car {car_id}: {', '.join(open_items).title()} opened")
            
            # Check if should send alert
            duration = (datetime.now() - self.doors_open_start_time[car_id]).total_seconds()
            
            if (duration >= DOOR_ALERT_DELAY and 
                not self.doors_alert_sent[car_id] and
                self.can_send_doors_alert(car_id)):
                
                self.send_doors_alert(car_id, doors_open, windows_open, trunk_open, frunk_open, duration)
                self.doors_alert_sent[car_id] = True
        else:
            # Everything closed
            if car_id in self.doors_open_start_time:
                logger.info(f"✓ Car {car_id}: All doors/windows/trunk closed")
                del self.doors_open_start_time[car_id]
                del self.doors_alert_sent[car_id]
    
    def can_send_doors_alert(self, car_id):
        """Check door alert cooldown"""
        if car_id not in self.doors_last_alert_time:
            return True
        time_since = (datetime.now() - self.doors_last_alert_time[car_id]).total_seconds()
        return time_since >= DOOR_COOLDOWN

    def can_send_unlocked_alert(self, car_id):
        """Check unlocked-no-user alert cooldown (independent of the door/window cooldown)"""
        if car_id not in self.unlocked_no_user_last_alert_time:
            return True
        time_since = (datetime.now() - self.unlocked_no_user_last_alert_time[car_id]).total_seconds()
        return time_since >= UNLOCKED_NO_USER_COOLDOWN
    
    def send_doors_alert(self, car_id, doors, windows, trunk, frunk, duration):
        """Send Telegram alert for open doors/windows/trunk/frunk"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        # Build list of what's open
        open_items = []
        if doors:
            open_items.append("🚪 Doors")
        if windows:
            open_items.append("🪟 Windows")
        if trunk:
            open_items.append("🎒 Trunk")
        if frunk:
            open_items.append("📦 Frunk")
        
        message = f"⚠️  <b>Vehicle Left Open!</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n\n"
        
        message += f"<b>Open Items:</b>\n"
        for item in open_items:
            message += f"  {item}\n"
        
        message += f"\n⏱️  <b>Duration:</b> {int(duration)} seconds\n"
        
        state = car.get('state', 'N/A')
        message += f"🚦 <b>State:</b> {state.title()}\n"
        
        locked = car.get('locked', 'unknown')
        if locked.lower() == 'false':
            message += f"⚠️  <b>Locked:</b> NO - UNLOCKED!\n"
        elif locked.lower() == 'true':
            message += f"🔒 <b>Locked:</b> Yes\n"
        
        message += f"\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        inside_temp = car.get('inside_temp', 'N/A')
        outside_temp = car.get('outside_temp', 'N/A')
        if inside_temp != 'N/A' or outside_temp != 'N/A':
            message += f"🌡️  <b>Temperature:</b> "
            if inside_temp != 'N/A':
                message += f"Inside {inside_temp}°C"
            if outside_temp != 'N/A':
                message += f" / Outside {outside_temp}°C"
            message += "\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Reminder:</b> Close all doors, windows, and compartments to secure your vehicle."
        
        if self.send_telegram(message, alert_type='doors_open', car_id=car_id):
            self.doors_last_alert_time[car_id] = datetime.now()
            logger.info(f"✓ Door/window/trunk alert sent for car {car_id}")
    
    # ==================== LOCKED WITH USER PRESENT ====================
    
    def check_locked_with_user(self, car_id):
        """Check if car is locked but user is present (accidental lockout)"""
        car = self.car_data[car_id]
        
        locked = car.get('locked', '').lower() == 'true'
        user_present = car.get('is_user_present', '').lower() == 'true'
        state = car.get('state', '')
        shift_state = car.get('shift_state', '')
        
        # Don't alert if actively driving or in gear
        if state == 'driving':
            if car_id in self.locked_user_present_start_time:
                del self.locked_user_present_start_time[car_id]
                del self.locked_user_present_alert_sent[car_id]
            return
        if shift_state and shift_state not in ['P', '', None]:
            if car_id in self.locked_user_present_start_time:
                del self.locked_user_present_start_time[car_id]
                del self.locked_user_present_alert_sent[car_id]
            return
        
        # Only alert when parked/online/asleep
        if state not in ['parked', 'online', 'asleep']:
            if car_id in self.locked_user_present_start_time:
                del self.locked_user_present_start_time[car_id]
                del self.locked_user_present_alert_sent[car_id]
            return
        
        # Alert if car is locked and user is inside
        if locked and user_present:
            # Start timer if not already started
            if car_id not in self.locked_user_present_start_time:
                self.locked_user_present_start_time[car_id] = datetime.now()
                self.locked_user_present_alert_sent[car_id] = False
                logger.info(f"⚠️  Car {car_id}: Locked with user present - starting timer")
            
            # Check if should send alert
            duration = (datetime.now() - self.locked_user_present_start_time[car_id]).total_seconds()
            
            if (duration >= LOCKED_USER_PRESENT_DELAY and 
                not self.locked_user_present_alert_sent[car_id] and
                self.can_send_locked_user_alert(car_id)):
                
                self.send_locked_user_alert(car_id, duration)
                self.locked_user_present_alert_sent[car_id] = True
        else:
            # Car unlocked or user left
            if car_id in self.locked_user_present_start_time:
                logger.info(f"✓ Car {car_id}: Unlocked or user left")
                del self.locked_user_present_start_time[car_id]
                del self.locked_user_present_alert_sent[car_id]
    
    def can_send_locked_user_alert(self, car_id):
        """Check locked-with-user alert cooldown"""
        if car_id not in self.locked_user_present_last_alert:
            return True
        time_since = (datetime.now() - self.locked_user_present_last_alert[car_id]).total_seconds()
        return time_since >= LOCKED_USER_PRESENT_COOLDOWN
    
    def send_locked_user_alert(self, car_id, duration):
        """Send alert for locked car with user present"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        message = f"🔐 <b>Potential Lockout Alert!</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n\n"
        message += f"⚠️  <b>Status:</b> Vehicle is locked but user is detected inside\n"
        message += f"⏱️  <b>Duration:</b> {int(duration)} seconds ({int(duration/60)} minutes)\n\n"
        
        state = car.get('state', 'N/A')
        message += f"🚦 <b>State:</b> {state.title()}\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"\n📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"\n🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Note:</b> This might be a false positive if someone is near the car. Use the Tesla app to unlock if needed."
        
        if self.send_telegram(message, alert_type='locked_user', car_id=car_id):
            self.locked_user_present_last_alert[car_id] = datetime.now()
            logger.info(f"✓ Locked-with-user alert sent for car {car_id}")
    
    # ==================== UNLOCKED WITHOUT USER ====================
    
    def check_unlocked_no_user(self, car_id):
        """Check if car is unlocked and user has left"""
        car = self.car_data[car_id]
        
        locked = car.get('locked', '').lower() == 'true'
        user_present = car.get('is_user_present', '').lower() == 'true'
        state = car.get('state', '')
        
        # Only check when parked/online/asleep
        if state not in ['parked', 'online', 'asleep']:
            if car_id in self.unlocked_no_user_start_time:
                del self.unlocked_no_user_start_time[car_id]
                del self.unlocked_no_user_alert_sent[car_id]
            return
        
        # Alert condition: unlocked AND no user present
        if not locked and not user_present:
            # Start timer if not already started
            if car_id not in self.unlocked_no_user_start_time:
                self.unlocked_no_user_start_time[car_id] = datetime.now()
                self.unlocked_no_user_alert_sent[car_id] = False
                logger.info(f"⚠️  Car {car_id}: Unlocked with no user present")
            
            # Check if should send alert
            duration = (datetime.now() - self.unlocked_no_user_start_time[car_id]).total_seconds()
            
            if (duration >= UNLOCKED_NO_USER_DELAY and
                not self.unlocked_no_user_alert_sent[car_id] and
                self.can_send_unlocked_alert(car_id)):

                self.send_unlocked_no_user_alert(car_id, duration)
                self.unlocked_no_user_alert_sent[car_id] = True
        else:
            # Car locked or user returned
            if car_id in self.unlocked_no_user_start_time:
                logger.info(f"✓ Car {car_id}: Locked or user returned")
                del self.unlocked_no_user_start_time[car_id]
                del self.unlocked_no_user_alert_sent[car_id]
    
    def send_unlocked_no_user_alert(self, car_id, duration):
        """Send alert for unlocked car with no user present"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        message = f"🔓 <b>Vehicle Left Unlocked!</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n\n"
        message += f"⚠️  <b>Status:</b> Vehicle is unlocked and no user detected nearby\n"
        message += f"⏱️  <b>Duration:</b> {int(duration)} seconds ({int(duration/60)} minutes)\n\n"
        
        state = car.get('state', 'N/A')
        message += f"🚦 <b>State:</b> {state.title()}\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"\n📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"\n🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Reminder:</b> Lock your vehicle using the Tesla app to secure it."
        
        if self.send_telegram(message, alert_type='unlocked_no_user', car_id=car_id):
            self.unlocked_no_user_last_alert_time[car_id] = datetime.now()
            logger.info(f"✓ Unlocked-no-user alert sent for car {car_id}")
    
    # ==================== CAR STARTED ====================
    
    def check_car_started(self, car_id, heading_value):
        """Detect car starting by monitoring heading changes"""
        car = self.car_data[car_id]
        
        try:
            current_heading = float(heading_value)
        except (ValueError, TypeError):
            return
        
        # Track heading changes
        if car_id in self.last_heading:
            old_heading = self.last_heading[car_id]
            heading_change = abs(current_heading - old_heading)
            
            # Normalize for 360 degree wrap-around
            if heading_change > 180:
                heading_change = 360 - heading_change
            
            # If heading changed significantly (>5 degrees) and car was parked
            state = car.get('state', '')
            if heading_change > 5 and state in ['asleep', 'parked', 'online']:
                # Check cooldown
                if car_id in self.car_started_last_alert:
                    time_since = (datetime.now() - self.car_started_last_alert[car_id]).total_seconds()
                    if time_since < CAR_STARTED_COOLDOWN:
                        self.last_heading[car_id] = current_heading
                        return
                
                logger.info(f"🚗 Car {car_id}: Car started (heading change: {heading_change:.1f}°)")
                self.send_car_started_alert(car_id, old_heading, current_heading)
                self.car_started_last_alert[car_id] = datetime.now()
        
        self.last_heading[car_id] = current_heading
    
    def send_car_started_alert(self, car_id, old_heading, new_heading):
        """Send alert when car starts moving"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        def heading_to_direction(h):
            try:
                h = float(h)
                directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
                index = int((h + 22.5) / 45) % 8
                return directions[index]
            except:
                return 'N/A'
        
        message = f"🚗 <b>Vehicle Started!</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n\n"
        message += f"🧭 <b>Heading Changed:</b>\n"
        message += f"   From: {heading_to_direction(old_heading)} ({old_heading:.0f}°)\n"
        message += f"   To: {heading_to_direction(new_heading)} ({new_heading:.0f}°)\n\n"
        
        state = car.get('state', 'N/A')
        message += f"🚦 <b>State:</b> {state.title()}\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"\n📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"\n🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='car_started', car_id=car_id):
            logger.info(f"✓ Car started alert sent for car {car_id}")

    # ==================== PARKED WEATHER DIGEST ====================

    def check_car_parked(self, car_id, new_state):
        """Detect the driving -> parked transition and send a weather/AQI/UV digest"""
        old_state = self.last_car_state.get(car_id)
        self.last_car_state[car_id] = new_state

        if old_state != 'driving' or new_state == 'driving':
            return

        if car_id in self.parked_weather_last_alert:
            time_since = (datetime.now() - self.parked_weather_last_alert[car_id]).total_seconds()
            if time_since < PARKED_WEATHER_COOLDOWN:
                return

        geofence_name = self.car_data[car_id].get('geofence', '') or ''
        if geofence_name.strip().lower().startswith('home'):
            logger.info(f"🅿️  Car {car_id}: parked at home ({geofence_name}), skipping weather digest")
            return

        logger.info(f"🅿️  Car {car_id}: parked (state {old_state} -> {new_state})")
        self.send_parked_weather_alert(car_id)
        self.parked_weather_last_alert[car_id] = datetime.now()

    @staticmethod
    def aqi_category(us_aqi):
        if us_aqi is None:
            return 'N/A'
        if us_aqi <= 50:
            return 'Good'
        if us_aqi <= 100:
            return 'Moderate'
        if us_aqi <= 150:
            return 'Unhealthy (Sensitive)'
        if us_aqi <= 200:
            return 'Unhealthy'
        if us_aqi <= 300:
            return 'Very Unhealthy'
        return 'Hazardous'

    @staticmethod
    def uv_category(uv_index):
        if uv_index is None:
            return 'N/A'
        if uv_index < 3:
            return 'Low'
        if uv_index < 6:
            return 'Moderate'
        if uv_index < 8:
            return 'High'
        if uv_index < 11:
            return 'Very High'
        return 'Extreme'

    def send_parked_weather_alert(self, car_id):
        """Fetch current conditions + 8h forecast + AQI + UV for the car's
        current position (Open-Meteo, no API key) and send a Telegram digest"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')

        try:
            lat = float(car.get('latitude'))
            lon = float(car.get('longitude'))
        except (TypeError, ValueError):
            logger.warning(f"  ⚠ No known position for car {car_id}, skipping parked weather alert")
            return

        try:
            fc_resp = requests.get(FORECAST_URL, params={
                'latitude': lat, 'longitude': lon,
                'current': 'temperature_2m,weather_code,uv_index',
                'hourly': 'temperature_2m,weather_code,precipitation_probability',
                'forecast_days': 2,
                'timezone': 'auto',
            }, timeout=15)
            fc_resp.raise_for_status()
            fc_data = fc_resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"  ✗ Forecast fetch failed: {e}")
            fc_data = {}

        try:
            aq_resp = requests.get(AIR_QUALITY_URL, params={
                'latitude': lat, 'longitude': lon,
                'current': 'us_aqi,european_aqi',
            }, timeout=15)
            aq_resp.raise_for_status()
            aq_data = aq_resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"  ✗ Air quality fetch failed: {e}")
            aq_data = {}

        if not fc_data and not aq_data:
            logger.error("  ✗ Both weather and air quality fetches failed, skipping alert")
            return

        current = fc_data.get('current', {})
        hourly = fc_data.get('hourly', {})
        aq_current = aq_data.get('current', {})

        cur_temp = current.get('temperature_2m')
        cur_code = current.get('weather_code')
        cur_emoji = WEATHER_CODE_EMOJI.get(cur_code, '🌡️')
        uv_index = current.get('uv_index')
        us_aqi = aq_current.get('us_aqi')
        eu_aqi = aq_current.get('european_aqi')

        message = f"🅿️ <b>Vehicle Parked - Weather Update</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n"
        message += f"📍 <a href='https://www.google.com/maps?q={lat},{lon}'>View Location on Map</a>\n\n"

        if cur_temp is not None:
            message += f"{cur_emoji} <b>Current:</b> {cur_temp:.1f}°C\n"
        if uv_index is not None:
            message += f"☀️ <b>UV Index:</b> {uv_index:.1f} ({self.uv_category(uv_index)})\n"
        if us_aqi is not None:
            message += f"🌬️ <b>Air Quality:</b> {us_aqi} US AQI ({self.aqi_category(us_aqi)})"
            if eu_aqi is not None:
                message += f" / {eu_aqi} EU AQI"
            message += "\n"

        # Next 8 hours, starting from the current hour
        times = hourly.get('time', [])
        temps = hourly.get('temperature_2m', [])
        codes = hourly.get('weather_code', [])
        precip = hourly.get('precipitation_probability', [])

        # the forecast hours are in the car's local time (timezone=auto), so is this
        now_hour = self.local_stamp(car_id, '%Y-%m-%dT%H:00')
        try:
            start_idx = times.index(now_hour)
        except ValueError:
            start_idx = 0

        forecast_lines = []
        for i in range(start_idx, min(start_idx + 8, len(times))):
            hour_label = times[i][11:16]
            t = temps[i] if i < len(temps) else None
            c = codes[i] if i < len(codes) else None
            p = precip[i] if i < len(precip) else None
            emoji = WEATHER_CODE_EMOJI.get(c, '🌡️')
            line = f"  {hour_label}  {emoji} {t:.0f}°C" if t is not None else f"  {hour_label}  {emoji}"
            if p is not None and p >= 20:
                line += f"  ({p:.0f}% rain)"
            forecast_lines.append(line)

        if forecast_lines:
            message += f"\n⏳ <b>Next 8 Hours:</b>\n" + "\n".join(forecast_lines) + "\n"

        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"

        if self.send_telegram(message, alert_type='parked_weather', car_id=car_id):
            logger.info(f"✓ Parked weather alert sent for car {car_id}")

    # ==================== CABIN OVERHEAT ====================
    
    def check_cabin_overheat(self, car_id):
        """Check for dangerous cabin temperature (potential child/pet forgotten scenario)"""
        car = self.car_data[car_id]
        
        state = car.get('state', '')
        if state not in ['parked', 'asleep', 'online']:
            # Car is moving, clear monitoring
            if car_id in self.cabin_overheat_start_time:
                del self.cabin_overheat_start_time[car_id]
                del self.cabin_overheat_alert_sent[car_id]
            return
        
        # Track if car was recently driven (within last 30 minutes)
        if state == 'driving' or car.get('shift_state') not in ['', 'P', None]:
            self.recently_driven[car_id] = datetime.now()
        
        try:
            inside_temp = float(car.get('inside_temp', 0))
        except (ValueError, TypeError):
            return
        
        locked = car.get('locked', '').lower() == 'true'
        user_present = car.get('is_user_present', '').lower() == 'true'
        
        # Check if car was recently driven
        was_recently_driven = False
        if car_id in self.recently_driven:
            time_since_driven = (datetime.now() - self.recently_driven[car_id]).total_seconds()
            was_recently_driven = time_since_driven < 1800  # 30 minutes
        
        # Alert conditions: high temp + parked + locked + no user + recently driven
        dangerous_condition = (
            inside_temp > CABIN_TEMP_THRESHOLD and
            locked and
            not user_present and
            was_recently_driven
        )
        
        if dangerous_condition:
            # Start timer if not already started
            if car_id not in self.cabin_overheat_start_time:
                self.cabin_overheat_start_time[car_id] = datetime.now()
                self.cabin_overheat_alert_sent[car_id] = False
                logger.info(f"🔥 Car {car_id}: High cabin temperature detected ({inside_temp}°C)")
            
            # Check if should send alert
            duration = (datetime.now() - self.cabin_overheat_start_time[car_id]).total_seconds()
            
            if (duration >= CABIN_ALERT_DELAY and 
                not self.cabin_overheat_alert_sent[car_id] and
                self.can_send_cabin_alert(car_id)):
                
                self.send_cabin_overheat_alert(car_id, inside_temp, duration)
                self.cabin_overheat_alert_sent[car_id] = True
        else:
            # Conditions no longer met
            if car_id in self.cabin_overheat_start_time:
                logger.info(f"✓ Car {car_id}: Cabin temperature back to normal")
                del self.cabin_overheat_start_time[car_id]
                del self.cabin_overheat_alert_sent[car_id]
    
    def can_send_cabin_alert(self, car_id):
        """Check cabin overheat alert cooldown"""
        if car_id not in self.cabin_overheat_last_alert:
            return True
        time_since = (datetime.now() - self.cabin_overheat_last_alert[car_id]).total_seconds()
        return time_since >= CABIN_COOLDOWN
    
    def send_cabin_overheat_alert(self, car_id, inside_temp, duration):
        """Send critical alert for high cabin temperature"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        outside_temp = car.get('outside_temp', 'N/A')
        
        message = f"🚨🔥 <b>CRITICAL: High Cabin Temperature!</b>\n\n"
        message += f"⚠️  <b>POTENTIAL SAFETY HAZARD</b>\n\n"
        message += f"🚗 <b>Vehicle:</b> {car_name}\n\n"
        message += f"🌡️  <b>Inside Temperature:</b> {inside_temp:.1f}°C (DANGER!)\n"
        if outside_temp != 'N/A':
            message += f"🌡️  <b>Outside Temperature:</b> {outside_temp}°C\n"
        message += f"⏱️  <b>High Temp Duration:</b> {int(duration)} seconds\n\n"
        
        message += f"🚦 <b>State:</b> {car.get('state', 'N/A').title()}\n"
        message += f"🔒 <b>Locked:</b> Yes\n"
        message += f"👤 <b>User Present:</b> No\n"
        message += f"⚠️  <b>Recently Driven:</b> Yes (within 30 min)\n\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
            message += f"🗺️  <a href='https://www.google.com/maps?q={lat},{lon}'>View on Map</a>\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n\n"
        
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}\n\n"
        
        message += f"⚠️  <b>WARNING:</b> This alert is triggered when:\n"
        message += f"  • Cabin temperature exceeds {CABIN_TEMP_THRESHOLD}°C\n"
        message += f"  • Car is parked and locked\n"
        message += f"  • No user detected inside\n"
        message += f"  • Car was recently driven\n\n"
        
        message += f"💡 <b>Action Required:</b>\n"
        message += f"  • Check if anyone (child/pet) is in the vehicle\n"
        message += f"  • Verify Cabin Overheat Protection is enabled\n"
        message += f"  • Use Tesla app to check vehicle interior\n"
        message += f"  • Consider enabling climate control remotely\n\n"
        
        message += f"<i>Note: This is a precautionary alert. False positives may occur in hot weather. Always verify vehicle status.</i>"
        
        if self.send_telegram(message, alert_type='cabin_overheat', car_id=car_id):
            self.cabin_overheat_last_alert[car_id] = datetime.now()
            logger.info(f"✓ Cabin overheat alert sent for car {car_id} ({inside_temp}°C)")
    
    # ==================== CHARGING ALERTS ====================
    
    def handle_charging(self, car_id, topic, value):
        """Handle charging-related alerts"""
        car = self.car_data[car_id]
        
        # Handle plugged in/unplugged
        if 'plugged_in' in topic:
            plugged = value.lower() == 'true'
            if car_id in self.plugged_in:
                if plugged != self.plugged_in[car_id]:
                    if plugged:
                        # Capture battery level at plug-in time
                        try:
                            battery = int(car.get('battery_level', 0))
                            self.charge_start_battery[car_id] = battery
                        except (ValueError, TypeError):
                            self.charge_start_battery[car_id] = 0
                        
                        self.charge_start_time[car_id] = datetime.now()
                        # Reset charge limit alerts
                        self.charge_78_alerted[car_id] = False
                        self.charge_98_alerted[car_id] = False
                        self.send_charging_plugged_alert(car_id, True)
                    else:
                        self.send_charging_plugged_alert(car_id, False)
                        # Send charging summary
                        if car_id in self.charge_start_time:
                            self.send_charging_summary(car_id)
                        # Reset charge limit flags
                        self.charge_78_alerted[car_id] = False
                        self.charge_98_alerted[car_id] = False
            self.plugged_in[car_id] = plugged
        
        # Handle low power charging
        if 'charger_power' in topic:
            try:
                power = float(value)
                plugged = self.plugged_in.get(car_id, False)
                if plugged and power > 0 and power < LOW_POWER_THRESHOLD:
                    self.send_low_power_alert(car_id, power)
            except (ValueError, TypeError):
                pass
        
        # Handle charge limit alerts (78% and 98%)
        if 'battery_level' in topic:
            try:
                battery = int(value)
                plugged = self.plugged_in.get(car_id, False)
                
                if plugged:
                    # Alert at 78%
                    if battery >= CHARGE_LIMIT_78_THRESHOLD and not self.charge_78_alerted.get(car_id, False):
                        self.send_charge_limit_alert(car_id, 78, battery)
                        self.charge_78_alerted[car_id] = True
                    
                    # Alert at 98%
                    if battery >= CHARGE_LIMIT_98_THRESHOLD and not self.charge_98_alerted.get(car_id, False):
                        self.send_charge_limit_alert(car_id, 98, battery)
                        self.charge_98_alerted[car_id] = True
            except (ValueError, TypeError):
                pass
    
    def send_charging_plugged_alert(self, car_id, plugged_in):
        """Alert when car is plugged in or unplugged"""
        if not self.can_send_charging_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        if plugged_in:
            message = f"🔌 <b>Charging Started</b> - {car_name}\n\n"
            message += f"✅ Vehicle has been plugged in\n\n"
        else:
            message = f"🔌 <b>Unplugged</b> - {car_name}\n\n"
            message += f"❌ Vehicle has been unplugged\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='charging', car_id=car_id):
            self.charging_last_alert[car_id] = datetime.now()
            logger.info(f"✓ Charging {'plugged' if plugged_in else 'unplugged'} alert sent for car {car_id}")
    
    def send_charging_summary(self, car_id):
        """Send summary when charging session ends"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        duration = (datetime.now() - self.charge_start_time[car_id]).total_seconds() / 3600
        start_battery = self.charge_start_battery.get(car_id, 0)
        current_battery = int(car.get('battery_level', 0))
        added = current_battery - start_battery
        
        message = f"⚡ <b>Charging Session Complete</b> - {car_name}\n\n"
        message += f"🔋 <b>Battery:</b> {start_battery}% → {current_battery}% (+{added}%)\n"
        message += f"⏱️  <b>Duration:</b> {duration:.1f} hours\n"
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='charging_summary', car_id=car_id):
            logger.info(f"✓ Charging summary sent for car {car_id}")
    
    def send_low_power_alert(self, car_id, power):
        """Alert when charging at very low power"""
        if not self.can_send_charging_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        message = f"⚠️  <b>Low Power Charging</b> - {car_name}\n\n"
        message += f"🔌 <b>Charging Power:</b> {power:.1f} kW\n\n"
        message += f"⚠️  Vehicle is charging at very low power (< {LOW_POWER_THRESHOLD} kW)\n"
        message += f"This may indicate:\n"
        message += f"  • Weak power supply\n"
        message += f"  • Damaged cable or outlet\n"
        message += f"  • Charging limited by vehicle\n\n"
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='low_power', car_id=car_id):
            self.charging_last_alert[car_id] = datetime.now()
            logger.info(f"✓ Low power charging alert sent for car {car_id} ({power} kW)")
    
    def can_send_charging_alert(self, car_id):
        """Check if enough time has passed since last charging alert"""
        if car_id not in self.charging_last_alert:
            return True
        time_since = (datetime.now() - self.charging_last_alert[car_id]).total_seconds()
        return time_since >= CHARGING_COOLDOWN
    
    def send_charge_limit_alert(self, car_id, limit_threshold, actual_battery):
        """Alert when charge reaches specific threshold (78% or 98%)"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        if limit_threshold == 78:
            emoji = "🔋"
            title = "Charge Limit Reached (78%)"
            note = "Your vehicle has reached the daily charge limit of 78%. This is optimal for battery longevity."
        else:  # 98%
            emoji = "⚡"
            title = "Nearly Full Charge (98%)"
            note = "Your vehicle is nearly fully charged at 98%. Consider unplugging soon to preserve battery health."
        
        message = f"{emoji} <b>{title}</b> - {car_name}\n\n"
        message += f"🔋 <b>Current Battery:</b> {actual_battery}%\n"
        
        # Calculate charge session info if available
        if car_id in self.charge_start_battery and car_id in self.charge_start_time:
            start_battery = self.charge_start_battery[car_id]
            duration = (datetime.now() - self.charge_start_time[car_id]).total_seconds() / 3600
            added = actual_battery - start_battery
            
            message += f"📈 <b>Added:</b> +{added}% (from {start_battery}%)\n"
            message += f"⏱️  <b>Charge Time:</b> {duration:.1f} hours\n"
        
        message += f"\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Note:</b> {note}"
        
        if self.send_telegram(message, alert_type='charge_limit', car_id=car_id):
            logger.info(f"✓ Charge limit alert ({limit_threshold}%) sent for car {car_id}")
    
    # ==================== SOFTWARE UPDATE ALERTS ====================
    
    def handle_update(self, car_id, topic, value):
        """Handle software update alerts"""
        car = self.car_data[car_id]
        
        # Handle update available
        if 'update_available' in topic:
            available = value.lower() == 'true'
            if available and car_id not in self.update_available_alerted:
                self.send_update_available_alert(car_id)
                self.update_available_alerted[car_id] = True
            elif not available and car_id in self.update_available_alerted:
                del self.update_available_alerted[car_id]
        
        # Handle update version tracking
        elif 'update_version' in topic:
            if value and value.lower() not in ['', 'none', 'null']:
                self.update_version_tracking[car_id] = value
        
        # Handle update status (installing/installed)
        elif 'update_status' in topic:
            status = value.lower()

            # Safety net: if we've been "installing" longer than any real Tesla
            # update takes, the update was likely cancelled/stalled on a status
            # value we don't recognize. Clear the flag so future installs aren't
            # suppressed forever, instead of waiting indefinitely for a status
            # we explicitly handle below.
            if (self.update_installing.get(car_id, False) and
                    car_id in self.update_install_start_time):
                stuck_duration = (datetime.now() - self.update_install_start_time[car_id]).total_seconds()
                if stuck_duration >= UPDATE_INSTALL_MAX_DURATION:
                    logger.warning(f"⚠️  Car {car_id}: Update install flag stuck for "
                                    f"{stuck_duration/3600:.1f}h (status={status!r}) - clearing")
                    self.update_installing[car_id] = False
                    del self.update_install_start_time[car_id]

            # Update started installing
            if status in ['installing', 'in_progress'] and not self.update_installing.get(car_id, False):
                self.update_installing[car_id] = True
                self.update_install_start_time[car_id] = datetime.now()
                self.send_update_installing_alert(car_id)
                logger.info(f"📱 Car {car_id}: Software update installation started")

            # Update completed
            elif status in ['available', 'installed', 'done', ''] and self.update_installing.get(car_id, False):
                # Check if this is completion (not just available again)
                if car_id in self.update_install_start_time:
                    self.send_update_completed_alert(car_id)
                    logger.info(f"✓ Car {car_id}: Software update installation completed")

                self.update_installing[car_id] = False
                if car_id in self.update_install_start_time:
                    del self.update_install_start_time[car_id]
    
    def send_update_available_alert(self, car_id):
        """Alert when software update is available"""
        if not self.can_send_update_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        version = car.get('update_version', 'Unknown')
        
        message = f"📱 <b>Software Update Available</b> - {car_name}\n\n"
        message += f"🆕 <b>New Version:</b> {version}\n\n"
        message += f"A new software update is available for your vehicle.\n"
        message += f"You can install it from the Tesla app or vehicle touchscreen.\n\n"
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='update_available', car_id=car_id):
            self.update_status_last[car_id] = datetime.now()
            logger.info(f"✓ Software update available alert sent for car {car_id}")
    
    def send_update_installing_alert(self, car_id):
        """Alert when software update installation starts"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        version = self.update_version_tracking.get(car_id, car.get('update_version', 'Unknown'))
        
        message = f"⚙️ <b>Software Update Installing</b> - {car_name}\n\n"
        message += f"📥 <b>Installing Version:</b> {version}\n\n"
        message += f"🔄 Your vehicle is now installing the software update.\n"
        message += f"This may take 20-45 minutes.\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"\n📍 <b>Location:</b> {lat}, {lon}\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Note:</b> The vehicle will be unavailable during installation."
        
        if self.send_telegram(message, alert_type='update_installing', car_id=car_id):
            logger.info(f"✓ Software update installing alert sent for car {car_id}")
    
    def send_update_completed_alert(self, car_id):
        """Alert when software update installation completes"""
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        version = self.update_version_tracking.get(car_id, car.get('update_version', 'Unknown'))
        
        # Calculate installation duration
        duration_str = "Unknown"
        if car_id in self.update_install_start_time:
            duration = (datetime.now() - self.update_install_start_time[car_id]).total_seconds()
            duration_min = int(duration / 60)
            duration_str = f"{duration_min} minutes"
        
        message = f"✅ <b>Software Update Complete!</b> - {car_name}\n\n"
        message += f"🎉 <b>Installed Version:</b> {version}\n"
        message += f"⏱️  <b>Installation Time:</b> {duration_str}\n\n"
        message += f"Your vehicle has successfully updated to the new software version.\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        message += f"\n\n💡 <b>Note:</b> Your vehicle is now ready to use with the latest features!"
        
        if self.send_telegram(message, alert_type='update_completed', car_id=car_id):
            logger.info(f"✓ Software update completed alert sent for car {car_id}")
    
    def send_update_alert(self, car_id):
        """Legacy method - redirect to new method"""
        self.send_update_available_alert(car_id)
    
    def can_send_update_alert(self, car_id):
        """Check if enough time has passed since last update alert"""
        if car_id not in self.update_status_last:
            return True
        time_since = (datetime.now() - self.update_status_last[car_id]).total_seconds()
        return time_since >= UPDATE_COOLDOWN
    
    # ==================== CLIMATE CONTROL ALERTS ====================
    
    def handle_climate(self, car_id, topic, value):
        """Handle climate control alerts"""
        car = self.car_data[car_id]
        
        if 'is_climate_on' in topic:
            climate_on = value.lower() == 'true'
            self.climate_on[car_id] = climate_on
            
            # Check if climate left on while parked
            state = car.get('state', '')
            if climate_on and state in ['parked', 'asleep']:
                self.check_climate_left_on(car_id)
        
        if 'climate_keeper_mode' in topic:
            if value and value != '0' and value.lower() != 'off':
                self.send_dog_mode_alert(car_id, value)
    
    def check_climate_left_on(self, car_id):
        """Alert if climate control is left on while parked"""
        if not self.can_send_climate_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        if car_id not in self.climate_left_on_alert_sent:
            message = f"❄️  <b>Climate Control Active</b> - {car_name}\n\n"
            message += f"⚠️  Climate control is on while vehicle is parked\n\n"
            
            battery = car.get('battery_level', 'N/A')
            if battery != 'N/A':
                message += f"🔋 <b>Battery:</b> {battery}%\n"
            
            inside_temp = car.get('inside_temp', 'N/A')
            if inside_temp != 'N/A':
                message += f"🌡️  <b>Inside Temp:</b> {inside_temp}°C\n"
            
            message += f"\n💡 This will drain battery. Turn off if not needed.\n"
            message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
            
            if self.send_telegram(message, alert_type='climate_left_on', car_id=car_id):
                self.climate_left_on_alert_sent[car_id] = datetime.now()
                logger.info(f"✓ Climate left on alert sent for car {car_id}")
    
    def send_dog_mode_alert(self, car_id, mode):
        """Alert when dog mode or similar is activated"""
        if not self.can_send_climate_alert(car_id):
            return
        
        if car_id in self.dog_mode_alerted:
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        message = f"🐕 <b>Climate Keeper Activated</b> - {car_name}\n\n"
        message += f"✅ Climate keeper mode: {mode}\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        message += f"\n💡 Keep an eye on battery level\n"
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='dog_mode', car_id=car_id):
            self.dog_mode_alerted[car_id] = datetime.now()
            logger.info(f"✓ Climate keeper alert sent for car {car_id}")
    
    def can_send_climate_alert(self, car_id):
        """Check if enough time has passed since last climate alert"""
        if car_id not in self.climate_left_on_alert_sent:
            return True
        time_since = (datetime.now() - self.climate_left_on_alert_sent[car_id]).total_seconds()
        return time_since >= CLIMATE_COOLDOWN
    
    # ==================== RANGE ALERTS ====================
    
    def handle_range(self, car_id, topic, value):
        """Handle range-based alerts"""
        if 'rated_battery_range_km' in topic:
            try:
                range_km = float(value)
                self.last_range[car_id] = range_km
                
                if range_km < LOW_RANGE_THRESHOLD_KM:
                    self.send_low_range_alert(car_id, range_km)
            except (ValueError, TypeError):
                pass
    
    def send_low_range_alert(self, car_id, range_km):
        """Alert when range is low"""
        if not self.can_send_range_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        message = f"⚡ <b>Low Range Warning</b> - {car_name}\n\n"
        message += f"⚠️  <b>Range:</b> {range_km:.1f} km\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        if 'latitude' in car and 'longitude' in car:
            lat, lon = car['latitude'], car['longitude']
            message += f"📍 <b>Location:</b> {lat}, {lon}\n"
        
        message += f"\n💡 Consider charging soon\n"
        message += f"🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='low_range', car_id=car_id):
            self.range_alert_sent[car_id] = datetime.now()
            logger.info(f"✓ Low range alert sent for car {car_id} ({range_km} km)")
    
    def can_send_range_alert(self, car_id):
        """Check if enough time has passed since last range alert"""
        if car_id not in self.range_alert_sent:
            return True
        time_since = (datetime.now() - self.range_alert_sent[car_id]).total_seconds()
        return time_since >= RANGE_COOLDOWN
    
    # ==================== GEOFENCE ALERTS ====================
    
    def handle_geofence(self, car_id, geofence_name):
        """Handle named geofence entry/exit"""
        if car_id in self.current_geofence:
            old_geofence = self.current_geofence[car_id]
            if old_geofence != geofence_name:
                # Geofence changed
                if geofence_name and geofence_name.lower() != 'nil':
                    self.send_geofence_alert(car_id, geofence_name, entered=True)
                elif old_geofence and old_geofence.lower() != 'nil':
                    self.send_geofence_alert(car_id, old_geofence, entered=False)
        
        self.current_geofence[car_id] = geofence_name
    
    def send_geofence_alert(self, car_id, location_name, entered=True):
        """Alert when entering or leaving a named location"""
        if not self.can_send_geofence_alert(car_id):
            return
        
        car = self.car_data[car_id]
        car_name = car.get('display_name', f'Car {car_id}')
        
        if entered:
            message = f"📍 <b>Arrived at {location_name}</b> - {car_name}\n\n"
            message += f"✅ Vehicle has entered {location_name}\n\n"
        else:
            message = f"📍 <b>Left {location_name}</b> - {car_name}\n\n"
            message += f"🚗 Vehicle has left {location_name}\n\n"
        
        battery = car.get('battery_level', 'N/A')
        if battery != 'N/A':
            message += f"🔋 <b>Battery:</b> {battery}%\n"
        
        odometer = car.get('odometer', 'N/A')
        if odometer != 'N/A':
            message += f"🛣️  <b>Odometer:</b> {odometer} km\n"
        
        message += f"\n🕐 <b>Time:</b> {self.local_stamp(car_id)}"
        
        if self.send_telegram(message, alert_type='geofence', car_id=car_id):
            self.geofence_last_alert[car_id] = datetime.now()
            logger.info(f"✓ Geofence alert sent for car {car_id} ({'entered' if entered else 'left'} {location_name})")
    
    def can_send_geofence_alert(self, car_id):
        """Check if enough time has passed since last geofence alert"""
        if car_id not in self.geofence_last_alert:
            return True
        time_since = (datetime.now() - self.geofence_last_alert[car_id]).total_seconds()
        return time_since >= GEOFENCE_COOLDOWN
    
    # ==================== TELEGRAM ====================
    
    def send_telegram_legacy(self, message, alert_type='general'):
        """Earlier sender (one ringing message per alert). Unused; ThreadsMixin.send_telegram routes now."""
        # Check rate limiting
        one_hour_ago = datetime.now() - timedelta(hours=1)
        self.alert_history = [a for a in self.alert_history if a > one_hour_ago]
        
        if len(self.alert_history) >= MAX_ALERTS_PER_HOUR:
            logger.warning(f"⚠️  Alert rate limit reached ({MAX_ALERTS_PER_HOUR}/hour), skipping alert")
            return False
        
        # Try sending with retries
        for attempt in range(TELEGRAM_MAX_RETRIES):
            try:
                url = f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                data = {
                    'chat_id': TELEGRAM_CHAT_ID,
                    'text': message,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': False
                }
                
                response = requests.post(url, data=data, timeout=10)
                
                if response.status_code == 200:
                    self.telegram_successes += 1
                    self.alert_counts[alert_type] += 1
                    self.alert_history.append(datetime.now())
                    return True
                else:
                    logger.error(f"✗ Telegram send failed (attempt {attempt + 1}/{TELEGRAM_MAX_RETRIES}): {response.status_code}")
                    if attempt < TELEGRAM_MAX_RETRIES - 1:
                        delay = TELEGRAM_RETRY_DELAY * (2 ** attempt)  # Exponential backoff
                        logger.info(f"  Retrying in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"  Response: {response.text}")
                        self.telegram_failures += 1
                        return False
                    
            except Exception as e:
                logger.error(f"✗ Error sending Telegram (attempt {attempt + 1}/{TELEGRAM_MAX_RETRIES}): {e}")
                if attempt < TELEGRAM_MAX_RETRIES - 1:
                    delay = TELEGRAM_RETRY_DELAY * (2 ** attempt)
                    logger.info(f"  Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.telegram_failures += 1
                    return False
        
        return False
    
    # ==================== MAIN LOOP ====================
    
    def run(self):
        """Start monitoring"""
        try:
            logger.info("=" * 60)
            logger.info("TeslaMate Unified Alert Monitor")
            logger.info("=" * 60)
            logger.info(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
            
            # Use 60s keepalive - sends PINGREQ every 60s to keep connection alive
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            
            # Start MQTT loop in background thread
            self.client.loop_start()
            
            # Log statistics every hour and save state
            last_stats_time = datetime.now()
            stats_interval = timedelta(hours=1)
            
            try:
                while True:
                    time.sleep(1)  # 1 s tick for the drive thread / parked incident
                    with self.state_lock:
                        self.threads_tick()
                    
                    # Log statistics and save state every hour
                    if datetime.now() - last_stats_time >= stats_interval:
                        self.log_statistics()
                        self.save_state()
                        last_stats_time = datetime.now()
                        
            except KeyboardInterrupt:
                logger.info("\n⚠️  Shutting down gracefully...")
                self.log_statistics()
                self.save_state()
                self.client.loop_stop()
                self.client.disconnect()
                if self.db_conn:
                    self.db_conn.close()
                sys.exit(0)
            
        except KeyboardInterrupt:
            logger.info("\n⚠️  Shutting down gracefully...")
            self.save_state()
            self.client.disconnect()
            if self.db_conn:
                self.db_conn.close()
            sys.exit(0)
        except Exception as e:
            logger.error(f"✗ Error: {e}")
            self.save_state()
            if self.db_conn:
                self.db_conn.close()
            sys.exit(1)


def test_telegram():
    """Test Telegram connection"""
    print("Testing Telegram connection...\n")
    
    message = """✅ <b>TeslaMate Unified Alert Test</b>

🛡️  <b>Sentry Mode Monitoring:</b> Active
🛞 <b>TPMS Monitoring:</b> Active (&lt; 2.0 bar)
🚨 <b>Speed Monitoring:</b> Active (&gt; 3% over limit)
🚪 <b>Doors/Windows/Trunk:</b> Active (&gt; 60s open)
🔓 <b>Unlocked Without User:</b> Active (&gt; 2 min)
🔐 <b>Locked with User Present:</b> Active (&gt; 2 min)
🔋 <b>Charge Limits:</b> Active (78% and 98%)
📱 <b>Software Updates:</b> Active (available/installing/completed)
🚗 <b>Car Started Detection:</b> Active
🔥 <b>Cabin Overheat:</b> Active (&gt; 40°C)

📍 <b>Test Location:</b> 52.3702, 4.8952
🗺️  <a href='https://www.google.com/maps?q=52.3702,4.8952'>View on Map</a>

🕐 <b>Time:</b> 2025-12-19 14:30:00

<i>All monitoring systems are operational. You will receive alerts for all configured events including charge limits and unlocked vehicle warnings.</i>"""
    
    try:
        url = f"{TELEGRAM_API_BASE}/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False
        }
        
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            print("✓ Test alert sent to Telegram successfully!")
            print(f"  Chat ID: {TELEGRAM_CHAT_ID}")
            print("\nCheck your Telegram to see the alert!")
            return True
        else:
            print(f"✗ Failed to send message: {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def test_mqtt():
    """Test MQTT connection"""
    print("Testing MQTT connection...\n")
    connected = False
    
    def on_connect_test(client, userdata, flags, rc, properties=None):
        nonlocal connected
        if rc == 0:
            print(f"✓ Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
            connected = True
            client.disconnect()
        else:
            print(f"✗ MQTT connection failed with code: {rc}")
            client.disconnect()
    
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if MQTT_USERNAME and MQTT_PASSWORD:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.on_connect = on_connect_test
        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()
        time.sleep(2)
        client.loop_stop()
        return connected
    except Exception as e:
        print(f"✗ MQTT Error: {e}")
        return False


def main():
    """Main entry point"""
    # Parse arguments with proper validation
    parser = argparse.ArgumentParser(
        description='TeslaMate Unified Alert System',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test Telegram connectivity'
    )
    parser.add_argument(
        '--test-mqtt',
        action='store_true',
        help='Test MQTT connectivity'
    )
    parser.add_argument(
        '--test-all',
        action='store_true',
        help='Test all connections'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show current monitoring statistics and exit'
    )
    parser.add_argument(
        '--no-restore',
        action='store_true',
        help='Start with fresh state (do not restore previous state)'
    )
    
    args = parser.parse_args()
    
    # Handle stats mode
    if args.stats:
        try:
            if not Path(STATE_FILE).exists():
                print("No state file found. The monitor has not been run yet.")
                sys.exit(1)
            
            with open(STATE_FILE, 'rb') as f:
                state = pickle.load(f)
            
            print("=" * 60)
            print("📊 MONITORING STATISTICS (from last run)")
            print("=" * 60)
            print(f"Last saved: {state.get('saved_at', 'unknown')}")
            print(f"Total alerts sent: {sum(state.get('alert_counts', {}).values())}")
            print(f"Telegram success rate: {round(100 * state.get('telegram_successes', 0) / max(1, state.get('telegram_successes', 0) + state.get('telegram_failures', 0)), 1)}%")
            print(f"Telegram successes: {state.get('telegram_successes', 0)}")
            print(f"Telegram failures: {state.get('telegram_failures', 0)}")
            
            if state.get('alert_counts'):
                print("\nAlert breakdown:")
                for alert_type, count in sorted(state['alert_counts'].items(), key=lambda x: x[1], reverse=True):
                    print(f"  {alert_type}: {count}")
            
            print("=" * 60)
            sys.exit(0)
        except Exception as e:
            print(f"Error reading statistics: {e}")
            sys.exit(1)
    
    # Handle test modes
    if args.test:
        sys.exit(0 if test_telegram() else 1)
    elif args.test_mqtt:
        sys.exit(0 if test_mqtt() else 1)
    elif args.test_all:
        mqtt_ok = test_mqtt()
        print()
        telegram_ok = test_telegram()
        sys.exit(0 if (mqtt_ok and telegram_ok) else 1)
    
    # Normal operation - set up monitoring
    restore_state = not args.no_restore
    monitor = UnifiedAlertMonitor(restore_state=restore_state)
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        signal_name = 'SIGTERM' if signum == signal.SIGTERM else 'SIGINT'
        logger.info(f"Received {signal_name}, shutting down gracefully...")
        
        # Log final statistics
        try:
            monitor.log_statistics()
        except Exception as e:
            logger.error(f"Error logging statistics: {e}")
        
        # Save state
        try:
            monitor.save_state()
        except Exception as e:
            logger.error(f"Error saving state: {e}")
        
        # Disconnect MQTT
        if monitor.client:
            try:
                monitor.client.loop_stop()
                monitor.client.disconnect()
                logger.info("✓ MQTT client disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting MQTT: {e}")
        
        # Close database connection
        if monitor.db_conn:
            try:
                monitor.db_conn.close()
                logger.info("✓ Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database: {e}")
        
        logger.info("Shutdown complete")
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Signal handlers registered (SIGTERM, SIGINT)")
    logger.info("Press Ctrl+C to shutdown gracefully")
    logger.info("")
    
    # Start monitoring
    monitor.run()


if __name__ == '__main__':
    main()

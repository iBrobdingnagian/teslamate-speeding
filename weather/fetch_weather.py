#!/usr/bin/env python3
"""
Fetch weather data for TeslaMate drives using the Open-Meteo Historical Weather API
Correlates drive positions with weather conditions

Open-Meteo (https://open-meteo.com) requires no API key and provides free,
non-commercial access to historical hourly weather by latitude/longitude/date.
"""

import os
import sys
import time
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
from pathlib import Path
from contextlib import closing

# Load .env file if it exists
def load_env_file():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent / '.env'

    if env_path.exists():
        print(f"Loading environment from: {env_path}")
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip().strip('"').strip("'")
                    os.environ.setdefault(key, value)
        print("✓ Environment loaded from .env file\n")
        return True
    return False

# Load .env before reading configuration
load_env_file()

# Configuration - try both naming conventions
DB_HOST = os.getenv('DB_HOST', os.getenv('DATABASE_HOST', 'localhost'))
DB_PORT = os.getenv('DB_PORT', os.getenv('DATABASE_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', os.getenv('DATABASE_NAME', 'teslamate'))
DB_USER = os.getenv('DB_USER', os.getenv('DATABASE_USER', 'teslamate'))
DB_PASS = os.getenv('DB_PASSWORD', os.getenv('DATABASE_PASS', 'teslamate'))

# Open-Meteo historical weather archive - no API key required
ARCHIVE_WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,"
    "surface_pressure,cloud_cover,wind_speed_10m,wind_direction_10m,"
    "precipitation,rain,snowfall,weather_code"
)

# WMO weather codes -> (main, description), matching the OpenWeatherMap-style
# categories the weather_data table was originally designed around.
WMO_CODES = {
    0: ("Clear", "clear sky"),
    1: ("Clouds", "mainly clear"),
    2: ("Clouds", "partly cloudy"),
    3: ("Clouds", "overcast"),
    45: ("Fog", "fog"),
    48: ("Fog", "depositing rime fog"),
    51: ("Drizzle", "light drizzle"),
    53: ("Drizzle", "moderate drizzle"),
    55: ("Drizzle", "dense drizzle"),
    56: ("Drizzle", "light freezing drizzle"),
    57: ("Drizzle", "dense freezing drizzle"),
    61: ("Rain", "slight rain"),
    63: ("Rain", "moderate rain"),
    65: ("Rain", "heavy rain"),
    66: ("Rain", "light freezing rain"),
    67: ("Rain", "heavy freezing rain"),
    71: ("Snow", "slight snow fall"),
    73: ("Snow", "moderate snow fall"),
    75: ("Snow", "heavy snow fall"),
    77: ("Snow", "snow grains"),
    80: ("Rain", "slight rain showers"),
    81: ("Rain", "moderate rain showers"),
    82: ("Rain", "violent rain showers"),
    85: ("Snow", "slight snow showers"),
    86: ("Snow", "heavy snow showers"),
    95: ("Thunderstorm", "thunderstorm"),
    96: ("Thunderstorm", "thunderstorm with slight hail"),
    99: ("Thunderstorm", "thunderstorm with heavy hail"),
}

def get_db_connection():
    """Connect to PostgreSQL database"""
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)

def fetch_historical_weather(lat, lon, when):
    """
    Fetch the hourly weather observation closest to `when` (a naive/UTC
    datetime) from Open-Meteo's free historical archive - no API key needed.
    """
    day = when.strftime('%Y-%m-%d')
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': day,
        'end_date': day,
        'hourly': HOURLY_VARS,
        'timezone': 'UTC',
    }

    try:
        response = requests.get(ARCHIVE_WEATHER_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather: {e}")
        return None

    hourly = data.get('hourly')
    if not hourly or 'time' not in hourly:
        return None

    times = hourly['time']
    target_hour = when.strftime('%Y-%m-%dT%H:00')
    try:
        idx = times.index(target_hour)
    except ValueError:
        # Fall back to the closest available hour that day
        idx = min(
            range(len(times)),
            key=lambda i: abs(
                datetime.strptime(times[i], '%Y-%m-%dT%H:%M') - when.replace(minute=0, second=0, microsecond=0)
            ),
        )

    def at(key):
        values = hourly.get(key)
        return values[idx] if values and idx < len(values) else None

    weather_code = at('weather_code')
    main, description = WMO_CODES.get(int(weather_code), ('Unknown', 'unknown')) if weather_code is not None else ('Unknown', 'unknown')

    return {
        'temp': at('temperature_2m'),
        'feels_like': at('apparent_temperature'),
        'humidity': at('relative_humidity_2m'),
        'pressure': at('surface_pressure'),
        'weather_main': main,
        'weather_description': description,
        'clouds': at('cloud_cover'),
        'wind_speed': at('wind_speed_10m'),
        'wind_deg': at('wind_direction_10m'),
        'rain_1h': at('rain'),
        'snow_1h': at('snowfall'),
    }

def insert_weather_data(conn, car_id, date, lat, lon, weather):
    """Insert weather data into database"""
    if not weather:
        return False

    try:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                INSERT INTO weather_data (
                    car_id, date, latitude, longitude,
                    temperature, feels_like, humidity, pressure,
                    weather_main, weather_description,
                    clouds, visibility, wind_speed, wind_deg,
                    rain_1h, snow_1h
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
            """, (
                car_id, date, lat, lon,
                weather.get('temp'), weather.get('feels_like'),
                weather.get('humidity'), weather.get('pressure'),
                weather.get('weather_main'), weather.get('weather_description'),
                weather.get('clouds'),
                None,  # visibility - not provided by Open-Meteo's historical archive
                weather.get('wind_speed'), weather.get('wind_deg'),
                weather.get('rain_1h', 0), weather.get('snow_1h', 0)
            ))

        conn.commit()
        return True
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
        return False

def backfill_weather_for_drives(days=7, limit=100, start_date=None, end_date=None):
    """
    Backfill weather data for recent drives
    Samples one position per drive to minimize API calls

    Args:
        days: Number of days to look back (ignored if start_date/end_date provided)
        limit: Maximum number of drives to process
        start_date: Optional start date (YYYY-MM-DD format or datetime)
        end_date: Optional end date (YYYY-MM-DD format or datetime)
    """
    conn = get_db_connection()
    try:
        # Parse start_date and end_date if provided
        start_dt = None
        if start_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d') if isinstance(start_date, str) else start_date

        end_dt = None
        if end_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d') if isinstance(end_date, str) else end_date

        with closing(conn.cursor()) as cursor:
            # Build query based on parameters
            if start_dt and end_dt:
                # Custom date range
                print(f"Fetching weather for date range: {start_dt.date()} to {end_dt.date()}")
                cursor.execute("""
                    SELECT
                        d.id,
                        d.car_id,
                        d.start_date,
                        p.latitude,
                        p.longitude
                    FROM drives d
                    INNER JOIN positions p ON d.start_position_id = p.id
                    WHERE d.start_date >= %s
                        AND d.start_date <= %s
                        AND NOT EXISTS (
                            SELECT 1 FROM weather_data w
                            WHERE w.car_id = d.car_id
                            AND w.date = d.start_date
                        )
                    ORDER BY d.start_date DESC
                    LIMIT %s
                """, (start_dt, end_dt, limit))
            else:
                # Get drives from last N days without weather data
                cutoff = datetime.now() - timedelta(days=days)
                print(f"Fetching weather for last {days} days (since {cutoff.date()})")
                cursor.execute("""
                    SELECT
                        d.id,
                        d.car_id,
                        d.start_date,
                        p.latitude,
                        p.longitude
                    FROM drives d
                    INNER JOIN positions p ON d.start_position_id = p.id
                    WHERE d.start_date >= %s
                        AND NOT EXISTS (
                            SELECT 1 FROM weather_data w
                            WHERE w.car_id = d.car_id
                            AND w.date = d.start_date
                        )
                    ORDER BY d.start_date DESC
                    LIMIT %s
                """, (cutoff, limit))

            drives = cursor.fetchall()

        if not drives:
            print("No drives to process. All recent drives already have weather data!")
            return

        print(f"Found {len(drives)} drives to process")
        print("Processing with a 1s delay between requests (Open-Meteo free tier)")
        print("-" * 60)

        success_count = 0
        fail_count = 0

        for idx, (drive_id, car_id, start_date, lat, lon) in enumerate(drives):
            print(f"[{idx+1}/{len(drives)}] Drive {drive_id} at {start_date.strftime('%Y-%m-%d %H:%M')}")
            print(f"  Location: {lat:.4f}, {lon:.4f}")

            # Open-Meteo expects a naive UTC datetime; TeslaMate stores drive
            # timestamps in UTC already.
            when = start_date if start_date.tzinfo is None else start_date.astimezone(timezone.utc).replace(tzinfo=None)
            weather = fetch_historical_weather(lat, lon, when)

            if weather:
                if insert_weather_data(conn, car_id, start_date, lat, lon, weather):
                    print(f"  ✓ Weather: {weather['weather_description']}, {weather['temp']}°C")
                    success_count += 1
                else:
                    print(f"  ✗ Failed to insert weather data")
                    fail_count += 1
            else:
                print(f"  ✗ Failed to fetch weather")
                fail_count += 1

            # Be polite to the free Open-Meteo service between requests
            if idx < len(drives) - 1:  # Don't sleep after last item
                time.sleep(1.0)

        print("-" * 60)
        print(f"✓ Successfully processed: {success_count}")
        print(f"✗ Failed: {fail_count}")
        print("Done!")
    finally:
        conn.close()

def show_weather_stats():
    """Show statistics about collected weather data"""
    conn = get_db_connection()
    try:
        with closing(conn.cursor()) as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT car_id) as cars,
                    MIN(date) as earliest,
                    MAX(date) as latest,
                    COUNT(DISTINCT weather_main) as conditions
                FROM weather_data
            """)

            result = cursor.fetchone()

            if result and result[0] > 0:
                print("Weather Data Statistics:")
                print(f"  Total records: {result[0]}")
                print(f"  Cars: {result[1]}")
                print(f"  Date range: {result[2]} to {result[3]}")
                print(f"  Unique conditions: {result[4]}")

                cursor.execute("""
                    SELECT weather_main, COUNT(*) as count
                    FROM weather_data
                    GROUP BY weather_main
                    ORDER BY count DESC
                    LIMIT 10
                """)

                print("\n  Top weather conditions:")
                for condition, count in cursor.fetchall():
                    print(f"    {condition}: {count}")
            else:
                print("No weather data collected yet.")
    finally:
        conn.close()

def main():
    """Main entry point"""
    print("TeslaMate Weather Data Fetcher (Open-Meteo)")
    print("=" * 60)

    # Parse command line arguments
    days = 7
    limit = 100
    start_date = None
    end_date = None

    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--stats':
            show_weather_stats()
            return
        elif sys.argv[i] == '--help':
            print("\nUsage: python3 fetch_weather.py [OPTIONS]")
            print("\nOptions:")
            print("  --stats              Show weather data statistics")
            print("  --days N             Number of days to backfill (default: 7)")
            print("  --limit N            Maximum drives to process (default: 100)")
            print("  --start-date DATE    Start date (YYYY-MM-DD format)")
            print("  --end-date DATE      End date (YYYY-MM-DD format)")
            print("\nExamples:")
            print("  # Fetch last 30 days")
            print("  python3 fetch_weather.py --days 30 --limit 50")
            print("")
            print("  # Fetch specific date range")
            print("  python3 fetch_weather.py --start-date 2025-06-13 --end-date 2025-12-18 --limit 200")
            print("\nEnvironment Variables (all optional, defaults shown):")
            print("  DATABASE_HOST        PostgreSQL host (default: localhost)")
            print("  DATABASE_PORT        PostgreSQL port (default: 5432)")
            print("  DATABASE_NAME        Database name (default: teslamate)")
            print("  DATABASE_USER        Database user (default: teslamate)")
            print("  DATABASE_PASS        Database password (default: secret)")
            print("\nNo API key is required - weather comes from the free Open-Meteo archive.")
            return
        elif sys.argv[i] == '--days' and i + 1 < len(sys.argv):
            days = int(sys.argv[i + 1])
            i += 1
        elif sys.argv[i] == '--limit' and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])
            i += 1
        elif sys.argv[i] == '--start-date' and i + 1 < len(sys.argv):
            start_date = sys.argv[i + 1]
            i += 1
        elif sys.argv[i] == '--end-date' and i + 1 < len(sys.argv):
            end_date = sys.argv[i + 1]
            i += 1
        i += 1

    print(f"\nConfiguration:")
    print(f"  Provider: Open-Meteo historical archive (no API key required)")
    print(f"  Database: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    if start_date and end_date:
        print(f"  Date range: {start_date} to {end_date}")
    else:
        print(f"  Backfill period: Last {days} days")

    print(f"  Limit: {limit} drives")
    print()

    # Backfill weather data
    backfill_weather_for_drives(days=days, limit=limit, start_date=start_date, end_date=end_date)

if __name__ == '__main__':
    main()

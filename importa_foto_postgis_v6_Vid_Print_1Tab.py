import os
import json
import psycopg2
from psycopg2.extras import execute_values
import subprocess
import re
from exif import Image

# Function to connect to PostgreSQL
def connect_to_postgis(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['user'],
        password=config['password']
    )
    print("Connected to PostgreSQL successfully.")
    return conn

# Function to extract GPS metadata from a photo
def extract_gps_metadata_from_photo(photo_path):
    try:
        with open(photo_path, 'rb') as photo_file:
            img = Image(photo_file)
            if img.has_exif and hasattr(img, 'gps_latitude') and hasattr(img, 'gps_longitude'):
                lat = img.gps_latitude
                lon = img.gps_longitude
                alt = getattr(img, 'gps_altitude', None)
                lat_sign = 1 if img.gps_latitude_ref == 'N' else -1
                lon_sign = 1 if img.gps_longitude_ref == 'E' else -1
                lat_dec = lat_sign * (lat[0] + lat[1] / 60 + lat[2] / 3600)
                lon_dec = lon_sign * (lon[0] + lon[1] / 60 + lon[2] / 3600)
                print(f"Photo metadata extracted: {photo_path} -> (Lat: {lat_dec}, Lon: {lon_dec}, Alt: {alt})")
                return lat_dec, lon_dec, alt
        print(f"No GPS metadata found for photo: {photo_path}")
        return None
    except Exception as e:
        print(f"Error extracting GPS metadata from photo {photo_path}: {e}")
        return None

# Function to extract GPS metadata from a video using ffprobe
def extract_gps_metadata_from_video(video_path):
    try:
        command = [
            'ffprobe', '-v', 'error',
            '-show_entries', 'format_tags',
            '-select_streams', 'v:0',
            '-of', 'json', video_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        metadata = json.loads(result.stdout)

        if 'tags' in metadata['format']:
            tags = metadata['format']['tags']
            if 'location' in tags:
                location = tags['location']
                match = re.match(r'([+-]\d+\.\d+)([+-]\d+\.\d+)', location)
                if match:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    alt = None
                    print(f"Video metadata extracted: {video_path} -> (Lat: {lat}, Lon: {lon}, Alt: {alt})")
                    return lat, lon, alt
        print(f"No GPS metadata found for video: {video_path}")
        return None
    except Exception as e:
        print(f"Error extracting GPS metadata from video {video_path}: {e}")
        return None

# Function to create a single unified table for all missions
def create_drone_mission_table(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS drone_mission (
            id SERIAL PRIMARY KEY,
            full_path VARCHAR(255) UNIQUE,
            file_name VARCHAR(255),
            media_type VARCHAR(10) NOT NULL,
            altitude NUMERIC,
            geom GEOMETRY(Point, 4326),
            mission_id INT NOT NULL REFERENCES missoes (id) ON DELETE CASCADE
        );
        """)
        conn.commit()
    print("Table 'drone_mission' created or already exists.")

# Function to process media files for a mission
def process_mission_media(conn, mission_name, mission_path):
    # Step 1: Retrieve the mission_id from the `missoes` table
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM missoes WHERE nome = %s;", (mission_name,))
        mission_id = cursor.fetchone()
        if mission_id is None:
            raise ValueError(f"Mission '{mission_name}' not found in the `missoes` table.")
        mission_id = mission_id[0]

    # Step 2: Process media files
    data = []
    for root, dirs, files in os.walk(mission_path):
        for file in files:
            full_path = os.path.join(root, file)
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):  # Process photos
                metadata = extract_gps_metadata_from_photo(full_path)
                if metadata:
                    lat, lon, alt = metadata
                    data.append((full_path, file, 'photo', lat, lon, alt, mission_id))
            elif file.lower().endswith(('.mp4', '.mov', '.avi', '.mkv')):  # Process videos
                metadata = extract_gps_metadata_from_video(full_path)
                if metadata:
                    lat, lon, alt = metadata
                    data.append((full_path, file, 'video', lat, lon, alt, mission_id))

    # Step 3: Insert processed data into the single drone_mission table
    if data:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO drone_mission (full_path, file_name, media_type, altitude, geom, mission_id)
            VALUES %s
            ON CONFLICT (full_path) DO NOTHING;
            """
            execute_values(cursor, query, [
                (
                    full_path, file_name, media_type, altitude,
                    f'SRID=4326;POINT({lon} {lat})', mission_id
                )
                for full_path, file_name, media_type, lat, lon, altitude, mission_id in data
            ])
        conn.commit()
    print(f"Processed media for mission '{mission_name}'. Files added: {len(data)}")

# Main function
def process_all(base_folder, config_path):
    conn = connect_to_postgis(config_path)

    # Step 1: Create the unified drone_mission table
    create_drone_mission_table(conn)

    # Step 2: Process each mission folder
    for mission in os.listdir(base_folder):
        mission_path = os.path.join(base_folder, mission)
        if os.path.isdir(mission_path) and mission.startswith("Mission"):
            print(f"Processing mission: {mission}")
            process_mission_media(conn, mission, mission_path)

    conn.close()
    print("Processing complete.")

if __name__ == "__main__":
    base_folder = "C:\\GeoSpacialDataBase"
    config_path = "config.json"
    process_all(base_folder, config_path)

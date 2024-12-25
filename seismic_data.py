'''
File name: seismic_data.py
Description: USGS coding exercise to analyze seismic data.
'''

import argparse
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import logging
import matplotlib.pyplot as plt
import obspy
import pathlib
import sqlite3
import sys

from matplotlib.dates import DateFormatter
from obspy.clients.fdsn import Client

def read_seismic_data(input_dir):
    '''
        Read and normalize seismic data from a directory of miniSEED files.
    '''
    streams = {}
    dir_path = pathlib.Path(input_dir)
    for file_path in dir_path.iterdir():
        if file_path.is_file() and file_path.suffix == '.mseed':
            logging.info(f'Read file: {file_path}')
            stream = obspy.read(str(file_path))

            # Normalize 
            stream.detrend(type='linear')
            stream.taper(max_percentage=0.05, type='hann')

            streams[file_path.name] = stream
            logging.info(f'Stream: {stream}')
    return streams

def insert_seismic_data_into_db(streams, db_file):
    '''
        Insert seismic data into a SQLite database.
    '''
    # Connect to a SQLite database (or create one)
    logging.info(f'Insert data into database: {db_file}')
    conn = sqlite3.connect(db_file, timeout=10)
    cursor = conn.cursor()

    # Create a table for seismic data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS seismic_data (
        id INTEGER PRIMARY KEY,
        network TEXT,
        station TEXT,
        location TEXT,
        channel TEXT,
        timestamp TEXT,
        amplitude REAL
    )
    ''')

    # Insert data into the table
    cursor.execute('PRAGMA synchronous = OFF;')
    cursor.execute('PRAGMA journal_mode = MEMORY;')
    for stream_name in sorted(streams.keys()):
        stream = streams[stream_name]
        for trace in stream:
            sql_data = []
            metadata = trace.stats
            trace_times = trace.times('timestamp')
            for i, value in enumerate(trace.data):
                sql_data.append((metadata.network, metadata.station, metadata.location, metadata.channel, trace_times[i], value))
            cursor.executemany('INSERT INTO seismic_data (network, station, location, channel, timestamp, amplitude) VALUES (?, ?, ?, ?, ?, ?)', sql_data)
            conn.commit()
            logging.info(f'Inserted stream {stream_name} trace {str(trace)}')

    # Check data
    sql_check = 'SELECT * FROM seismic_data limit 10;'
    cursor.execute(sql_check)
    rows = cursor.fetchall()
    logging.info('Check SQL data')
    logging.info(f'{sql_check}')
    logging.info(f'{rows}')

    conn.close()

def create_helicorder(streams, output):
    '''
        Create Helicorder-style charts of the seismic data.
    '''
    fig, axes = plt.subplots(nrows=len(streams), ncols=1, figsize=(15, 8))
    date_format = DateFormatter('%H:%M:%S.%f')
    for i, stream_name in enumerate(sorted(streams.keys())):
        stream = streams[stream_name]
        for trace in stream:
            axes[i].plot(trace.times('matplotlib'), trace.data, linewidth=0.5, color='blue')
        axes[i].set_xlabel('Time')
        axes[i].xaxis.set_major_formatter(date_format)
        axes[i].set_ylabel('Amplitude')
        axes[i].set_title(f'Helicorder Plot ({str(trace)})')
    plt.tight_layout()
    plt.savefig(output)
    logging.info(f'Save helicorder plot: {output}')
    plt.show()

def create_map(streams, output):
    '''
        Create a map with station locations.
    '''
    stations = set()
    for stream_name in sorted(streams.keys()):
        stream = streams[stream_name]
        for trace in stream:
            stations.add(trace.stats.station)
    other_stations = set({'HOA', 'SUG'})

    # Get station locations from IRIS
    client = Client('IRIS')
    station_info = {}
    for sta in stations.union(other_stations):
        try:
            inventory = client.get_stations(
                station=sta,
                level='station'
            )
            for network in inventory:
                for station in network.stations:
                    station_info[station.code] = {
                        'latitude': station.latitude,
                        'longitude': station.longitude
                    }
        except Exception as e:
            logging.exception(f'Error: {e}')

    logging.info(f'Station info: {station_info}')

    # Create map
    fig, ax = plt.subplots(
        figsize=(10, 8), 
        subplot_kw={'projection': ccrs.PlateCarree()}
    )
    ax.add_feature(cfeature.LAND, edgecolor='black')
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.RIVERS, edgecolor='blue')
    ax.add_feature(cfeature.LAKES, edgecolor='blue')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.STATES, linestyle='--')
    ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
    for station in station_info:
        lat = station_info[station]['latitude']
        lon = station_info[station]['longitude']
        label = station
        if station in stations:
            ax.plot(lon, lat, marker='^', color='blue', markersize=6, transform=ccrs.PlateCarree())
        else:
            ax.plot(lon, lat, marker='o', color='black', markersize=6, transform=ccrs.PlateCarree())
        ax.text(lon + 0.002, lat + 0.002, label, fontsize=8, transform=ccrs.PlateCarree())

    lats = list(map(lambda x: x['latitude'], station_info.values()))
    lons = list(map(lambda x: x['longitude'], station_info.values()))
    ax.set_extent([min(lons) - 0.1, max(lons) + 0.1, min(lats) - 0.1, max(lats) + 0.1],
                  crs=ccrs.PlateCarree())
    label1_marker, = ax.plot([], [], '^', color='blue', label='Stations found in data')
    label2_marker, = ax.plot([], [], 'o', color='black', label='Other stations')
    ax.legend(handles=[label1_marker, label2_marker], loc='upper left')

    plt.title('Station Locations')
    plt.savefig(output)
    logging.info(f'Save map: {output}')
    plt.show()

def main():
    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_dir', type=str, default='./SEP')
    parser.add_argument('--db_file', type=str, default='seismic_data.db')
    parser.add_argument('--helicorder_file', type=str, default='helicorder.png')
    parser.add_argument('--map_file', type=str, default='map.png')
    args = parser.parse_args()

    logging.info(f'Args: {args}')

    streams = read_seismic_data(args.input_dir)
    insert_seismic_data_into_db(streams, args.db_file)
    create_helicorder(streams, args.helicorder_file)
    create_map(streams, args.map_file)

if __name__ == '__main__':
    main()

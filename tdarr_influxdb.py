import sys
import subprocess
import pkg_resources
import json
import re
import os
import requests
import configparser
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

def install_missing_packages():
    """
    Install any missing Python packages required by the script.

    This function checks for missing packages and installs them using pip.

    Packages:
        sys, subprocess, pkg_resources

    Args:
        None

    Returns:
        None
    """
    required = {'requests', 'configparser', 'influxdb_client'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

def read_config():
    """
    Read the configuration file and return the parsed configuration.

    This function reads the config file and parses it's contents using configparser.
    It then returns the parsed config for use by other functions.

    Packages:
        os, configparser

    Args:
        None

    Returns:
        configparser.RawConfigParser: The parsed configuration object.
    """
    config = configparser.RawConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tdarr_influxdb.conf")
    with open(config_path) as f:
        config.read_file(f)
    return config

def scrape_tdarr_api(config):
    """
    Scrape the TDarr API and retrieve library statistics.

    This function sends a POST request to the TDarr API from the config file and returns it as a library.

    Packages:
        requests, json

    Args:
        config (configparser.RawConfigParser): The parsed configuration object.

    Returns:
        dict: The statistics data returned by the TDarr API.
    """
    url = config['Tdarr']['proto']+"://"+config['Tdarr']['hostname']+":"+config['Tdarr']['port']+"/api/v2/cruddb"
    payload = {"data": {
            "collection": "StatisticsJSONDB",
            "mode": "getAll",
            "docID": "statistics"
        }}
    headers = {"content-type": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers)
    except requests.exceptions.RequestException as e:
        print("Network error:", e)

    return json.loads(response.content)

def connect_influxdb(config):
    """
    Connect to the InfluxDB instance using the provided configuration.

    This function establishes a connection to the InfluxDB instance using the credentials from the config file.
    It then returns the information needed to write to InfluxDBv2 for the write function.

    Packages:
        sys, influxdb_client, influx_client.client.write_api > SYNCHRONOUS

    Args:
        config (configparser.RawConfigParser): The parsed configuration object.

    Returns:
        tuple: A tuple containing the InfluxDB write API object, the InfluxDB bucket, and the InfluxDB organization.
    """
    influx_bucket = config['Influx']['bucket']
    influx_org = config['Influx']['org']
    influx_token = config['Influx']['token']
    influx_url = config['Influx']['proto']+"://"+config['Influx']['hostname']+":"+config['Influx']['port']

    influx_client = influxdb_client.InfluxDBClient(
    url = influx_url,
    token = influx_token,
    org = influx_org
    )

    try:
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    except Exception as e:
        print("An error occurred:", e)
        sys.exit(1)
    return write_api, influx_bucket, influx_org

def parse_tdarr(tdarr_api):
    """
    Parse the TDarr API response and generate InfluxDB data points.

    This function takes the TDarr API response, extracts relevant data, and formats it into InfluxDB data points.
    It returns a list of InfluxDB data points to be written by the write function.

    Packages:
        re, influxdb_client

    Args:
        tdarr_api (dict): The TDarr API response containing the statistics data.

    Returns:
        list: A list of InfluxDB data points representing the parsed statistics.
    """
    table = re.compile('table\dCount')
    table_num = re.compile('\d')
    string_to_int = re.compile('[a-zA-Z\s]*')
    tables = ["Hold","Transcode_Queue","Transcode_Success","Transcode_Error","Health_Check_Queue","Health_Check_Healthy","Health_Check_Error"]
    libraries = ["FileCount","Transcodes","Space_Saved","Num_of_Health_Checks","Transcode_Status","Health_Check_Status","Count_by_Codec","Count_by_Container","Count_by_Quality"]
    field = ["Status","Status","Codec","Container","Quality"]
    field_status = ["Not_Required","Queued","Success","Hold","Error","Ignored"]

    record = []

    for item_key in tdarr_api[0]:
        if item_key not in set(["pies", "streamStats","languages","_id"]):
            if re.search(table,item_key):
                match = re.search(table_num,item_key)
                items = match.group()

                if type(tdarr_api[0][item_key]) == int or type(tdarr_api[0][item_key]) == float:
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"],tables[int(items)]).field("value", "{:f}".format(tdarr_api[0][item_key]))
                    record.append(influx_point)

            else:
                if type(tdarr_api[0][item_key]) == bool:
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).field("value", "{:f}".format(tdarr_api[0][item_key]))
                    record.append(influx_point)

                elif type(tdarr_api[0][item_key]) == int or type(tdarr_api[0][item_key]) == float:
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).field("value", "{:f}".format(tdarr_api[0][item_key]))
                    record.append(influx_point)

                elif tdarr_api[0][item_key] == "":
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).field("value", "{:f}".format(0))
                    record.append(influx_point)

                elif item_key != "DBLoadStatus":
                    strint = re.sub(string_to_int,"",tdarr_api[0][item_key])
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).field("value", "{:f}".format(float(strint)))
                    record.append(influx_point)

                else:
                    if tdarr_api[0][item_key] == "Stable":
                        status = 0

                    else:
                        status = 9999
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).field("value", "{:f}".format(status))
                    record.append(influx_point)

        elif item_key == "pies":
            library_key = 0
            while library_key < len(tdarr_api[0][item_key]):
                library_stat = 0
                while library_stat <= 3:
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[library_stat]).tag("library_name", tdarr_api[0][item_key][library_key][0]).tag("library_id", tdarr_api[0][item_key][library_key][1]).field("value", "{:f}".format(tdarr_api[0][item_key][library_key][library_stat+2]))
                    record.append(influx_point)

                    library_stat = library_stat + 1
                while library_stat < len(tdarr_api[0][item_key][library_key])-4:
                    status_key = 0
                    while status_key < len(tdarr_api[0][item_key][library_key][library_stat+2]):
                        no_space = re.sub("\s","_",tdarr_api[0][item_key][library_key][library_stat+2][status_key]["name"])
                        if library_stat < 1:
                            influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[library_stat]).tag("library_name", tdarr_api[0][item_key][library_key][0]).tag("library_id", tdarr_api[0][item_key][library_key][1]).tag(field[library_stat-4], field_status[status_key]).field("value", "{:f}".format(tdarr_api[0][item_key][library_key][library_stat+2][status_key]["value"]))
                            record.append(influx_point)

                        else:
                            influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[library_stat]).tag("library_name", tdarr_api[0][item_key][library_key][0]).tag("library_id", tdarr_api[0][item_key][library_key][1]).tag(field[library_stat-4], no_space).field("value", "{:f}".format(tdarr_api[0][item_key][library_key][library_stat+2][status_key]["value"]))
                            record.append(influx_point)

                        status_key = status_key + 1

                    library_stat = library_stat + 1

                library_key = library_key + 1

        elif item_key == "streamStats":
            for stream_stat in tdarr_api[0][item_key]:
                for value_key in tdarr_api[0][item_key][stream_stat]:
                    influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).tag("statistic", stream_stat).tag("range", value_key).field("value", "{:f}".format(tdarr_api[0][item_key][stream_stat][value_key]))
                    record.append(influx_point)

        elif item_key == "languages":
            for language_key in tdarr_api[0][item_key]:
                influx_point = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], item_key).tag("language", language_key).field("value", "{:f}".format(tdarr_api[0][item_key][language_key]["count"]))
                record.append(influx_point)

    return record

def write_influxdb(write_api, influx_bucket, influx_org, record):
    """
    Write the parsed Tdarr statistics to InfluxDB.

    This function writes the parsed statistics data to the InfluxDBv2 bucket and organization from the config using the provided write API object.
    It handles any exceptions that occur during the writing process.

    Packages:
        sys

    Args:
        write_api: The InfluxDB write API object.
        influx_bucket (str): The name of the InfluxDB bucket.
        influx_org (str): The name of the InfluxDB organization.
        record (list): A list of InfluxDB data points representing the parsed statistics.

    Returns:
        None
    """
    try:
        for influx_point in record:
            write_api.write(bucket=influx_bucket, org=influx_org, record=influx_point)
            print(influx_point)
    except Exception as e:
        print("An error occurred:", e)
        sys.exit(1)

def main():
    """
    Main entry point of the script.

    This function is the main entry point of the script. It executes the previously defined functions to:
        install missing packages, read the config, scrape the TDarr API, connect to InfluxDBv2, parse the Tdarr stats, and write it to InfluxDBv2.

    Args:
        None

    Returns:
        None
    """
    install_missing_packages()
    config = read_config()
    tdarr_api = scrape_tdarr_api(config)
    write_api, influx_bucket, influx_org = connect_influxdb(config)
    record = parse_tdarr(tdarr_api)
    write_influxdb(write_api, influx_bucket, influx_org, record)

if __name__ == "__main__":
    main()
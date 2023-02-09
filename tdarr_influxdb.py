import sys
import subprocess
import pkg_resources
import json
import time
import re

required = {'requests', 'configparser', 'influxdb_client'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

import requests
import configparser
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

ts = time.time_ns()

config = configparser.RawConfigParser()
config.read('tdarr_influxdb.conf')

url = config['Tdarr']['proto']+"://"+config['Tdarr']['hostname']+":"+config['Tdarr']['port']+"/api/v2/cruddb"
payload = {"data": {
        "collection": "StatisticsJSONDB",
        "mode": "getAll",
        "docID": "statistics"
    }}
headers = {"content-type": "application/json"}

response = requests.post(url, json=payload, headers=headers)
tdarr_api = json.loads(response.content)

influx_bucket = config['Influx']['bucket']
influx_org = config['Influx']['org']
influx_token = config['Influx']['token']
influx_url = config['Influx']['proto']+"://"+config['Influx']['hostname']+":"+config['Influx']['port']

influx_client = influxdb_client.InfluxDBClient(
   url = influx_url,
   token = influx_token,
   org = influx_org
)

write_api = influx_client.write_api(write_options=SYNCHRONOUS)

p = influxdb_client.Point("TESTING").tag("statistics","totalFileCount").field("value",10345)
write_api.write(bucket=influx_bucket, org=influx_org, record=p)

table = re.compile('table\dCount')
table_num = re.compile('\d')
string_to_int = re.compile('[a-zA-Z\s]*')
tables = ["Hold","Transcode_Queue","Transcode_Success","Transcode_Error","Health_Check_Queue","Health_Check_Healthy","Health_Check_Error"]
libraries = ["FileCount","Transcodes","Space_Saved","Num_of_Health_Checks","Transcode_Status","Health_Check_Status","Count_by_Codec","Count_by_Container","Count_by_Quality"]
field = ["Status","Status","Codec","Container","Quality"]
field_status = ["Not_Required","Queued","Success","Hold","Error","Ignored"]

for i in tdarr_api[0]:
    if i not in set(["pies", "streamStats","languages","_id"]):
        if re.search(table,i):
            match = re.search(table_num,i)
            items = match.group()
            if type(tdarr_api[0][i]) == int or type(tdarr_api[0][i]) == float:
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"],tables[int(items)]).field("value", "{:f}".format(tdarr_api[0][i]))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
        else:
            if type(tdarr_api[0][i]) == bool:
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).field("value", "{:f}".format(tdarr_api[0][i]))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
            elif type(tdarr_api[0][i]) == int or type(tdarr_api[0][i]) == float:
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).field("value", "{:f}".format(tdarr_api[0][i]))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
            elif tdarr_api[0][i] == "":
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).field("value", "{:f}".format(0))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
            elif i != "DBLoadStatus":
                strint = re.sub(string_to_int,"",tdarr_api[0][i])
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).field("value", "{:f}".format(float(strint)))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
            else:
                if tdarr_api[0][i] == "Stable":
                    status = 0
                else:
                    status = 9999
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).field("value", "{:f}".format(status))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
    elif i == "pies":
        j = 0
        while j < len(tdarr_api[0][i]):
            k = 0
            while k <= 3:
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[k]).tag("library_name", tdarr_api[0][i][j][0]).tag("library_id", tdarr_api[0][i][j][1]).field("value", "{:f}".format(tdarr_api[0][i][j][k+2]))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
                k = k + 1
            while k < len(tdarr_api[0][i][j])-4:
                l = 0
                while l < len(tdarr_api[0][i][j][k+2]):
                    no_space = re.sub("\s","_",tdarr_api[0][i][j][k+2][l]["name"])
                    if k < 1:
                        p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[k]).tag("library_name", tdarr_api[0][i][j][0]).tag("library_id", tdarr_api[0][i][j][1]).tag(field[k-4], field_status[l]).field("value", "{:f}".format(tdarr_api[0][i][j][k+2][l]["value"]))
                        write_api.write(bucket=influx_bucket, org=influx_org, record=p)
                    else:
                        p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], libraries[k]).tag("library_name", tdarr_api[0][i][j][0]).tag("library_id", tdarr_api[0][i][j][1]).tag(field[k-4], no_space).field("value", "{:f}".format(tdarr_api[0][i][j][k+2][l]["value"]))
                        write_api.write(bucket=influx_bucket, org=influx_org, record=p)
                    l = l + 1
                k = k + 1
            j = j + 1
    elif i == "streamStats":
        for m in tdarr_api[0][i]:
            for n in tdarr_api[0][i][m]:
                p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).tag("statistic", m).tag("range", n).field("value", "{:f}".format(tdarr_api[0][i][m][n]))
                write_api.write(bucket=influx_bucket, org=influx_org, record=p)
    elif i == "languages":
        for o in tdarr_api[0][i]:
            p = influxdb_client.Point("tdarr").tag(tdarr_api[0]["_id"], i).tag("language", o).field("value", "{:f}".format(tdarr_api[0][i][o]["count"]))
            write_api.write(bucket=influx_bucket, org=influx_org, record=p)
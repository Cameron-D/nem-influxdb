import urllib.request
import dateutil.parser
from influxdb import InfluxDBClient
from datetime import datetime
import json, time

INFLUX_HOST = "influxdb"

lastrun = datetime.now()
starttime = time.time()

while 1:
    inf = InfluxDBClient(host=INFLUX_HOST, port=8086, database="nem")
    out = []

    try:
        req = urllib.request.Request("https://ausrealtimefueltype.global-roam.com/api/SeriesSnapshot")
        res = urllib.request.urlopen(req, timeout=5)
        dat = res.read()
        j = json.loads(dat.decode("utf-8"))

        for point in j["seriesCollection"]:
            reading = {
                "measurement": "watts",
                "tags": {
                    "region": point["metadata"]["region"]["id"],
                    "fuelType": ''.join([i for i in point["metadata"]["fuelType"]["id"] if i.isalpha()])
                },
                "fields": { point["metadata"]["discriminator"]: point["value"] },
                "timestamp": dateutil.parser.parse(j["timeStamp"]).timestamp()
            }
            out.append(reading)

    except Exception as ex:
        print("Collection Error:")
        print(ex)

    try:
        inf.write_points(out, time_precision='m')
        print(out)

    except Exception as ex:
        print("Storage Error:")
        print(ex)

    time.sleep(300.0 - ((time.time() - starttime) % 300.0))

from influxdb import InfluxDBClient
from datetime import datetime
from urllib.request import urlopen
from io import BytesIO, StringIO
from zipfile import ZipFile
import time, re, csv, dateutil.parser, traceback

INFLUX_HOST = ""

starttime = time.time()

SCADA_URL = 'http://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/'
SOLAR_URL = 'http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/'

def load_generator_info():
    generators = []
    with open('generators.csv') as gen_file:
        gen_csv = csv.reader(gen_file)
        generator = map(map_generator_info, gen_csv)
        generators = list(generator)
    return generators


def map_generator_info(row):
    # In:  0:name, 1:region, 2:fuel, 3:id
    # Out: id, region, fuel
    fuel = re.sub(r'[\W]+', '', row[2])
    fuel = fuel if len(fuel) > 0 else "Unknown"
    return (tidy_gen_name(row[3]), row[1], fuel)


def tidy_gen_name(generator):
    return re.sub(r'[\W_]+', '_', generator)


def is_scada_reading(row):
    return row[:3] == ["D", "DISPATCH", "UNIT_SCADA"]


def is_solar_reading(row):
    return row[:3] == ["D", "ROOFTOP", "ACTUAL"] and row[5][-1] == "1"


def find_generator_details(gen_id):
    for generator in GENLIST:
        if generator[0] == gen_id:
            return generator
    return (id, "Unknown", "Unknown")


def map_scada_reading(row):
    generator = tidy_gen_name(row[5])
    generator_details = find_generator_details(generator)
    return "scada,region=%s,fuelType=%s,generator=%s reading=%f %d" % (generator_details[1], generator_details[2],
                                                    generator, float(row[6]), dateutil.parser.parse(row[4]).timestamp())


def map_solar_reading(row):
    return "scada,region=%s,fuelType=Solar,generator=ROOFTOPPV reading=%f %d" % (
                row[5], float(row[6]), dateutil.parser.parse(row[4]).timestamp())


def process_scada_zip(zipfile):
    scada_data = []

    for file in zipfile.namelist():
        scada_file = zipfile.open(file)
        scada_txt = StringIO(scada_file.read().decode("utf-8"))
        scada_csv = csv.reader(scada_txt)

        scada_readings = filter(is_scada_reading, scada_csv)
        scada_data += map(map_scada_reading, scada_readings)

        return list(scada_data)


def process_solar_zip(zipfile):
    try:
        solar_data = []

        for file in zipfile.namelist():
            solar_file = zipfile.open(file)
            solar_txt = StringIO(solar_file.read().decode("utf-8"))
            solar_csv = csv.reader(solar_txt)

            solar_readings = list(filter(is_solar_reading, solar_csv))

            for reading in solar_readings:
                old_ts = dateutil.parser.parse(reading[4]).timestamp()

                for i in range(0,6):
                    new_ts = old_ts + i*300
                    str_ts = datetime.fromtimestamp(new_ts).strftime("%Y/%m/%d %H:%M:00")
                    reading[4] = str_ts
                    data_list = list(map(map_solar_reading, [reading]))
                    solar_data += data_list

        return list(solar_data)

    except Exception as ex:
        print(ex)
        return [] 


def fetch_zip_bytes(zip_url):
    zipbytes = urlopen(zip_url).read()
    time.sleep(1) # don't flood AEMO with requests
    return ZipFile(BytesIO(zipbytes))


def load_scada_zip(zip_url):
    zipfile = fetch_zip_bytes(SCADA_URL + zip_url)
    return process_scada_zip(zipfile)


def load_solar_zip(zip_url):
    zipfile = fetch_zip_bytes(SOLAR_URL + zip_url)
    return process_solar_zip(zipfile)

GENLIST = load_generator_info()
last_solar = ""
last_pv_out = []

while 1:
    inf = InfluxDBClient(host=INFLUX_HOST, port=8086, database="nem")

    try:
        scada_page = urlopen(SCADA_URL).read().decode("utf-8")
        scada_links = re.findall(r'\/PUBLIC_DISPATCHSCADA_\d+_\d+\.zip', scada_page)
        scada_out = load_scada_zip(scada_links[-1][1:])
        print(datetime.now(), "SCADA")
        inf.write(scada_out,{'db':"nem","precision":"s"},protocol="line")

        solar_page = urlopen(SOLAR_URL).read().decode("utf-8")
        solar_links = re.findall(r'\/PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_\d+_\d+\.zip', solar_page)
        if solar_links[-1] != last_solar:
            pv_out = load_solar_zip(solar_links[-1][1:])
            print(datetime.now(), "Solar")
            inf.write(pv_out,{'db':"nem","precision":"s"},protocol="line")
            last_solar = solar_links[-1]

    except Exception as ex:
        print("Collection Error:")
        print(ex)
        traceback.print_exc()
    
    sleep_time = 60.0 - ((time.time() - starttime) % 60.0)
    time.sleep(sleep_time)

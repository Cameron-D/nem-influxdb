from influxdb import InfluxDBClient
from datetime import datetime
from urllib.request import urlopen
from io import BytesIO, StringIO
from zipfile import ZipFile
import time, re, csv, dateutil.parser, math

INFLUX_HOST = "influxdb"

lastrun = datetime.now()
starttime = time.time()

SCADA_CURRENT = 'https://nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/'
SOLAR_CURRENT = 'https://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/'
SCADA_ARCHIVE = 'https://nemweb.com.au/Reports/Archive/Dispatch_SCADA/'
SOLAR_ARCHIVE = 'https://nemweb.com.au/Reports/Archive/ROOFTOP_PV/ACTUAL/'

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
        print(file)
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

                solar_data += extrapolate(reading, old_ts, 6, True)

        return list(solar_data)

    except Exception as ex:
        print(ex)
        return [] 

def extrapolate(reading, old_ts, intervals,solar=False):
    extrapolated_data = []
    for i in range(0, intervals):
        new_ts = old_ts + i*300
        str_ts = datetime.fromtimestamp(new_ts).strftime("%Y/%m/%d %H:%M:00")
        reading[4] = str_ts
        data_list = []
        if solar:
            data_list = list(map(map_solar_reading, [reading]))
        else:
            data_list = list(map(map_scada_reading, [reading]))
        extrapolated_data += data_list
    return extrapolated_data


def fetch_zip_bytes(zip_url):
    zipbytes = urlopen(zip_url).read()
    time.sleep(1) # don't flood AEMO with requests
    return ZipFile(BytesIO(zipbytes))


def load_scada_zip(zip_url):
    zipfile = fetch_zip_bytes(SCADA_CURRENT + zip_url)
    return process_scada_zip(zipfile)


def load_solar_zip(zip_url):
    zipfile = fetch_zip_bytes(SOLAR_CURRENT + zip_url)
    return process_solar_zip(zipfile)


def process_scada_current(inf):
    report_page = urlopen(SCADA_CURRENT).read().decode("utf-8")
    report_links = re.findall(r'\/PUBLIC_DISPATCHSCADA_\d+_\d+\.zip', report_page)

    for link in report_links:
        print(link)
        out = load_scada_zip(link[1:])
        inf.write(out,{'db':"nem","precision":"s"},protocol="line")


def process_solar_current(inf):
    report_page = urlopen(SOLAR_CURRENT).read().decode("utf-8")
    report_links = re.findall(r'\/PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_\d+_\d+\.zip', report_page)

    for link in report_links:
        print(link)
        out = load_solar_zip(link[1:])
        print(out)
        inf.write(out,{'db':"nem","precision":"s"},protocol="line")


def process_scada_archive(inf):
    report_page = urlopen(SCADA_ARCHIVE).read().decode("utf-8")
    report_links = re.findall(r'\/PUBLIC_DISPATCHSCADA_\d+\.zip', report_page)

    for link in report_links:
        print(link)
        zip_of_zips = fetch_zip_bytes(SCADA_ARCHIVE + link[1:])

        for file in zip_of_zips.namelist():
            print(file)
            zipped_zip = zip_of_zips.open(file)
            csv_zip = ZipFile(BytesIO(zipped_zip.read()))
            out = process_scada_zip(csv_zip)
            inf.write(out,{'db':"nem","precision":"s"},protocol="line")


def process_solar_archive(inf):
    report_page = urlopen(SOLAR_ARCHIVE).read().decode("utf-8")
    report_links = re.findall(r'\/PUBLIC_ROOFTOP_PV_ACTUAL_\d+\.zip', report_page)

    for link in report_links:
        print(link)
        zip_of_zips = fetch_zip_bytes(SOLAR_ARCHIVE + link[1:])

        for file in zip_of_zips.namelist():
            print(file)
            zipped_zip = zip_of_zips.open(file)
            csv_zip = ZipFile(BytesIO(zipped_zip.read()))
            out = process_solar_zip(csv_zip)
            inf.write(out,{'db':"nem","precision":"s"},protocol="line")


def process_scada_historic(inf):
    for y in range(2018, datetime.now().year+1):
        for m in range(1,13): # excludes last item.....
                baseurl = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/%d/MMSDM_%d_%02d/MMSDM_Historical_Data_SQLLoader/DATA/'
                baseurl = baseurl % (y, y, m)
                report_page = urlopen(baseurl).read().decode("utf-8")
                report_links = re.findall(r'\/PUBLIC_DVD_DISPATCH_UNIT_SCADA_\d+\.zip', report_page)

                for link in report_links:
                    print(link)
                    historic_zip = fetch_zip_bytes(baseurl + link[1:])
                    out = process_scada_zip(historic_zip)

                    chunk_size = 500
                    total_chunks = math.ceil(len(out)/chunk_size)
                    
                    for i in range(0, len(out), chunk_size):
                        chunk = out[i:i + chunk_size]
                        inf.write(chunk,{'db':"nem","precision":"s"},protocol="line")
                        print("Wrote chunk %d of %d" % (i/chunk_size, total_chunks), end='\r')


GENLIST = load_generator_info()
inf = InfluxDBClient(host=INFLUX_HOST, port=8086, database="nem")

process_scada_current(inf)
process_solar_current(inf)
process_scada_archive(inf)
process_solar_archive(inf)
#process_scada_historic(inf)

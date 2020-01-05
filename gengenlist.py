from urllib.request import urlopen
from io import BytesIO, StringIO
from zipfile import ZipFile
import time, re, csv, dateutil.parser
from datetime import datetime

# year, year, month
BASE_URL = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/%d/MMSDM_%d_%02d/MMSDM_Historical_Data_SQLLoader/DATA/'

def load_generator_info():
    generators = []
    with open('generators.csv') as gen_file:
        gen_csv = csv.reader(gen_file)
        generator = map(map_generator_info, gen_csv)
        generators = list(generator)
    return generators


def map_generator_info(row):
    # In:  0:name, 1:region, 2:fuel, 3:id
    fuel = re.sub(r'[\W]+', '', row[2])
    fuel = fuel if len(fuel) > 0 else "Unknown"
    return [row[0], row[1], fuel, tidy_gen_name(row[3])]


def load_co2_info():
    generators = []
    co2_url = "http://www.nemweb.com.au/Reports/CURRENT/CDEII/CO2EII_AVAILABLE_GENERATORS.CSV"
    with urlopen(co2_url) as gen_file:
    #with open("Emission_Intensity.csv") as gen_file:
        gen_txt = StringIO(gen_file.read().decode('utf-8'))
        gen_csv = csv.reader(gen_txt)
        gen_list = filter(is_generator_line, gen_csv)
        generator = map(map_co2_info, gen_list)
        generators += list(generator)
    return generators

def is_generator_line(row):
    return row[:3] == ["D", "CO2EII", "PUBLISHING"]


def map_co2_info(row):
    print(row)

    # Out: id, name, region
    return [tidy_gen_name(row[5]), row[4], row[7], row[6]]


def tidy_gen_name(generator):
    return re.sub(r'[\W_]+', '_', generator)


def load_fuel_info():
    fuels = []
    with open('fueltypes.csv') as fuel_file:
        fuel_csv = csv.reader(fuel_file)
        fuel = map(map_fuel_info, fuel_csv)
        fuels = list(fuel)
    return fuels


def map_fuel_info(row):
    return [row[0], row[1]]


def fetch_zip_bytes(zip_url):
    zipbytes = urlopen(zip_url).read()
    #time.sleep(1) # don't flood AEMO with requests
    return ZipFile(BytesIO(zipbytes))


def is_generator(row):
    return row[:3] == ["D", "PARTICIPANT_REGISTRATION", "GENUNITS"]


def map_aemo_gen(row):
    #4: id
    #21: fuel
    fuel = re.sub(r'[\W]+', '', row[21])
    return [tidy_gen_name(row[4]), fuel]


def find_generator_details(gen_id):
    for i, g in enumerate(genlist):
        if genlist[i][3] == gen_id:
            return i
    return None


def fuel_alias(fueltype):
    for fuel in FUELLIST:
        if fuel[0] == fueltype:
            return fuel[1]
    return "Unknown"


def find_co2_details(generator):
    for gen in CO2LIST:
        if gen[0] == generator or gen[3] == generator:
            return gen
    return [None,None,None,None]


def fetch_monthly_generators(y, m):
    try:
        baseurl = BASE_URL % (y, y, m)
        report_page = urlopen(baseurl).read().decode("utf-8")
        report_links = re.findall(r'\/PUBLIC_DVD_GENUNITS_\d+\.zip', report_page)

        generator_list = []

        for link in report_links:
            print(link)
            gen_zip = fetch_zip_bytes(baseurl + link[1:])

            for file in gen_zip.namelist():
                gen_file = gen_zip.open(file)
                gen_txt = StringIO(gen_file.read().decode("utf-8"))
                gen_csv = csv.reader(gen_txt)

                generators = filter(is_generator, gen_csv)
                generator_list += map(map_aemo_gen, generators)

        return list(generator_list)
    except Exception as ex:
        print(ex)
        return []

# load existing info...
genlist = load_generator_info()
FUELLIST = load_fuel_info()
CO2LIST = load_co2_info()

for y in range(2010, datetime.now().year+1):
    for m in range(1,13): # excludes last item.....
        generators = fetch_monthly_generators(y,m)
        # 0: id   1: fuel
        for generator in generators:

            if generator[0] == "" or generator[1] == "":
                continue

            geninfo = find_generator_details(generator[0])
            
            if geninfo == None:
                newgen = ["Unknown", "Unknown", fuel_alias(generator[1]), generator[0]]
                print("added", newgen)
                genlist.append(newgen)

            else:
                if genlist[geninfo][2] == "Unknown":
                    genlist[geninfo][2] == generator[1]

for i, g in enumerate(genlist):
    geninfo = find_co2_details(g[3])
    # name
    if g[0] == "Unknown" and geninfo[1]:
        genlist[i][0] = geninfo[1]


    # region
    if g[1] == "Unknown" and geninfo[2]:
        genlist[i][1] = geninfo[2]

    print(genlist[i])


with open("generators.csv", "w") as gen_file:
    wr = csv.writer(gen_file,quoting=csv.QUOTE_MINIMAL)
    for gen in genlist:
        wr.writerow(gen)
    #print(genlist)

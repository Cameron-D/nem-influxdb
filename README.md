# NEM Tools

A handful of scripts for reading NEM SCADA data and logging to InfluxDB.

![Screenshot](https://i.imgur.com/oRKIZbn.png "Screenshot")

## The scripts

### gengenlist.py

Generates the list of registered NEM generators and their fuel types (`generators.csv`), the output CSV sometimes needs some manual review and touching up. Should be run occasionally to pick up new generators (otherwise their fuel type will be registered as Unknown).

### backfill.py

Reads and stores all the old archive data published by AEMO. Needs InfluxDB host details configured inside.

There are 5 main methods in this script:

**`process_scada_current` and `process_solar_current`**

Processes the latest data for both SCADA and Rooftop PV (about 2 days worth of 5-minute data for SCADA and 14 days worth of 30-minute PV data).

**`process_scada_archive` and `process_solar_archive`**

Processes the monthly archives for SCADA and Rooftop PV (about 1 year worth of 5-minute data for SCADA and 18 months worth of 30-minute PV data).

**`process_scada_historic`**

Contains SCADA data back to July 2009

### scada.py

Continually reads the latest SCADA and PV data, made to always run. The supplied `Dockerfile` can be used to run in Docker.

### grafana.json

Grafana dashboard, requires the InfluxDB datasource set up and named "NEM"

### nem.py

Old script that just scrapes GlobalRoam's live generation by fuel type data.

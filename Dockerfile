FROM python:3.7-alpine
RUN pip install influxdb python-dateutil
ADD scada.py /
ADD generators.csv /
CMD [ "python", "/scada.py" ]

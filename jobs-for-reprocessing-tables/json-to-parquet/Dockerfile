FROM python:3.9.1

RUN apt-get update
ADD table-reprocessing-jobs/json-to-parquet/code/. /json-to-parquet/code
ADD table-reprocessing-jobs/json-to-parquet/requirements.txt /json-to-parquet
WORKDIR /json-to-parquet
RUN mkdir out
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
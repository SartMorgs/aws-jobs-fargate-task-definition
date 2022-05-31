FROM python:3.9.1

RUN apt-get update
ADD de-extract-data/dump-rds-table-to-s3/code/. /dump-rds-table-to-s3/code
ADD de-extract-data/dump-rds-table-to-s3/requirements.txt /dump-rds-table-to-s3
WORKDIR /dump-rds-table-to-s3
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
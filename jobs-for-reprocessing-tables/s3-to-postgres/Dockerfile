FROM python:3.9.1

RUN apt-get update
ADD table-reprocessing-jobs/s3-to-postgres/code/. /s3-to-postgres/code
ADD table-reprocessing-jobs/s3-to-postgres/requirements.txt /s3-to-postgres
WORKDIR /s3-to-postgres
RUN mkdir out
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
FROM python:3.9.1

RUN apt-get update
ADD table-reprocessing-jobs/s3-to-s3/code/. /s3-to-s3/code
ADD table-reprocessing-jobs/s3-to-s3/requirements.txt /s3-to-s3
WORKDIR /s3-to-s3
RUN mkdir out
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
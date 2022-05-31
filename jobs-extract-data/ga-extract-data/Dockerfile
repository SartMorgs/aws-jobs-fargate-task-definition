FROM python:3.9.1

RUN apt-get update
ADD de-extract-data/ga-extract-data/code/. /ga-data-extract/code
ADD de-extract-data/ga-extract-data/requirements.txt /ga-data-extract
ADD de-extract-data/ga-extract-data/client_secrets.json /ga-data-extract
WORKDIR /ga-data-extract
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
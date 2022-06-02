FROM godatadriven/pyspark

ADD de-extract-data/bi-json-transform/code/. /bi-json-transform/code
ADD de-extract-data/bi-json-transform/requirements.txt /bi-json-transform
WORKDIR /bi-json-transform
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
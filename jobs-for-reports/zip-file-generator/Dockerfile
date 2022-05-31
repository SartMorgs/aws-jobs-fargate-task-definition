FROM python:3.9.1

RUN apt-get update
ADD bi-report-flows/zip-file-generator/code/. /zip-file-generator/code
ADD bi-report-flows/zip-file-generator/requirements.txt /zip-file-generator
WORKDIR /zip-file-generator
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
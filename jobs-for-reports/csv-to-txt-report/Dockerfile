FROM python:3.9.1

RUN apt-get update
ADD bi-report-flows/csv-to-txt-report/code/. /csv-to-txt-report/code
ADD bi-report-flows/csv-to-txt-report/requirements.txt /csv-to-txt-report
WORKDIR /csv-to-txt-report
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
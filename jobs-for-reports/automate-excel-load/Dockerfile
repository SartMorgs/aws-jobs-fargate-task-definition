FROM python:3.9.1

RUN apt-get update
ADD bi-report-flows/automate-excel-load/code/. /automate-excel-load/code
ADD bi-report-flows/automate-excel-load/tmp/. /automate-excel-load/tmp
ADD bi-report-flows/automate-excel-load/requirements.txt /automate-excel-load
WORKDIR /automate-excel-load
RUN pip install -r requirements.txt

ENTRYPOINT [ "python3" ]
CMD [ "code/main.py" ]
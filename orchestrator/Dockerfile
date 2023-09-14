FROM python:3.9-slim

WORKDIR /orchestrator

COPY ./requirements.txt /orchestrator/requirements.txt

RUN pip install -r /orchestrator/requirements.txt

COPY . /orchestrator

EXPOSE 5001

ENV LOG_LEVEL="info"

ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:5001", \
    "--access-logfile", "-", "--access-logformat", \
    "%(h)s %(t)s \"%(r)s\" %(s)s %(b)s %(M)s", \
    "app:app"]

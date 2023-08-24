FROM python:3.9-slim

WORKDIR /orchestrator

COPY ./requirements.txt /orchestrator/requirements.txt

RUN pip install -r /orchestrator/requirements.txt

COPY . /orchestrator

EXPOSE 5001

ENTRYPOINT ["gunicorn", "--bind", "0.0.0.0:5001", "--log-level", "info", "app:app"]

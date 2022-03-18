FROM python:3.6.10-slim

COPY . /orchestrator

RUN pip install -r /orchestrator/requirements.txt

ENTRYPOINT [ "python"]
CMD ["orchestrator/app.py"]

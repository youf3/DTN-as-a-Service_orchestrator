FROM jupyterhub/jupyterhub

# oauthenticator needed for CILogon, notebook for multi-user setup
RUN python3 -m pip install oauthenticator notebook

# instead of copying python files directly, use pip to install
WORKDIR /client
COPY setup.py /client/
COPY dtn_orchestrator /client/dtn_orchestrator

RUN python3 -m pip install .

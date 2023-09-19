FROM jupyterhub/jupyterhub

# fix NAME_REGEX so CILogon ePPNs work for unix usernames
RUN echo "NAME_REGEX='^[a-z][-.@a-z0-9]*$'" >> /etc/adduser.conf

# oauthenticator needed for CILogon, notebook for multi-user setup
RUN python3 -m pip install oauthenticator notebook pyjwt prometheus-http-client pandas matplotlib

# instead of copying python files directly, use pip to install
WORKDIR /client
COPY setup.py /client/
COPY dtn_orchestrator /client/dtn_orchestrator

RUN python3 -m pip install .

# Configuration file for jupyterhub.

from oauthenticator.cilogon import LocalCILogonOAuthenticator
c.JupyterHub.authenticator_class = LocalCILogonOAuthenticator
c.LocalCILogonOAuthenticator.create_system_users = True
c.LocalCILogonOAuthenticator.oauth_callback_url = 'https://<orchestrator host>/hub/oauth_callback'
c.LocalCILogonOAuthenticator.client_id = '<client id from CILogon registration>'
c.LocalCILogonOAuthenticator.client_secret = '<client secret from CILogon registration>'

# SSL certificate
c.JupyterHub.ssl_cert = '/cert.cer'
c.JupyterHub.ssl_key = '/cert.key'

# allowed users
c.Authenticator.allowed_users = set(["user1@domain", "user2@anotherdomain"])

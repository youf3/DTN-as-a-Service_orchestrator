# DTN-as-a-Service v2 orchestrator

# Deployment
Create volume for db and use -v orchestrator:/orchestrator/db to bind it to the container if it is docker.

# Configuration
No specific configuration needed

# Starting transfer
1) Start agent container in two DTNs and get the details of the DTNs (name, man_addr, data_addr, username, interface).
2) Add two DTNs to orchestrator using http post to /DTN with the detail (as in orchestrator/transfer_test.py). Get their ID to use for a transfer.
3) Create files in one of the DTNs's mounted data location.
4) Start transfer with srcfile, dstfile, num_workers specified and post these to /transfer/nuttcp/sender_id/receiver_id. Get the transfer ID.
5) Wait for the transfer to finish using http post to /wait/transfer_id to finish up the transfer.
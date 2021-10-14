##
# client.py
#
# Jupyter notebook class for easy interactions with the DTN orchestrator.
# It connects directly to the orchestrator container running on the same overlay net.
##
import requests
import os

class DTN(object):
    def __init__(self, client, dtndata):
        self._client = client
        if 'id' not in dtndata.keys():
            raise ValueError("ID not given in DTN object")
        # just use the scheme in the returned JSON - defined in orchestrator app.py
        for key in dtndata:
            if '.' not in key and '__' not in key:
                self.__dict__[key] = dtndata[key]

    def __str__(self):
        try:
            return f"{self.id}: {self.name} ({self.man_addr})"
        except:
            return str(self.id if hasattr(self, "id") else 0)

    def files(self, filedir=None):
        # TODO permissions checking
        result = requests.get(f"http://{self.man_addr}/files/{filedir if filedir else ''}")
        if result.status_code == 200:
            return result.json()
        else:
            raise Exception(result.text)
    
    def ping(self, remote_id):
        return self._client.ping(self.id, remote_id)

    def nvmeof_get(self):
        result = requests.get(f"http://{self.man_addr}/nvme/devices")
        if result.status_code != 200:
            raise Exception(f"Error retrieving NVMEoF data")
        return result.json()

    def nvmeof_setup(self):
        result = requests.post(f"http://{self.man_addr}/nvme/setup", json={
            'addr': self.data_addr,
            'numa': 0 # FIXME
        })
        if result.status_code != 200:
            raise Exception(f"Error setting up NVMEoF, code {result.status_code}: {result.text}")
        return result.json()

    def nvmeof_stop(self):
        result = requests.delete(f"http://{self.man_addr}/nvme/setup", json={
            'addr': self.data_addr,
            'numa': 0 # FIXME
        })
        if result.status_code != 200:
            raise Exception(f"Error stopping NVMEoF, code {result.status_code}: {result.text}")
        return

    def nvmeof_connect(self, remote_addr, mountpoint="/remote_nvme"):
        result = requests.post(f"http://{self.man_addr}/nvme/connect", json={
            'remote_addr': remote_addr,
            'mountpoint': mountpoint,
            'num_disk': 1 # FIXME read from sender side
        })
        if result.status_code != 200:
            raise Exception(f"Error connecting NVMEoF, code {result.status_code}: {result.text}")
        return result.json()
    
    def nvmeof_disconnect(self, remote_addr):
        result = requests.delete(f"http://{self.man_addr}/nvme/connect", json={
            'remote_addr': remote_addr,
        })
        if result.status_code != 200:
            raise Exception(f"Error connecting NVMEoF, code {result.status_code}: {result.text}")
        return

class NVMEConnection(object):
    def __init__(self, client, sender, receiver, mountpoint="/remote_nvme", setup=True):
        self.orchestrator = client
        self.sender = sender
        self.receiver = receiver
        self.mountpoint = mountpoint
        self.transfer_id = None
        self._status = "initialized"

        # first, check for an existing connection
        sender_existing = self.sender.nvmeof_get()
        receiver_existing = self.receiver.nvmeof_get()
        if any(dev.get('transport') == 'tcp' for dev in receiver_existing.get('devices')):
            print('existing connection found')
            self.sender_devices = sender_existing.get('devices')
            self.receiver_devices = receiver_existing.get('devices')
        elif setup:
            # on sender (side 1): create the nvmeof service for connecting
            self.sender_devices = self.sender.nvmeof_setup()
            print('sender devices: ')
            print(self.sender_devices)
            
            # on receiver (side 2): connect to side 1 and bring nvmeof up
            self.receiver_devices = self.receiver.nvmeof_connect(self.sender.data_addr, mountpoint=self.mountpoint)
            print('receiver devices:')
            print(self.receiver_devices)

        # update mountpoint to real directory (computed in DiskManager.py's mount())
        remote_devices = [dev for dev in self.receiver_devices.get('devices') if dev.get('transport') == 'tcp']
        if remote_devices:
            # first remote device
            self.mountpoint = remote_devices[0].get('mounted')

    def disconnect(self):
        self.receiver.nvmeof_disconnect(self.sender.man_addr)
        # sender errors should not be catastrophic
        try:
            self.sender.nvmeof_stop()
        except Exception as e:
            print('Warning on sender disconnect: ' + str(e))
        self._status = "disconnected"

    def copy(self, sourcefiles, destination):
        if self._status == "disconnected":
            raise Exception("copy after disconnect")

        # find remote mount name from receiver and prepend to source files
        sourcefiles = [os.path.join(self.mountpoint, srcf) for srcf in sourcefiles]

        self.transfer_id = self.orchestrator.transfer(sourcefiles, destination,
                self.sender.id, self.receiver.id, remote_mount="/remote_nvme/disk1/", tool="dd")
        self._status = "copying"
        return self.transfer_id

    def status(self):
        if self._status == "copying":
            transfer_status = self.orchestrator.get_transfer(self.transfer_id)
            return transfer_status
        else:
            return {"status": self._status}

class Transfer(object):
    def __init__(self):
        pass

class DTNOrchestratorClient(object):
    def __init__(self, host="orchestrator", port=5000):
        self.base_url = f"http://{host}:{port}/"
    
    def check(self):
        result = requests.get(self.base_url)
        if result.status_code == 200:
            return "OK"
        else:
            raise Exception(result.text)

    def list_dtns(self):
        dtnlist = requests.get(self.base_url + f"DTN/").json()
        return [DTN(self, dtnjson) for dtnjson in dtnlist]

    def get_dtn(self, id):
        result = requests.get(self.base_url + f"DTN/{id}")
        # TODO error checking
        dtn = DTN(self, result.json())
        return dtn

    def add_dtn(self, name, management_addr, data_addr, username, interface):
        # before adding, check for an existing one (identical name & mgmt addr)
        dtnlist = self.list_dtns()
        for dtn in dtnlist:
            if name == dtn.name and management_addr in dtn.man_addr:
                print('Found existing DTN registered')
                return dtn

        result = requests.post(self.base_url + "DTN/", json={
            'name': name,
            'man_addr': management_addr,
            'data_addr': data_addr,
            'username': username,
            'interface': interface
            })
        # TODO error checking
        if result.status_code != 200:
            print(f"Error {result.status_code}")
            raise Exception(result.text)
        return self.get_dtn(result.json()['id'])

    def register_dtn(self, address, data_addr=None, interface=None):
        result = requests.post(f"http://{address}/register", json={
            "address": address,
            "data_addr": data_addr,
            "interface": interface,
        })
        if result.status_code != 200:
            print(f"Error {result.status_code}")
            raise Exception(result.text)
        dtn_data = result.json()
        return self.add_dtn(dtn_data["name"], dtn_data["man_addr"], 
            dtn_data["data_addr"], dtn_data["username"], dtn_data["interface"])

    def delete_dtn(self, id):
        # TODO permissions checking
        result = requests.delete(self.base_url + f"DTN/{id}")
        # TODO error checking
        return result.json()

    def setup_nvmeof(self, sender, receiver):
        return NVMEConnection(self, sender, receiver)

    def get_transfers(self):
        result = requests.get(self.base_url + "running")
        # TODO error checking
        return result.json()

    def get_transfer(self, id):
        # TODO permissions checking
        result = requests.get(self.base_url + f"transfer/{id}")
        # TODO error checking
        return result.json()
    
    def ping(self, sender, receiver):
        # TODO permissions checking
        # sender/receiver may be DTN ID or DTN object
        result = requests.get(self.base_url + f"ping/{sender}/{receiver}")
        # TODO error checking
        return result.json()       

    def transfer(self, sourcefiles, destfiles, sender, receiver, tool="nuttcp", remote_mount=None, num_workers=1, blocksize=8192, zerocopy=False):
        # TODO permissions checking
        result = requests.post(self.base_url + f"transfer/{tool}/{sender}/{receiver}", json={
            "srcfile": sourcefiles,
            "dstfile": destfiles,
            "remote_mount": remote_mount,
            "num_workers": num_workers,
            "blocksize": blocksize,
            "zerocopy": zerocopy
        })
        return result.json()

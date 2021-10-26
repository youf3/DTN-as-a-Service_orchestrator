##
# client.py
#
# Jupyter notebook class for easy interactions with the DTN orchestrator.
# It connects directly to the orchestrator container running on the same overlay net.
##
import requests
import os
import json
import getpass
import jwt
import time
import pandas
from prometheus_http_client import Prometheus
from http.client import HTTPException
from collections import namedtuple

Transfer = namedtuple("Transfer", ['id', 'srcfiles', 'dstfiles'])

# TODO save these in the DTN object on the orchestrator. 
#  This hardcoded/static map is needed since Prometheus jobs are different from
#  the actual hostnames and registered DTN names.
PROMETHEUS_JOBS = {
    "138.44.15.78:5001": "aarnet01",
    "74.114.96.98:5000": "starlight01",
    "109.171.131.68:5000": "kaust01",
    "210.119.23.12:5000": "kisti01",
    "145.146.1.10:5000": "uva01",
    "213.135.51.226:5000": "icm01",
    "193.166.254.54:5000": "csc01",
    "193.166.254.50:5000": "csc02",
    "103.72.192.66:5001": "nscc01"
}

class DTN(object):
    """
    Object that represents a DTN object on the orchestrator. However, this object
    also facilitates functions that talk directly to the DTN agent.

    This allows checking local files, testing latency to other DTNs, and configuring
    NVME over fabric (NVMEoF).
    """
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

    def _token_header(self):
        if hasattr(self, 'jwt_token') and self.jwt_token:
            encoded_data = {'sub': getpass.getuser()}
            return {"Authorization": f"Bearer {jwt.encode(encoded_data, self.jwt_token, 'HS256')}"}

    def files(self, filedir=None):
        """
        Retrieve a list of files available on the DTN.
        
        :param filedir: Optional parent file directory to filter.

        :returns: A list of dictionaries containing file modification time, path, 
        type (file or dir) and size.
        """
        # TODO permissions checking
        result = requests.get(f"http://{self.man_addr}/files/{filedir if filedir else ''}", headers=self._token_header())
        if result.status_code == 200:
            return result.json()
        else:
            raise Exception(result.text)
    
    def create_dirs(self, dirlist):
        """
        Create a directory on the DTN.

        :param dirlist: List of directories to create.
        """
        mkdir_result = requests.post(f"http://{self.man_addr}/create_dir/", headers=self._token_header(), json=dirlist)
        if mkdir_result.status_code != 200:
            raise Exception(f"Error creating directories, code {mkdir_result.status_code} ({mkdir_result.text})")

    def ping(self, remote_id):
        """
        Ping (run latency test) to another DTN.

        :param remote_id: Remote DTN object or ID.
        :returns: Float/double that represents latency between this DTN and the
        remote DTN in seconds.
        """
        return self._client.ping(self.id, remote_id)

    def nvmeof_get(self):
        """
        Get NVME drives available on the DTN.
        """
        result = requests.get(f"http://{self.man_addr}/nvme/devices", headers=self._token_header())
        if result.status_code != 200:
            raise Exception(f"Error retrieving NVMEoF data")
        return result.json()

    def nvmeof_setup(self):
        """
        Set up the NVME subsystem so other DTNs can remotely interact with this 
        DTN's drives via NVMEoF.
        """
        result = requests.post(f"http://{self.man_addr}/nvme/setup", headers=self._token_header(), json={
            'addr': self.data_addr,
            'numa': 0 # FIXME
        })
        if result.status_code != 200:
            raise Exception(f"Error setting up NVMEoF, code {result.status_code}: {result.text}")
        return result.json()

    def nvmeof_stop(self):
        """
        Stop the NVME subsystem that allows remote interaction via NVMEoF.
        """
        result = requests.delete(f"http://{self.man_addr}/nvme/setup", headers=self._token_header(), json={
            'addr': self.data_addr,
            'numa': 0 # FIXME
        })
        if result.status_code != 200:
            raise Exception(f"Error stopping NVMEoF, code {result.status_code}: {result.text}")
        return

    def nvmeof_connect(self, remote_addr, mountpoint="/remote_nvme"):
        """
        Connect to a remote DTN's NVME subsystem so remote drives can be mounted.

        :param remote_addr: IP address of the remote DTN.
        :param mountpoint: Optional mountpoint to mount the remote NVME drive(s).
        """
        result = requests.post(f"http://{self.man_addr}/nvme/connect", headers=self._token_header(), json={
            'remote_addr': remote_addr,
            'mountpoint': mountpoint,
            'num_disk': 1 # FIXME read from sender side
        })
        if result.status_code != 200:
            raise Exception(f"Error connecting NVMEoF, code {result.status_code}: {result.text}")
        return result.json()
    
    def nvmeof_disconnect(self, remote_addr):
        """
        Disconnect from a remote DTN's NVME subsystem. This closes the NVMEoF connection.
        """
        result = requests.delete(f"http://{self.man_addr}/nvme/connect", headers=self._token_header(), json={
            'remote_addr': remote_addr,
        })
        if result.status_code != 200:
            raise Exception(f"Error connecting NVMEoF, code {result.status_code}: {result.text}")
        return

    def get(self, url, **kwargs):
        """
        Run a REST GET directly to the client, while auto-adding the authorization token.
        This accepts all parameters from requests.get().
        """
        kwargs['headers'] = self._token_header()
        return requests.get(f"http://{self.man_addr}/{url}", **kwargs)
    
    def post(self, url, **kwargs):
        """
        Run a REST POST directly to the client, while auto-adding the authorization token.
        This accepts all parameters from requests.post().
        """
        kwargs['headers'] = self._token_header()
        return requests.post(f"http://{self.man_addr}/{url}", **kwargs)

class StatsExtractor(object):
    """
    Statistics extractor for file transfers. This updates data on a remote
    Prometheus server for performance analysis.
    """
    def __init__(self, client, extractor_url="http://165.124.33.158:9091"):
        self.orchestrator = client
        self.extractor_url = extractor_url
        self.promclient = Prometheus()
        self.promclient.url = self.extractor_url
    
    def _prettify_header(self, metric):
        metrics_to_remove = ['instance', 'job', 'mode', '__name__', 'container', 'endpoint', 'namespace', 'pod', 'prometheus', 'service']
        for i in metrics_to_remove:
            if i in metric: del metric[i]
        if len(metric) > 1 : raise Exception('too many metric labels')
        else:
            return next(iter(metric.keys()))

    def extract(self, sender, receiver, start_time=time.time(), end_time=time.time() + 1):
        """
        Extract metrics from the Prometheus server. Requires sender/receiver data as well as
        start and end time to narrow down metrics to this specific transfer.

        :param sender: Sender DTN object or ID.
        :param receiver: Receiver DTN object or ID.
        :param start_time: Start time of the transfer.
        :param end_time: End time of the transfer.

        :returns: A Pandas DataFrame object if metrics were found.
        """
        sender = self.orchestrator._id2dtn(sender)
        receiver = self.orchestrator._id2dtn(receiver)

        # monitor address = management address with a different port (default 9100)
        sender_mon_addr = sender.man_addr.split(':')[0] + ":9100"
        receiver_mon_addr = receiver.man_addr.split(':')[0] + ":9100"
        
        STEP = 15
        AVG_INT = 15
        MAX_RES = 11000
        query = (
        'label_replace(sum by (instance)(irate(node_network_transmit_bytes_total{{instance=~"{4}.*", device="{2}"}}[{1}m])), "network_throughput", "$0", "instance", "(.+)") '
        'or label_replace(sum by (job)(irate(node_disk_written_bytes_total{{instance=~"{5}.*", device=~"nvme.*"}}[{1}m])),"Goodput", "$0", "job", "(.+)") '
        'or label_replace(sum by (job)(1 - irate(node_cpu_seconds_total{{mode="idle", instance="{4}"}}[1m])),"CPU", "$0", "job", "(.+)") '
        'or label_replace(max by (container)(container_memory_working_set_bytes{{namespace="{3}", container=~"{0}.*"}}), "Memory_used", "$0", "container", "(.+)") '
        'or label_replace(node_memory_Active_bytes{{instance="{4}"}}, "Memory_used", "$0", "instance", "(.+)") '
        'or label_replace(sum by (job)(irate(node_disk_read_bytes_total{{instance=~"{4}.*", device=~"nvme.*"}}[{1}m])),"NVMe_transfer_bytes", "$0", "job", "(.+)") '
        'or label_replace(sum by (job)(irate(node_disk_io_time_seconds_total{{instance=~"{4}.*", device=~"nvme.*"}}[{1}m])),"NVMe_total_util", "$0", "job", "(.+)") '
        'or label_replace(count by (job)(node_disk_io_time_seconds_total{{instance=~"{4}.*", device=~"nvme[0-7]n1"}}),"Storage_count", "$0", "job", "(.+)") '
        'or label_replace(sum by (job)(node_network_speed_bytes{{instance=~"{4}.*", device="{2}"}} * 8), "NIC_speed", "$0", "job", "(.+)") '
        'or label_replace(sum by (job)(irate(node_netstat_Tcp_RetransSegs{{instance=~"{4}.*"}}[{1}m])), "Packet_losses", "$0", "job", "(.+)") '
        'or label_replace(avg by (job)((node_hwmon_temp_celsius{{instance=~"{4}.*"}})), "CPU_temp", "$0", "job", "(.+)") '
        'or label_replace(count without(cpu, mode) (node_cpu_seconds_total{{mode="idle", job="{5}"}}), "CPU_number", "$0", "job", "(.+)") '
        '').format(sender.name, AVG_INT, sender.interface, 'dtnaas', sender_mon_addr, receiver_mon_addr, PROMETHEUS_JOBS.get(sender.man_addr))
        dataset = None
        
        while end_time > start_time:        
            data_in_period = None
            max_ts = start_time + (STEP * MAX_RES) 
            next_hop_ts = end_time if max_ts > end_time else max_ts
            print('Getting data for {} : {}'.format(start_time, end_time))
            res = self.promclient.query_rang(metric=query, start=start_time, end=next_hop_ts, step=STEP)
            if '401 Authorization Required' in res:
                raise HTTPException(res)
            response = json.loads(res)
            if response['status'] != 'success':
                raise Exception('Failed to query Prometheus server')
            
            for result in response['data']['result']:
                result['metric'] = self._prettify_header(result['metric'])
                df = pandas.DataFrame(data=result['values'], columns = ['Time', result['metric']], dtype=float)            
                df['Time'] = pandas.to_datetime(df['Time'], unit='s')
                df.set_index('Time', inplace=True)
                data_in_period = df if data_in_period is None else data_in_period.merge(df, how='outer',  on='Time').sort_index()
            
            dataset = data_in_period if dataset is None else dataset.append(data_in_period)
            start_time = next_hop_ts
        cols = dataset.columns.tolist()
        labels_to_rearrange = ['NVMe_total_util', 'NVMe_transfer_bytes']    
        for i in labels_to_rearrange:
            cols.remove(i)
            cols.insert(0,i)

        return dataset[cols]

class Connection(object):
    """
    Object that represents a connection between two DTNs. This connection can facilitate
    file transfers between these DTNs.
    """
    def __init__(self, client, sender, receiver, setup=False, tool="nuttcp"):
        self.orchestrator = client
        self.sender = sender
        self.receiver = receiver
        self.transfer_id = None
        self.tool = tool
        self._status = "initialized"

        self.extractor = StatsExtractor(client)
        self.pre_csv = ""
        self.post_csv = ""
        self.latency = None
        self.blocksize = None

        if setup: # run some setup if requested
            # TODO determine setup steps
            pass

    def disconnect(self):
        """
        Stop this connection. The object can be safely deleted after this is run.
        """
        self._status = "disconnected"
        pass # nothing needed on disconnection

    def copy(self, sourcedir, destinationdir, limit=None, num_workers=1, blocksize=8192, zerocopy=False, require_stats=False):
        """
        Copy all files from a sender DTN's source directory to a receiver DTN's destination directory.
        This starts a file transfer between two DTNs, and provides data to track transfer progress.

        :param sourcedir: Source directory, must exist on the sender DTN.
        :param destinationdir: Destination directory, must exist on the receiver DTN.
        :limit: Optional limit on number of directories/files to copy.
        :num_workers: Number of worker processes to facilitate the transfer.
        :blocksize: Block size for the transfer in bytes.
        :zerocopy: Zerocopy setting for nuttcp.

        :returns: A Transfer namedtuple that contains the transfer ID, and list 
        of source and destination files that will be copied.
        """
        if self._status == "disconnected":
            raise Exception("copy after disconnect")

        # get files from sender and key them by path for easy lookups
        read_files = self.sender.files(sourcedir)
        read_files = {f['name']: f for f in read_files}
        
        # full, file-only list
        complete_source_files = []
        # also need destination file list, parent dir is not enough
        complete_destination_files = []
        # generate a list of destination directories
        created_dirs = []
        for index, rf in enumerate(read_files):
            if limit and index >= limit:
                break # stop copying
            if read_files[rf]['type'] == 'file':
                complete_source_files.append(os.path.join(sourcedir, rf))
                complete_destination_files.append(os.path.join(destinationdir, rf))
            elif read_files[rf]['type'] == 'dir':
                created_dirs.append(os.path.join(destinationdir, rf))
        
        # make sure directories exist on receiver
        self.receiver.create_dirs(created_dirs)

        try:
            # get latency and blocksize for metrics later
            self.latency = self.orchestrator.ping(self.sender, self.receiver).get('latency')
            self.blocksize = blocksize

            pre_dataframe = self.extractor.extract(self.sender, self.receiver)
            self.pre_csv = pre_dataframe.to_csv(header=True, index=False)
        except Exception as e:
            print('Stats error: ' + str(e))
            if require_stats:
                raise e

        self.transfer_id = self.orchestrator.transfer(complete_source_files, complete_destination_files,
                self.sender.id, self.receiver.id, tool=self.tool,
                num_workers=num_workers, blocksize=blocksize, zerocopy=zerocopy)
        self._status = "copy initiated"
        return Transfer(self.transfer_id, complete_source_files, complete_destination_files)

    def status(self):
        """
        Get the current status of the connection transfer, if it has been started.
        """
        if self.transfer_id:
            return self.orchestrator.get_transfer_status(self.transfer_id)

    def get(self):
        """
        Get transfer data of the current transfer, if it has completed.
        """
        if self.transfer_id:
            return self.orchestrator.get_transfer(self.transfer_id)

    def get_stats(self):
        """
        Collect transfer statistics from Prometheus.
        """
        finish_data = self.get()
        post_dataframe = self.extractor.extract(self.sender, self.receiver,
            start_time=finish_data['start_time'], end_time=finish_data['end_time'])
        # modify the dataframe before saving
        mean_df = pandas.DataFrame(post_dataframe.mean())
        df_t = mean_df.T
        df_t['num_workers']= finish_data['num_workers']
        df_t['num_files']= finish_data['num_files']
        df_t['blocksize']= self.blocksize
        df_t['latency']= self.latency
        self.post_csv = df_t.to_csv(header=True, index=False)

        return self.post_csv

    def finish(self, cleanup=False):
        """
        Finish the transfer when all files have been copied.

        This object's .pre_csv and .post_csv are also written.

        :param cleanup: Optional boolean flag, enable to facilitate cleanup 
        (dependent on the tool used for the transfer)
        """
        if self.transfer_id:
            return self.orchestrator.finish_transfer(self.transfer_id,
                    sender=(self.sender if cleanup else None),
                    tool=(self.tool if cleanup else None))

    def copy_and_wait(self, sourcedir, destinationdir, limit=None, num_workers=1, blocksize=8192, zerocopy=False, require_stats=False):
        """
        Similar to copy(), except instead of a nonblocking function that returns a Transfer object 
        this is a blocking object that waits until the transfer is complete.
        
        copy_and_wait() accepts the same arguments as copy().
        """
        copydata = self.copy(sourcedir, destinationdir, limit=limit, num_workers=num_workers,
            blocksize=blocksize, zerocopy=zerocopy, require_stats=require_stats)
        print(f'Started transfer #{copydata.id}')

        finished = False
        while not finished:
            time.sleep(30)
            status = self.status()
            print(status)
            if not status:
                raise Exception("Problem running transfer, empty status from orchestrator")
            finished = status.get('Unfinished') == 0

        print('Finishing transfer...')
        time.sleep(5)
        self.finish()
        time.sleep(5)

        # get stats and return
        return self.get(), self.get_stats()

class NVMEConnection(Connection):
    """
    Object that adds NVME connection functions on top of the regular Connection object.
    """
    def __init__(self, client, sender, receiver, mountpoint="/remote_nvme", setup=True):
        super().__init__(client, sender, receiver, setup=False, tool="dd") # skip default setup
        self.mountpoint = mountpoint

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
            self.receiver_devices.get('devices')

        # update mountpoint to real directory (computed in DiskManager.py's mount())
        remote_devices = [dev for dev in self.receiver_devices if dev.get('transport') == 'tcp']
        if remote_devices:
            # first remote device
            self.mountpoint = remote_devices[0].get('mounted')

    def disconnect(self):
        """
        Shut down the NVMEoF connection between the sender and receiver.
        This disconnects on the receiver side, and shuts down the subsystem on the
        sender side.
        """
        self.receiver.nvmeof_disconnect(self.sender.man_addr)
        # sender errors should not be catastrophic
        try:
            self.sender.nvmeof_stop()
        except Exception as e:
            print('Warning on sender disconnect: ' + str(e))
        self._status = "disconnected"

    def copy(self, sourcedir, destinationdir, limit=None, num_workers=1, blocksize=8192, zerocopy=False,
            remote_mount="/remote_nvme/"):
        """
        Copy a list of source files from the sender DTN's remotely mounted NVME drives
        to a destination directory on the receiver DTN.
        """
        if self._status == "disconnected":
            raise Exception("copy after disconnect")

        # get files from sender and key them by path for easy lookups
        read_files = self.sender.files(sourcedir)
        read_files = {f['name']: f for f in read_files}
        
        # full, file-only list
        complete_source_files = []
        # also need destination file list, parent dir is not enough
        complete_destination_files = []
        # generate a list of destination directories
        created_dirs = []
        for index, rf in enumerate(read_files):
            if limit and index >= limit:
                break # stop copying
            if read_files[rf]['type'] == 'file':
                complete_source_files.append(os.path.join(sourcedir, rf))
                complete_destination_files.append(os.path.join(destinationdir, rf))
            elif read_files[rf]['type'] == 'dir':
                created_dirs.append(os.path.join(destinationdir, rf))
        
        # make sure directories exist on receiver
        self.receiver.create_dirs(created_dirs)

        # find remote mount name from receiver and prepend to source files
        #complete_source_files = [os.path.join(self.mountpoint, srcf) for srcf in complete_source_files]

        self.transfer_id = self.orchestrator.transfer(complete_source_files, complete_destination_files,
                self.sender.id, self.receiver.id, remote_mount=remote_mount, tool="dd",
                num_workers=num_workers, blocksize=blocksize, zerocopy=zerocopy)
        self._status = "copying"
        return self.transfer_id

class DTNOrchestratorClient(object):
    """
    Client wrapper for the DTN Orchestrator.
    """
    def __init__(self, host="orchestrator", port=5000):
        self.base_url = f"http://{host}:{port}/"
    
    def _id2dtn(self, id_or_dtn):
        if id_or_dtn is None:
            raise ValueError("DTN ID or object cannot be None")
        elif isinstance(id_or_dtn, DTN):
            return id_or_dtn
        else:
            return self.get_dtn(id_or_dtn)

    def check(self):
        """
        Check connection status between this host and the orchestrator.
        """
        result = requests.get(self.base_url)
        if result.status_code == 200:
            return "OK"
        else:
            raise Exception(result.text)

    def list_dtns(self):
        """
        Get a list of known DTN Objects from the orchestrator.
        
        :returns: List of DTN objects.
        """
        dtnlist = requests.get(self.base_url + f"DTN/").json()
        return [DTN(self, dtnjson) for dtnjson in dtnlist]

    def get_dtn(self, id):
        """
        Get a specific DTN from the orchestrator by DTN ID.

        :param id: DTN ID as an integer to look up.
        :returns: DTN object if found, otherwise a ValueError is raised.
        """
        try:
            result = requests.get(self.base_url + f"DTN/{id}")
            # TODO error checking
            dtn = DTN(self, result.json())
        except json.JSONDecodeError:
            raise ValueError(f"DTN ID {id} not found on orchestrator")
        return dtn

    def add_dtn(self, name, management_addr, data_addr, username, interface, jwt_token=""):
        """
        Manually add a DTN to the orchestrator. It does not have to be running
        or reachable to be added.

        :param name: Name of the DTN.
        :management_addr: Management IP address and port for the DTN agent.
        :data_addr: Dataplane IP address for the DTN - used for transfers.
        :username: Username for file permissions on certain operations.
        :interface: Dataplane interface on the DTN.

        :returns: A DTN object with the created DTN data.
        """
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
            'interface': interface,
            'jwt_token': jwt_token
            })
        # TODO error checking
        if result.status_code != 200:
            print(f"Error {result.status_code}")
            raise Exception(result.text)
        return self.get_dtn(result.json()['id'])

    def register_dtn(self, address, data_addr=None, interface=None):
        """
        Auto register a DTN. Unlike add_dtn(), this requires the DTN to be accessible
        and to have the iCAIR DTN agent running.

        :param address: Management IP Address and port of the DTN.
        :param data_addr: Optional dataplane IP address - if this is not given,
        the DTN agent will attempt to discover it by the interface parameter or
        by finding the fastest interface.
        :param interface: Optional dataplane interface - if this is not given,
        the DTN agent will attempt to discover it by the data_addr parameter or
        by finding the fastest interface.

        :returns: A DTN object with the created DTN data.
        """
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
            dtn_data["data_addr"], dtn_data["username"], dtn_data["interface"],
            jwt_token=dtn_data.get("jwt_token", ""))

    def delete_dtn(self, dtn):
        """
        Delete a DTN from the orchestrator.

        :param dtn: DTN object or DTN ID.

        :returns: The direct orchestrator API result.
        """
        dtn = self._id2dtn(dtn)
        # TODO permissions checking
        result = requests.delete(self.base_url + f"DTN/{dtn.id}")
        # TODO error checking
        return result.json()

    def setup_nvmeof(self, sender, receiver):
        """
        Set up an NVME over fabric (NVMEoF) connection between two DTNs.

        :param sender: Sender DTN object or ID. This should have some NVME drives
        that can be remotely mounted.
        :param receiver: Receiver DTN object or ID. This should be able to access
        the sender DTN over port 4420 from NVME over fabric.

        :returns: A configured NVMEConnection object.
        """
        sender = self._id2dtn(sender)
        receiver = self._id2dtn(receiver)
        return NVMEConnection(self, sender, receiver)

    def setup_connection(self, sender, receiver, tool="nuttcp"):
        """
        Set up a transfer connection between two DTNs.

        :param sender: Sender DTN object or ID.
        :param receiver: Receiver DTN object or ID.
        :param tool: Optional tool name, this will be used for the file transfer.

        :returns: A configured Connection object.
        """
        sender = self._id2dtn(sender)
        receiver = self._id2dtn(receiver)
        return Connection(self, sender, receiver, tool=tool)

    def get_transfers(self):
        """
        Get a list of running transfers from the orchestrator.
        """
        result = requests.get(self.base_url + "running")
        # TODO error checking
        return result.json()

    def get_transfer(self, id):
        """
        Get information about a specific transfer by Transfer ID.

        :param id: Transfer ID as an integer.
        :returns: Transfer data if available, otherwise a dictionary with the
        message "need to wait for transfer #id".
        """
        # TODO permissions checking
        result = requests.get(self.base_url + f"transfer/{id}")
        # TODO error checking
        return result.json()
    
    def get_transfer_status(self, id):
        """
        Get the status of a specific transfer currently in progress.

        :param id: Transfer ID as an integer.
        :returns: Transfer status data as a dictionary, usually in this format:
        {
            'Finished': 1,
            'Unfinished': 2,
            'throughput': 379.2342
        }
        """
        # TODO permissions checking
        result = requests.get(self.base_url + f"check/{id}")
        if result.status_code == 200:
            return result.json()
    
    def finish_transfer(self, transfer_id, sender=None, tool=None):
        """
        Complete a transfer if and when there are no more unfinished files.

        :param transfer_id: Transfer ID as an integer.
        :param sender: Optional sender DTN object or ID. If given with tool, this
        can be used to clean up the transfer.
        :param tool: Optional tool name. If given with sender, this can be used to 
        clean up the transfer.

        :returns: A status dictionary if the transfer has not finished, otherwise
        a transfer dictionary that has details about transfer statistics.
        """
        # check for transfer status first
        status = self.get_transfer_status(transfer_id)
        if status and status.get('Unfinished') == 0:
            # get transfer data and clean up if needed
            # TODO don't assume nuttcp as the transfer tool
            if sender and tool:
                sender = self._id2dtn(sender)
                requests.get(f"{sender.man_addr}/cleanup/{tool}", headers=sender._token_header())
            
            wait_data = requests.post(self.base_url + f"wait/{transfer_id}")
            if wait_data.status_code == 200:
                return wait_data.json()
            else:
                raise Exception(f"Error finishing transfer, code {wait_data.status_code}: {str(wait_data.text)}")
        else:
            return status

    def ping(self, sender, receiver):
        """
        Run a ping/latency test between two DTNs.

        :param sender: Source DTN object or ID.
        :param receiver: Destination DTN object or ID.

        :returns: A float/double that represents latency in seconds.
        """
        # sender/receiver may be DTN ID or DTN object
        sender = self._id2dtn(sender)
        receiver = self._id2dtn(receiver)
        # TODO permissions checking
        result = requests.get(self.base_url + f"ping/{sender.id}/{receiver.id}", headers=sender._token_header())
        # TODO error checking
        if result.status_code == 401:
            raise Exception("Not authorized (DTN not registered?)")
        if result.status_code == 200:
            return result.json()
        else:
            raise Exception(f"Error {result.status_code} running ping from {sender.name}")

    def transfer(self, sourcefiles, destfiles, sender, receiver, tool="nuttcp", remote_mount=None, num_workers=1, blocksize=8192, zerocopy=False):
        """
        Initiate a transfer between two DTNs with specific lists of files.

        :param sourcefiles: A list of source files (no directories!) to copy from the sender DTN.
        :param destfiles: A list of destination filenames to write on the receiver DTN.
        :sender: Sender DTN object or ID.
        :receiver: Receiver DTN object or ID.
        :tool: Optional transfer tool name.
        :remote_mount: Optional remote mount location for NVMEoF connections.
        :num_workers: Number of worker processes to facilitate the transfer.
        :blocksize: Block size for the transfer in bytes.
        :zerocopy: Zerocopy setting for nuttcp.
        """
        sender = self._id2dtn(sender)
        receiver = self._id2dtn(receiver)
        # TODO permissions checking
        # note! file/directory checking is NOT done here, that should be 
        # taken care of before sourcesfiles/destfiles gets passed
        result = requests.post(self.base_url + f"transfer/{tool}/{sender.id}/{receiver.id}", headers=sender._token_header(),
        json={
            "srcfile": sourcefiles,
            "dstfile": destfiles,
            # sender/receiver auth
            "sender_token": sender.jwt_token,
            "receiver_token": receiver.jwt_token,
            "remote_mount": remote_mount,
            "num_workers": num_workers,
            "blocksize": blocksize,
            "zerocopy": zerocopy
        })
        if result.status_code == 200:
            # get the transfer ID and start waiting
            transfer_id = result.json().get("transfer")
            return transfer_id
        else:
            raise Exception(f"Error starting transfer: {str(result.json)}")

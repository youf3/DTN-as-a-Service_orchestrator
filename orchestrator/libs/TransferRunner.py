import logging
import os
import requests
import time

from collections import namedtuple
from concurrent.futures import CancelledError
from datetime import datetime
from threading import get_ident

from .ThreadExecutor import ThreadPoolExecutor

Sender = namedtuple('Sender', ['addr', 'data_addr', 'token'])
Receiver = namedtuple('Receiver', ['addr', 'token'])

class TransferException(Exception):
    pass

def _transfer_file(sender, receiver, srcfile, dstfile, tool, params, timeout=None, retry=5):
    params = params.copy()
    if not params.get('blocksize'):
        raise ValueError("params requires blocksize")
    if not srcfile and 'duration' not in params:
        raise ValueError("params requires duration if no sourcefile given")
    # no retries if this is a mem-to-mem copy
    if not srcfile and not dstfile:
        retry = 1
    for i in range(retry):
        try:
            # ensure this data is thread-specific
            params['file'] = srcfile
            if 'remote_mount' in params and params['remote_mount']:
                # if we're using 'dd' for NVMEoF, trim off the remote mount directory
                # since that's only valid on the receiver side
                params['file'] = srcfile.replace(params['remote_mount'], "")
            
            # requests Sessions with connection pooling - reuse TCP connections for more performance
            sender_session = requests.Session()
            sender_session.headers.update(({"Authorization": f"Bearer {sender.token}"} if sender.token else None))
            receiver_session = requests.Session()
            receiver_session.headers.update(({"Authorization": f"Bearer {receiver.token}"} if receiver.token else None))

            # set up sender
            logging.debug(f"Runner {get_ident()}: attempt={i} sender request {sender.addr} {params['file']}")
            response = sender_session.post(
                f"http://{sender.addr}/sender/{tool}",
                json=params)
            rcv_payload = response.json()
            logging.debug(f"Runner {get_ident()}: sender response {srcfile} ({rcv_payload['cport'] if 'cport' in rcv_payload else ''}, {rcv_payload['dport'] if 'dport' in rcv_payload else ''})")
            if response.status_code == 404 and 'message' in rcv_payload:
                raise Exception("File not found on sender")
            if response.status_code != 200 or rcv_payload.pop('result') != True:
                raise Exception("Unable to start sender")
            if srcfile == None:
                rcv_payload['duration'] = params['duration']
            else:
                file_size = rcv_payload['size']

            # set up receiver
            if 'remote_mount' in params and params['remote_mount']:
                # patch the srcfile path here too, since it got written by the sender
                rcv_payload['srcfile'] = os.path.join(params['remote_mount'], params['file'])

            rcv_payload['address'] = sender.data_addr
            rcv_payload['file'] = dstfile
            rcv_payload['blocksize'] = params['blocksize']
            rcv_payload['compression'] = params.get('compression', None)

            logging.debug(f"Runner {get_ident()}: receiver request {receiver.addr} {rcv_payload['file']} ({rcv_payload['cport'] if 'cport' in rcv_payload else ''}, {rcv_payload['dport'] if 'dport' in rcv_payload else ''})")
            response = receiver_session.post(
                f"http://{receiver.addr}/receiver/{tool}",
                json=rcv_payload)
            receiver_check = response.json()
            receiver_check['dstfile'] = dstfile
            if srcfile != None:
                receiver_check['size'] = file_size

            logging.debug(f"Runner {get_ident()}: receiver response {dstfile} ({response['cport'] if 'cport' in response else ''}, {response['dport'] if 'dport' in response else ''})")

            if response.status_code != 200 or receiver_check.pop('result') != True:
                raise TransferException("Unable to start receiver")

            # at this point, the file transfer should have been started - check status
            transfer_info = receiver_check.copy()
            transfer_info['timeout'] = timeout
            receiver_check['node'] = 'receiver'
            port = receiver_check['cport']

            # check transfer status with the receiver
            transfer_status = receiver_session.get(
                f"http://{receiver.addr}/{tool}/poll",
                json=receiver_check)

            if transfer_status.status_code != 200 or transfer_status.json()[0] != 0:
                # something went wrong, make sure we clean up before retrying
                response = sender_session.get(
                    f"http://{sender.addr}/free_port/{tool}/{port}",
                    json=receiver_check)
                if response.status_code != 200:
                    raise TransferException(f"Transfer and sender cleanup failed for port {port}, HTTP {response.status_code}: {response.text}")
                raise TransferException(f"Transfer has failed for port {port}, HTTP {transfer_status.status_code}: {transfer_status.text}")

            sender_check = receiver_check
            sender_check['node'] = 'sender'

            # if we're running mem-to-mem tests, the receiver reports size instead of sender
            if srcfile == None and len(transfer_status.json()) > 1:
                transfer_info['size'] = transfer_status.json()[1]

            # check transfer status with the sender
            sndr_response = sender_session.get(
                f"http://{sender.addr}/{tool}/poll",
                json=sender_check)
            if sndr_response.status_code != 200 or sndr_response.json() != 0:
                raise Exception('Transfer has failed')
            logging.debug(f"Runner {get_ident()}: {params['file']} completed")

            # transfer complete
            return transfer_info, datetime.utcnow()
        except TransferException as e:
            if isinstance(e, requests.exceptions.ConnectionError):
                logging.error("Unable to connect to DTN")
            # transfer failed, try again after a short sleep
            logging.error(f"transfer exception, {sender.addr} -> {receiver.addr} {srcfile}: {e}")
            time.sleep(0.5)
            continue
        finally:
            sender_session.close()
            receiver_session.close()
    raise Exception(f"Transfer retries exceeded, file: {srcfile}")

class TransferRunner(object):
    """
    One TransferRunner per file transfer.
    """
    def __init__(self, id, sender, receiver, srcfiles, dstfiles, tool, params, num_workers=1, timeout=None):
        self.id = id
        self.sender = sender
        self.receiver = receiver
        transfer_function = _transfer_file

        # create a list of transfer jobs
        self.joblist = [(srcfile, dstfile) for srcfile, dstfile in zip(srcfiles, dstfiles)]

        # set workers and results collection
        self.num_workers = num_workers
        self.futures = set()
        self.futures_files = {}

        # set transfer statistics
        self.transfer_size = 0
        self.finished_count = 0
        self.cancelled_count = 0
        self.failed_files = []
        self.start_time = datetime.utcnow()
        self.end_time = None

        # create worker threads and start
        self.executor = ThreadPoolExecutor(max_workers=self.num_workers)
        for job in self.joblist:
            future = self.executor.submit(
                transfer_function, sender, receiver, job[0], job[1], tool, params, timeout=timeout)
            self.futures.add(future)
            self.futures_files[future] = job[0]
        logging.debug(f"Runner: started transfer ID {self.id}")

    def scale(self, num_workers):
        #self.executor.scale(num_workers)
        self.executor.set_max_workers(num_workers)

    def _record_futures(self, skip_running=True):
        for future in self.futures:
            if not future.done() and skip_running:
                # skip if still running, otherwise this function will block
                continue
            if future not in self.futures_files:
                # already recorded, ignore this future
                continue
            srcfile = self.futures_files[future]
            try:
                result, end_time = future.result()
                if future.done() and not future.cancelled():
                    self.finished_count += 1
                elif future.cancelled():
                    self.cancelled_count += 1
                self.transfer_size += result.get('size', 0)
                if self.end_time is None or end_time > self.end_time:
                    self.end_time = end_time
            except CancelledError:
                logging.debug(f"Runner: cancelled transfer on {srcfile}")
                self.cancelled_count += 1
                self.failed_files.append(srcfile)
            except Exception as exc:
                logging.debug(f"Runner: failed transfer on {srcfile}: {exc}")
                self.failed_files.append(srcfile)
            finally:
                del self.futures_files[future]

    def poll(self):
        """
        Non-blocking status check for the transfer.
        """
        self._record_futures()
        try:
            throughput = self.transfer_size / (datetime.utcnow() - self.start_time).seconds
        except ZeroDivisionError:
            throughput = 0
        return {
            'Finished': self.finished_count,
            'Unfinished': len(self.futures) - self.finished_count - len(self.failed_files),
            'Cancelled': self.cancelled_count,
            'Failed': len(self.failed_files),
            'throughput': throughput,
            'start': self.start_time.timestamp()
            }

    def wait(self):
        """
        Blocking status check for the transfer. This waits until all futures return.
        """
        self._record_futures(skip_running=False)

    def cancel(self):
        logging.debug(f"Runner: cancelling transfer ID {self.id}")
        for future in self.futures:
            if future.done():
                continue
            future.cancel()

    def shutdown(self):
        logging.debug(f"Runner: shutting down transfer ID {self.id}")
        self.executor.shutdown(cancel_futures=True)

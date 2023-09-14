import random
import time
import unittest
from datetime import datetime

import libs.TransferRunner

def test_transfer_file(sender, receiver, srcfile, dstfile, tool, params, timeout=None, retry=5):
    # fake setup sender/receiver
    time.sleep(0.5)
    port = random.randint(30000,30999)

    # fake an exception
    if '66' in srcfile:
        raise Exception("I failed!")

    # fake wait for /poll since it's blocking
    time.sleep(random.randint(1,3))

    return {
        'result': True,
        'cport': port,
        'dport': port + 1000,
        'dstfile': dstfile,
        'size': random.randint(4096,121238412),
        'timeout': None,
    }, datetime.utcnow()

class TransferRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        # monkey patch for testing
        libs.TransferRunner._transfer_file = test_transfer_file
        return super().setUp()

    def test_runner(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(100)]
        params = {}

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=50)
        i = 0
        while i < 100:
            results = runner.poll()
            if results.get('Unfinished', 1) == 0:
                runner.shutdown()
                break
            time.sleep(0.1)
            i += 1
        
        self.assertLess(i, 100)
        self.assertGreater(i, 40)

        result = runner.poll()
        self.assertEqual(result['Finished'], 99)
        self.assertEqual(result['Unfinished'], 0)
        self.assertEqual(result['Failed'], 1)
        self.assertEqual(result['Cancelled'], 0)
        self.assertGreater(result['throughput'], 10000)

        self.assertIn('a/66.txt', runner.failed_files)
    
    def test_runner_tinylist(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(10)]
        params = {}

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=20)
        i = 0
        while i < 100:
            results = runner.poll()
            if results.get('Unfinished', 1) == 0:
                runner.shutdown()
                break
            time.sleep(0.1)
            i += 1
        
        self.assertLess(i, 50)
        self.assertGreater(i, 20)

        result = runner.poll()
        self.assertEqual(result['Finished'], 10)
        self.assertEqual(result['Unfinished'], 0)
        self.assertEqual(result['Failed'], 0)
        self.assertEqual(result['Cancelled'], 0)
        self.assertGreater(result['throughput'], 10000)

        self.assertIn('a/66.txt', runner.failed_files)

    def test_runner_wait(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(100)]

        params = {}

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=50)
        runner.wait()
        result = runner.poll()
        runner.shutdown()

        self.assertEqual(result['Finished'], 99)
        self.assertEqual(result['Unfinished'], 0)
        self.assertEqual(result['Failed'], 1)
        self.assertEqual(result['Cancelled'], 0)
        self.assertGreater(result['throughput'], 10000)

        self.assertIn('a/66.txt', runner.failed_files)

    def test_runner_cancel(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(100)]

        params = {}

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=50)
        starttime = time.time()
        i = 0
        while i < 100:
            results = runner.poll()
            if results.get('Unfinished', 1) == 0:
                runner.shutdown()
                break
            if time.time() - starttime > 2:
                # cancel the transfer partway through
                runner.cancel()
            time.sleep(0.1)
            i += 1

        self.assertLess(i, 70)
        self.assertGreater(i, 30)
        result = runner.poll()
        self.assertLess(result['Finished'], 99)
        self.assertEqual(result['Unfinished'], 0)
        self.assertGreater(result['Failed'], 1)
        self.assertGreaterEqual(result['Cancelled'], 1)
        self.assertGreater(result['throughput'], 10000)

    def test_runner_scale(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(100)]

        params = {}

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=10)
        starttime = time.time()
        i = 0
        while i < 100:
            results = runner.poll()
            if results.get('Unfinished', 1) == 0:
                runner.shutdown()
                break
            if time.time() - starttime > 1:
                # scale the transfer up partway through
                runner.scale(50)
            time.sleep(0.1)
            i += 1

        self.assertLess(i, 100)
        self.assertGreater(i, 40)
        result = runner.poll()
        self.assertEqual(result['Finished'], 99)
        self.assertEqual(result['Unfinished'], 0)
        self.assertEqual(result['Failed'], 1)
        self.assertEqual(result['Cancelled'], 0)
        self.assertGreater(result['throughput'], 10000)

        self.assertIn('a/66.txt', runner.failed_files)

    def test_runner_threadsafety(self):
        sender = libs.TransferRunner.Sender('man_addr', 'data_addr', 'secret1')
        receiver = libs.TransferRunner.Receiver('man_addr', 'secret2')

        files = [f'a/{i}.txt' for i in range(100)]
        params = {}

        donefiles = set()
        def transfer_threadunsafe(sender, receiver, srcfile, dstfile, tool, params, timeout=None, retry=5):
            # fake setup sender/receiver
            params['file'] = srcfile
            time.sleep(0.5)
            port = random.randint(30000,30999)
            
            src2 = params['file']
            if src2 in donefiles:
                raise Exception("Thread safety violated")
            else:
                donefiles.add(src2)

            # fake wait for /poll since it's blocking
            time.sleep(random.randint(1,3))

            return {
                'result': True,
                'cport': port,
                'dport': port + 1000,
                'dstfile': dstfile,
                'size': random.randint(4096,121238412),
                'timeout': None,
            }, datetime.utcnow()

        # monkey patch for testing
        libs.TransferRunner._transfer_file = transfer_threadunsafe

        runner = libs.TransferRunner.TransferRunner(1, sender, receiver, files, files,
                                'nuttcp', params, num_workers=50)
        i = 0
        while i < 100:
            results = runner.poll()
            if results.get('Unfinished', 1) == 0:
                runner.shutdown()
                break
            time.sleep(0.1)
            i += 1
        
        self.assertLess(i, 100)
        self.assertGreater(i, 20)

        result = runner.poll()
        self.assertLess(result['Finished'], 100)
        self.assertEqual(result['Unfinished'], 0)
        self.assertGreater(result['Failed'], 1)
        self.assertEqual(result['Cancelled'], 0)
        self.assertGreater(result['throughput'], 10000)

if __name__ == '__main__':
    unittest.main(verbosity=2)
"""CPU-only signal fixture. Not a production DB worker."""
import os
import signal
import subprocess
import sys
import threading
import time

mode = os.environ.get('MODE', 'idle')
stop = threading.Event()
child = None


def terminate(signum, _):
    print('SIGNAL', signum, flush=True)
    if mode != 'ignore':
        stop.set()


signal.signal(signal.SIGTERM, terminate)
if mode == 'child':
    child = subprocess.Popen([sys.executable, '-u', '-c',
        'import signal,time,sys; signal.signal(signal.SIGTERM, lambda *a: sys.exit(0)); print("CHILD_READY",flush=True); time.sleep(300)'])
print('READY mode=' + mode, flush=True)
while not stop.wait(.1):
    pass
print('STOP_FETCH', flush=True)
if mode == 'short':
    time.sleep(1)
    print('COMMIT_RESULT', flush=True)
elif mode == 'long':
    print('CHECKPOINT', flush=True)
    print('RELEASE_LEASE', flush=True)
if child:
    child.terminate()
    child.wait(timeout=2)
    print('CHILD_REAPED', flush=True)
print('EXIT', flush=True)

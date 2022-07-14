import threading
import signal
import sys

stopped = False


def stop_threads(*args, **kwargs):
    global stopped
    stopped = True


signal.signal(signal.SIGINT, stop_threads)
signal.signal(signal.SIGTERM, stop_threads)


def excepthook(args):
    if args.exc_type != SystemExit:
        print(f'Exception in thread {args.thread.name}:', file=sys.stderr)
        sys.excepthook(args.exc_type, args.exc_value, args.exc_traceback)
    stop_threads()


threading.excepthook = excepthook


class MyThread(threading.Thread):
    def __init__(self, throw=False):
        super().__init__()
        self.throw = throw

    def run(self):
        while not stopped:
            if self.throw:
                raise ValueError('Ops!')
        print(f'{self.name} exiting...')


threads = [MyThread() for _ in range(10)]
threads.append(MyThread(True))

for t in threads:
    t.start()

for t in threads:
    t.join()

exit_code = 1 if stopped else 0
sys.exit(exit_code)

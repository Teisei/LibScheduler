import threading

import threadpool as tp
from threadpool import NoResultsPending
import time


class Executor(object):
    def __init__(self):
        """
        :param config: dict like object, contains thread_min_size (int),
                       thread_max_size (int), daemonize_thread (bool),
                       process_size (int)
        """

        # a thread pool instance, containing
        # a assigner thread and multiple workers
        self._executor = tp.ThreadPool(
            num_workers=3,
            q_size=0,
            resq_size=0,
            poll_timeout=5
        )

    def start(self):
        i = 0
        while True:
            try:
                time.sleep(0.5)
                self._executor.poll()
                print("Main thread working...")
                print("(active worker threads: %i)" % (threading.activeCount() - 1,))
                if i == 10:
                    print("**** Adding 3 more worker threads...")
                    self._executor.createWorkers(3)
                if i == 20:
                    print("**** Dismissing 2 worker threads...")
                    self._executor.dismissWorkers(2)
                i += 1
            except KeyboardInterrupt:
                print("**** Interrupted!")
                break
            except NoResultsPending:
                print("**** No pending results.")
                break

    def tear_down(self):
        if self._executor.dismissedWorkers:
            self._executor.joinAllDismissedWorkers()

    def run_job_sync(self, job, block=True):
        self._executor.putJob(job, block)
        pass

    def run_func_sync(self, func, args=(), kwargs=None, callback=None):
        """
        scheduler,
        add a work into the executor,
        wait for a worker to fetch it and run
        :param func:
        :param args:
        :param kwargs:
        :param callback:
        :return:
        """
        # self, callable_, args = None, kwds = None, requestID = None,
        # callback = None, exc_callback = _handle_thread_exception
        request = tp.WorkRequest(
            callable_=func,
            args=args,
            kwds=kwargs,
            callback=callback,
            exc_callback=None
        )
        self._executor.putJob(
            job=request,
            block=True,
            timeout=5
        )

    def run_io_func_sync(self, func, args=(), kwargs=None):
        """
        :param func: callable
        :param args: free params
        :param kwargs: named params
        :return whatever the func returns
        """

        return self._io_executor.apply(func, args, kwargs)

    def run_io_func_async(self, func, args=(), kwargs=None, callback=None):
        """
        :param func: callable
        :param args: free params
        :param kwargs: named params
        :calllback: when func is done and without exception, call the callback
        :return whatever the func returns
        """

        return self._io_executor.apply_async(func, args, kwargs, callback)

    def enqueue_io_funcs(self, funcs, block=True):
        """
        run jobs in a fire and forget way, no result will be handled
        over to clients
        :param funcs: tuple/list-like or generator like object, func shall be
                      callable
        """

        return self._io_executor.enqueue_funcs(funcs, block)

    def run_compute_func_sync(self, func, args=(), kwargs={}):
        """
        :param func: callable
        :param args: free params
        :param kwargs: named params
        :return whatever the func returns
        """

        assert self._compute_executor is not None
        return self._compute_executor.apply(func, args, kwargs)

    def run_compute_func_async(self, func, args=(), kwargs={}, callback=None):
        """
        :param func: callable
        :param args: free params
        :param kwargs: named params
        :calllback: when func is done and without exception, call the callback
        :return whatever the func returns
        """

        assert self._compute_executor is not None
        return self._compute_executor.apply_async(func, args, kwargs, callback)

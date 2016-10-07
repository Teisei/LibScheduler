import threading
import Queue

from libscheduler.mycollections.basic import MyQueue


class ScheduleJob(object):
    """
    A job to be scheduled by this scheduler.
    """

    def __init__(self, job_id, func, args=(), kwargs=None):
        self._id = job_id
        self._args = args
        self._kwargs = kwargs
        self._func = func

    def get_args(self):
        return self._args

    def get_kwargs(self):
        return self._kwargs

    # def __cmp__(self, other):
    #     pass

    def __eq__(self, other):
        return isinstance(other, ScheduleJob) and (self.ident() == other.ident())

    def __call__(self, *args, **kwargs):
        self._func(self)

    def stop(self):
        self._stopped = True

    def stopped(self):
        return self._stopped


class Scheduler(object):
    """
    A simple scheduler which schedules the periodic or once event
    First in, First run.
    """

    def __init__(self):
        self._jobs = MyQueue()
        self._wakeup_q = Queue.Queue()
        self._lock = threading.Lock()
        self._thr = threading.Thread(target=self._do_jobs)
        self._thr.deamon = True
        self._started = False

    def start(self):
        """
        Start the schduler which will start the internal thread for scheduling
        jobs. Please do tear_down when doing cleanup
        """

        if self._started:
            # log.logger.info("Scheduler already started.")
            return
        self._started = True

        self._thr.start()

    def tear_down(self):
        """
        Stop the schduler which will stop the internal thread for scheduling
        jobs.
        """

        if not self._started:
            # log.logger.info("Scheduler already tear down.")
            return

        self._wakeup_q.put(True)

    def _do_jobs(self):
        while 1:
            print '>>> start  running a job'
            ready_jobs = self._get_ready_jobs()
            self._do_execution(ready_jobs)
            print '<<< finish running a job'
            # check every 10 seconds to finish
            try:
                done = self._wakeup_q.get(timeout=5)
            except Queue.Empty:
                pass
            else:
                if done:
                    break
        self._started = False
        # log.logger.info("Scheduler exited.")

    def _get_ready_jobs(self):
        ready_jobs = []
        with self._lock:
            job = self._jobs.pop()
        if job:
            ready_jobs.append(job)
        return ready_jobs

    def add_jobs(self, jobs):
        with self._lock:
            job_set = self._jobs
            for job in jobs:
                job_set.add(job)
        self._wakeup()

    def update_jobs(self, jobs):
        with self._lock:
            job_set = self._jobs
            for njob in jobs:
                job_set.discard(njob)
                job_set.add(njob)
        self._wakeup()

    def remove_jobs(self, jobs):
        with self._lock:
            job_set = self._jobs
            for njob in jobs:
                njob.stop()
                job_set.discard(njob)
        self._wakeup()

    def number_of_jobs(self):
        with self._lock:
            return len(self._jobs)

    def _wakeup(self):
        self._wakeup_q.put(None)

    def _do_execution(self, jobs):
        for job in jobs:
            job()

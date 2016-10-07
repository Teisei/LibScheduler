from libscheduler.executor.basic_executor import Executor
from libscheduler.scheduler.basic_scheduler import Scheduler, ScheduleJob


class ResourceManager(object):
    """
    A ResourceManager is a the daemon,
    that runs in the NameNode,
    manage cluster resources,
    schedule and monitor jobs
    """
    def __init__(self):
        self.settings = None
        self._scheduler = Scheduler()
        self._executor = Executor()

    def run(self, jobs):

        # bridge between scheduler and executor
        def _enqueue_io_job(job):
            real_job = job.get_kwargs()["real_job"]
            self.run_io_jobs((real_job,))

        job_id = 0
        for job in jobs:
            j = ScheduleJob(
                job_id=job_id,
                func=_enqueue_io_job,
                args=(),
                kwargs={"real_job": job}
            )
            self._scheduler.add_jobs((j, ))
            # self._executor.run_job_sync(j)

        self._scheduler.start()
        self._executor.start()

        self._wait_for_tear_down()

        self._scheduler.tear_down()
        self._executor.tear_down()

    def _wait_for_tear_down(self):
        pass

    def tear_down(self):
        self._wakeup_queue.put(True)

    def stopped(self):
        return self._stopped

    def run_io_jobs(self, jobs, block=True):
        for job in jobs:
            self._executor.run_job_sync(job, block)

    # def run_compute_job(self, func, args=(), kwargs={}):
    #     self._executor.run_compute_func_sync(func, args, kwargs)
    #
    # def run_compute_job_async(self, func, args=(), kwargs={}, callback=None):
    #     """
    #     @return: AsyncResult
    #     """
    #
    #     return self._executor.run_compute_func_async(func, args,
    #                                                  kwargs, callback)
    #
    # def add_timer(self, callback, when, interval):
    #     return self._timer_queue.add_timer(callback, when, interval)
    #
    # def remove_timer(self, timer):
    #     self._timer_queue.remove_timer(timer)
    #
    # def write_events(self, events):
    #     return self._event_writer.write_events(events)
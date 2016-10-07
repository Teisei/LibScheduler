import unittest as ut
import time
import random

from libscheduler.scheduler.basic_scheduler import Scheduler
from libscheduler.scheduler.basic_scheduler import ScheduleJob


def do_something(job):
    """
    the work the threads will have to do (rather trivial in our example)
    """
    time.sleep(random.randint(1, 5))
    for item in job.get_args():
        if item > 5:
            print 'Something wrong'
            raise RuntimeError("Something extraordinary happened!")
        print item
    return True


class TestBasicScheduler(ut.TestCase):
    def test_run_job(self):
        job = ScheduleJob(
            job_id=1,
            func=do_something,
            args=(1, 2, 3),
            kwargs={}
        )
        job()

    def test_basic_scheduler(self):

        scheduler = Scheduler()
        jobs = [ScheduleJob(job_id=i, func=do_something, args=(i,), kwargs={}) for i in range(0, 10)]
        scheduler.add_jobs(jobs)
        scheduler.start()



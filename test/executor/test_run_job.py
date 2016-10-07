import unittest as ut
import time
import random

import sys

from libscheduler.executor.job import makeJobs
from libscheduler.mycollections.basic import MyQueue


def do_something(data):
    """
    the work the threads will have to do (rather trivial in our example)
    """
    time.sleep(random.randint(1, 5))
    result = round(random.random() * data, 5)
    # just to show off, we throw an exception once in a while
    if result > 5:
        raise RuntimeError("Something extraordinary happened!")
    return result


def print_result(job, result):
    """
    this will be called each time a result is available
    """
    print("**** Result from job #%s: %r" % (job.jobID, result))


def handle_exception(job, exc_info):
    """
    this will be called when an exception occurs within a thread
    this example exception handler does little more than the default handler
    """
    if not isinstance(exc_info, tuple):
        # Something is seriously wrong...
        print(job)
        print(exc_info)
        raise SystemExit
    print("**** Exception occured in job #%s: %s" % \
          (job.jobID, exc_info))


class TestRunJob(ut.TestCase):
    def test_run_a_job(self):
        data = [random.randint(1, 3) for i in range(20)]
        jobs = makeJobs(do_something, data, print_result, handle_exception)

        result_queue = MyQueue()
        for job in jobs:
            try:
                result = job.callable(*job.args, **job.kwds)
                print 'results of job[%s] %s' % (job.__str__(), result)
                result_queue.add((job, result))
            except:
                print 'job[%s] has exception' % job.__str__()
                job.exception = True
                result_queue.add((job, sys.exc_info()))

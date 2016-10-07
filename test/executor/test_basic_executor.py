import unittest as ut
import time
import random
from libscheduler.executor.basic_executor import Executor
from libscheduler.executor.job import makeJobs


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


class TestBasicExecutor(ut.TestCase):
    def test_1(self):
        executor = Executor()

        data = [random.randint(1, 3) for i in range(20)]
        jobs = makeJobs(do_something, data, print_result, handle_exception)
        data = [((random.randint(1, 3),), {}) for i in range(20)]
        jobs.extend(makeJobs(do_something, data, print_result, handle_exception))

        for job in jobs:
            executor.run_job_sync(job)

        executor.start()
        executor.tear_down()

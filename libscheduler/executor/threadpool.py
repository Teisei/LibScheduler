# -*- coding: UTF-8 -*-
"""Easy to use object-oriented thread pool framework.

A thread pool is an object that maintains a pool of worker threads to perform
time consuming operations in parallel. It assigns jobs to the threads
by putting them in a work job queue, where they are picked up by the
next available thread. This then run the job in the
background and puts the results in another queue.

The thread pool object can then collect the results from all threads from
this queue as soon as they become available or after all threads have
finished their work. It's also possible, to define callbacks to handle
each result as it comes in.

The basic concept and some code was taken from the book "Python in a Nutshell,
2nd edition" by Alex Martelli, O'Reilly 2006, ISBN 0-596-10046-9, from section
14.5 "Threaded Program Architecture". I wrapped the main program logic in the
ThreadPool class, added the Job class and the callback system and
tweaked the code here and there. Kudos also to Florent Aide for the exception
handling mechanism.

Basic usage::

    # >>> pool = ThreadPool(poolsize)
    # >>> jobs = makeJobs(some_callable, list_of_args, callback)
    # >>> [pool.putJob(req) for req in jobs]
    # >>> pool.wait()

See the end of the module code for a brief, annotated usage example.

Website : http://chrisarndt.de/projects/threadpool/

"""
from job import Job, makeJobs

__docformat__ = "restructuredtext en"

__all__ = [
    'NoResultsPending',
    'NoWorkersAvailable',
    'ThreadPool',
    'WorkerThread'
]

__author__ = "Christopher Arndt"
__version__ = '1.3.2'
__license__ = "MIT license"

# standard library modules
import sys
import threading

try:
    import Queue  # Python 2
except ImportError:
    import queue as Queue  # Python 3


# exceptions
class NoResultsPending(Exception):
    """All work requests have been processed."""
    pass


class NoWorkersAvailable(Exception):
    """No worker threads available to process remaining requests."""
    pass


# classes
class WorkerThread(threading.Thread):
    """Background thread connected to the requests/results queues.

    A worker thread sits in the background and picks up work requests from
    one queue and puts the results in another until it is dismissed.

    """

    def __init__(self, jobs_queue, results_queue, poll_timeout=5, **kwds):
        """Set up thread in daemonic mode and start it immediatedly.

        ``requests_queue`` and ``results_queue`` are instances of
        ``Queue.Queue`` passed by the ``ThreadPool`` class when it creates a
        new worker thread.

        """
        threading.Thread.__init__(self, **kwds)
        self.setDaemon(1)
        self._jobs_queue = jobs_queue
        self._results_queue = results_queue
        self._poll_timeout = poll_timeout
        self._dismissed = threading.Event()
        self.start()

    def run(self):
        """Repeatedly process the job queue until told to exit."""
        while True:
            if self._dismissed.isSet():
                # we are dismissed, break out of loop
                break
            # get next work job. If we don't get a new job from the
            # queue after self._poll_timout seconds, we jump to the start of
            # the while loop again, to give the thread a chance to exit.
            try:
                job = self._jobs_queue.get(True, self._poll_timeout)
            except Queue.Empty:
                continue
            else:
                if self._dismissed.isSet():
                    # we are dismissed, put back job in queue and exit loop
                    self._jobs_queue.put(job)
                    break
                try:
                    result = job.callable(*job.args, **job.kwds)
                    print '%s is process results %s' % (threading.currentThread().getName(), result)
                    self._results_queue.put((job, result))
                except:
                    job.exception = True
                    self._results_queue.put((job, sys.exc_info()))

    def dismiss(self):
        """Sets a flag to tell the thread to exit when done with current job.
        """
        self._dismissed.set()

class ThreadPool:
    """A thread pool, distributing work jobs and collecting results.

    See the module docstring for more information.

    """

    def __init__(self, num_workers, q_size=0, resq_size=0, poll_timeout=5):
        """Set up the thread pool and start num_workers worker threads.

        ``num_workers`` is the number of worker threads to start initially.

        If ``q_size > 0`` the size of the work *job queue* is limited and
        the thread pool blocks when the queue is full and it tries to put
        more work jobs in it (see ``putJob`` method), unless you also
        use a positive ``timeout`` value for ``putJob``.

        If ``resq_size > 0`` the size of the *results queue* is limited and the
        worker threads will block when the queue is full and they try to put
        new results in it.

        .. warning:
            If you set both ``q_size`` and ``resq_size`` to ``!= 0`` there is
            the possibilty of a deadlock, when the results queue is not pulled
            regularly and too many jobs are put in the work Jobs queue.
            To prevent this, always set ``timeout > 0`` when calling
            ``ThreadPool.putJob()`` and catch ``Queue.Full`` exceptions.

        """
        self._jobs_queue = Queue.Queue(q_size)
        self._results_queue = Queue.Queue(resq_size)
        self.workers = []
        self.dismissedWorkers = []
        self.workJobs = {}
        self.createWorkers(num_workers, poll_timeout)

    def createWorkers(self, num_workers, poll_timeout=5):
        """Add num_workers worker threads to the pool.

        ``poll_timout`` sets the interval in seconds (int or float) for how
        ofte threads should check whether they are dismissed, while waiting for
        jobs.

        """
        for i in range(num_workers):
            self.workers.append(WorkerThread(self._jobs_queue,
                                             self._results_queue, poll_timeout=poll_timeout))

    def dismissWorkers(self, num_workers, do_join=False):
        """Tell num_workers worker threads to quit after their current task."""
        dismiss_list = []
        for i in range(min(num_workers, len(self.workers))):
            worker = self.workers.pop()
            worker.dismiss()
            dismiss_list.append(worker)

        if do_join:
            for worker in dismiss_list:
                worker.join()
        else:
            self.dismissedWorkers.extend(dismiss_list)

    def joinAllDismissedWorkers(self):
        """Perform Thread.join() on all worker threads that have been dismissed.
        """
        for worker in self.dismissedWorkers:
            worker.join()
        self.dismissedWorkers = []

    def putJob(self, job, block=True, timeout=None):
        """Put work job into work queue and save its id for later."""
        assert isinstance(job, Job)
        # don't reuse old work requests
        assert not getattr(job, 'exception', None)
        self._jobs_queue.put(job, block, timeout)
        self.workJobs[job.jobID] = job

    def poll(self, block=False):
        """Process any new results in the queue."""
        while True:
            # still results pending?
            if not self.workJobs:
                raise NoResultsPending
            # are there still workers to process remaining requests?
            elif block and not self.workers:
                raise NoWorkersAvailable
            try:
                # get back next results
                job, result = self._results_queue.get(block=block)
                # has an exception occured?
                if job.exception and job.exc_callback:
                    job.exc_callback(job, result)
                # hand results to callback, if any
                if job.callback and not \
                        (job.exception and job.exc_callback):
                    job.callback(job, result)
                del self.workJobs[job.jobID]
            except Queue.Empty:
                break

    def wait(self):
        """Wait for results, blocking until all have arrived."""
        while 1:
            try:
                self.poll(True)
            except NoResultsPending:
                break


################
# USAGE EXAMPLE
################

if __name__ == '__main__':
    import random
    import time


    # the work the threads will have to do (rather trivial in our example)
    def do_something(data):
        time.sleep(random.randint(1, 5))
        result = round(random.random() * data, 5)
        # just to show off, we throw an exception once in a while
        if result > 5:
            raise RuntimeError("Something extraordinary happened!")
        return result


    # this will be called each time a result is available
    def print_job(job, result):
        print("**** Result from job #%s: %r" % (job.jobID, result))


    # this will be called when an exception occurs within a thread
    # this example exception handler does little more than the default handler
    def handle_exception(job, exc_info):
        if not isinstance(exc_info, tuple):
            # Something is seriously wrong...
            print(job)
            print(exc_info)
            raise SystemExit
        print("**** Exception occured in job #%s: %s" % \
              (job.jobID, exc_info))


    # assemble the arguments for each job to a list...
    data = [random.randint(1, 10) for i in range(20)]
    # ... and build a Job object for each item in data
    jobs = makeJobs(do_something, data, print_job, handle_exception)
    # to use the default exception handler, uncomment next line and comment out
    # the preceding one.
    # jobs = makeJobs(do_something, data, print_result)

    # or the other form of args_lists accepted by makeJobs: ((,), {})
    data = [((random.randint(1, 10),), {}) for i in range(20)]
    jobs.extend(
        makeJobs(do_something, data, print_job, handle_exception)
        # makeJobs(do_something, data, print_result)
        # to use the default exception handler, uncomment next line and comment
        # out the preceding one.
    )

    # we create a pool of 3 worker threads
    print("Creating thread pool with 3 worker threads.")
    main = ThreadPool(3)

    # then we put the work jobs in the queue...
    for req in jobs:
        main.putJob(req)
        print("Work job #%s added." % req.jobId)
    # or shorter:
    # [main.putJob(req) for req in jobs]

    # ...and wait for the results to arrive in the result queue
    # by using ThreadPool.wait(). This would block until results for
    # all work jobs have arrived:
    # main.wait()

    # instead we can poll for results while doing something else:
    i = 0
    while True:
        try:
            time.sleep(0.5)
            main.poll()
            print("Main thread working...")
            print("(active worker threads: %i)" % (threading.activeCount() - 1,))
            if i == 10:
                print("**** Adding 3 more worker threads...")
                main.createWorkers(3)
            if i == 20:
                print("**** Dismissing 2 worker threads...")
                main.dismissWorkers(2)
            i += 1
        except KeyboardInterrupt:
            print("**** Interrupted!")
            break
        except NoResultsPending:
            print("**** No pending results.")
            break
    if main.dismissedWorkers:
        print("Joining all dismissed worker threads...")
        main.joinAllDismissedWorkers()

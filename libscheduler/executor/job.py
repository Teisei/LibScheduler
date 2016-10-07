import traceback


# internal module helper functions
def _handle_thread_exception(job, exc_info):
    """Default exception handler callback function.

    This just prints the exception info via ``traceback.print_exception``.

    """
    traceback.print_exception(*exc_info)


# utility functions
def makeJobs(callable_, args_list, callback=None,
             exc_callback=_handle_thread_exception):
    """Create several work jobs for same callable with different arguments.

    Convenience function for creating several work jobs for the same
    callable where each invocation of the callable receives different values
    for its arguments.

    ``args_list`` contains the parameters for each invocation of callable.
    Each item in ``args_list`` should be either a 2-item tuple of the list of
    positional arguments and a dictionary of keyword arguments or a single,
    non-tuple argument.

    See docstring for ``WorkJob`` for info on ``callback`` and
    ``exc_callback``.

    """
    jobs = []
    for item in args_list:
        if isinstance(item, tuple):
            jobs.append(
                Job(callable_, item[0], item[1], callback=callback,
                    exc_callback=exc_callback)
            )
        else:
            jobs.append(
                Job(callable_, [item], None, callback=callback,
                    exc_callback=exc_callback)
            )
    return jobs


class Job:
    """A job to execute a callable for putting in the job queue later.

    See the module function ``makeJobs`` for the common case
    where you want to build several ``WorkJob`` objects for the same
    callable but with different arguments for each call.

    """

    def __init__(self, callable_, args=None, kwds=None, jobID=None,
                 callback=None, exc_callback=_handle_thread_exception):
        """Create a work job for a callable and attach callbacks.

        A work job consists of the a callable to be executed by a
        worker thread, a list of positional arguments, a dictionary
        of keyword arguments.

        A ``callback`` function can be specified, that is called when the
        results of the job are picked up from the result queue. It must
        accept two anonymous arguments, the ``WorkJob`` object and the
        results of the callable, in that order. If you want to pass additional
        information to the callback, just stick it on the job object.

        You can also give custom callback for when an exception occurs with
        the ``exc_callback`` keyword parameter. It should also accept two
        anonymous arguments, the ``WorkJob`` and a tuple with the exception
        details as returned by ``sys.exc_info()``. The default implementation
        of this callback just prints the exception info via
        ``traceback.print_exception``. If you want no exception handler
        callback, just pass in ``None``.

        ``jobID``, if given, must be hashable since it is used by
        ``ThreadPool`` object to store the results of that work job in a
        dictionary. It defaults to the return value of ``id(self)``.

        """
        if jobID is None:
            self.jobID = id(self)
        else:
            try:
                self.jobID = hash(jobID)
            except TypeError:
                raise TypeError("jobID must be hashable.")
        self.exception = False
        self.callback = callback
        self.exc_callback = exc_callback
        self.callable = callable_
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):
        return "<WorkJob id=%s args=%r kwargs=%r exception=%s>" % \
               (self.jobID, self.args, self.kwds, self.exception)
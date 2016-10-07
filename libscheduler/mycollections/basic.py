class MyStack():
    """
    A simple implementation of Stack.
    First in, Last out.
    """

    def __init__(self):
        self.stack = []

    def empty(self):
        return self.stack == []

    def push(self, data):
        self.stack.append(data)

    def pop(self):
        if self.empty():
            return None;
        else:
            return self.stack.pop(-1)

    def top(self):
        if self.empty():
            return None
        else:
            return self.stack[-1]

    def length(self):
        return len(self.stack)


class MyQueue():
    """
    A simple implementation of Queue.
    First in, First out.
    """

    def __init__(self):
        self.queue = []

    def empty(self):
        return self.queue == []

    def enqueue(self, data):
        self.queue.append(data)

    def dequeue(self):
        if self.empty():
            return None
        else:
            return self.queue.pop(0)

    def add(self, data):
        self.enqueue(data)

    def pop(self):
        return self.dequeue()

    def get(self):
        return self.pop()

    def head(self):
        if self.empty():
            return None
        else:
            return self.queue[0]

    def length(self):
        return len(self.queue)

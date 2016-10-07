import unittest as ut
from libscheduler.mycollections.basic import MyQueue


class TestBasicDataStructures(ut.TestCase):
    def test_queue(self):
        q = MyQueue()
        for i in range(0, 10):
            q.add(i)
        while not q.empty():
            print q.pop()

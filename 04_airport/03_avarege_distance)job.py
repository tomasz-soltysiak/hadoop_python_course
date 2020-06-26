from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import re
class MRFligths(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]
    def mapper(self, _, line):
        year,items=line.split('\t')
        year=(year[1:-1])
        items=items[1:-1]
        month,day,airline,distance=items.split(', ')
        distance=int(distance)
        yield None,distance

    def reducer(self, key, values):
        num=0
        val=0
        for value in values:
            num=num+1
            val=val+value
        yield key,val/num



if __name__ == '__main__':
    MRFligths.run()

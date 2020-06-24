from mrjob.job import MRJob
from mrjob.step import MRStep

class Wordcount(MRJob):
    def mapper(self, _,line):
        for x in line.split():
            yield x.lower(),1

    def reducer(self,key,values):
        yield key, sum(values)

if __name__ == '__main__':
    Wordcount.run()
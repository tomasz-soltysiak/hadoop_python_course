from mrjob.job import MRJob

class MRWordCount(MRJob):
    def mapper(self,_,line):
        yield 'chars',len(line)

    def reduce(self,key,values):
        yield key, values


if __name__ == '__main__':
    MRWordCount.run()
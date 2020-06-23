from mrjob.job import MRJob
#fsikao
class MRWordCount(MRJob):
    def mapper(self,_,line):
        yield 'chars',len(line)
        yield 'words', len(line.split())

    def reduce(self,key,values):
        yield key, values


if __name__ == '__main__':
    MRWordCount.run()
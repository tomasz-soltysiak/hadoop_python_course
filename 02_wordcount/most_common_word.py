from mrjob.job import MRJob
from mrjob.step import MRStep
import re
Word_re=re.compile(r'[\w]+')


class MRMostFrequentWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer_max_frequent_word)
        ]

    def mapper_get_words(self,_,line):
        word=Word_re.findall(line)
        for x in word:
            yield x.lower(),1

    def reducer_count_words(self, key, values):
        yield key, sum(values)


    def mapper_get_keys(self,key,value):
        yield None, (value,key)

    def reducer_max_frequent_word(self,key, value):
        yield max(value)



if __name__ == '__main__':
    MRMostFrequentWord.run()
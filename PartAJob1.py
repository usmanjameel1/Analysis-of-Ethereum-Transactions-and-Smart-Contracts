from mrjob.job import MRJob

import time
import re

WORD_REGEX = re.compile(r"\b\w+\b")

class PartAJob1(MRJob):

    def mapper(self, _,line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                timeframe = int(fields[6])
                month = time.strftime("%m", time.gmtime(timeframe))
                year = time.strftime("%y", time.gmtime(timeframe))
                yield ((month, year), 1)

        except:
            pass


    def reducer(self,word,counts):
        yield(word, sum(counts))



if __name__ == '__main__':
    PartAJob1.JOBCONF = {'mapreduce.job.reduces': '10'}
    PartAJob1.run()

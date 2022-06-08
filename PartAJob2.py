from mrjob.job import MRJob

import time
import re

WORD_REGEX = re.compile(r"\b\w+\b"

class PartAJob2(MRJob):

    def mapper(self,_, line):
        fields = line.split(",")
        try:
            if len(fields)==7:
                timeframe = int(fields[6])
                gasprice = int(fields[5])
                month = time.strftime("%m", time.gmtime(timeframe))
                year = time.strftime("%y", time.gmtime(timeframe))
                yield((month,year),(gasprice,1))

        except:
            pass



    def reducer2(self, date, price):
        average = 0
        count = 0
        for x, y in price:
            average = (average*count+x*y)/(count + y)
            count = count +y
        return(date, (average,count))

    def combiner(self, date, price):
        yield self.reducer2(date,price)

    def reducer(self,date,price):
        date, (average,count) = self.reducer2(date,price)
        yield(date,average)


if __name__ == '__main__':
    PartAJob2.run()

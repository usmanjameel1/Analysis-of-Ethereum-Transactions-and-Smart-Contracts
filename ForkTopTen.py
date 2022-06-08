
from mrjob.job import MRJob
import re

class ForkTopTen(MRJob):
    def mapper(self, _, line):



        lines = line.split("\t")

        if len(lines) == 2:

            date = lines[0].replace('\"', '')


            array = lines[1]
            breakdown = array.split(",")
            if len(breakdown) == 2:
                addre = str(breakdown[0].replace('"', '').replace('[', ''))
                values = float(breakdown[1].replace(' ','').replace(']',''))
                amountAndDate = (values, date)

        yield(None, (addre, amountAndDate))



    def reducer(self, key, values):
        inOrder = sorted(values, reverse=True, key = lambda y:y[1][0])[:10]
        for x in inOrder:
            yield(x[1][1], '{},{}'.format(x[0], x[1][0]))


if __name__ == '__main__':
    ForkTopTen.run()

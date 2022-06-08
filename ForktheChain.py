from mrjob.job import MRJob
import time
import re

class ForktheChain(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        chosen_timeframe = False
        try:
            if (len(fields)==7):
                value = int(fields[3])
                destination_address = str(fields[2])

                date = int(fields[6])
                timeformatting = time.strftime("%d %b %Y", time.gmtime(date))

                start_time = time.strptime("23 Nov 2016", "%d %b %Y")
                end_time = time.strptime("23 Jan 2017", "%d %b %Y")

                if (start_time <= time.gmtime(date) and  time.gmtime(date) <= end_time):
                    chosen_timeframe = True
                else:
                    chosen_timeframe = False

                if not (destination_address == "null" or value == 0):
                     if (chosen_timeframe == True):
                         yield(timeformatting, (destination_address, value))
        except:
            pass



        def reducer(self, date, values):
            for x in values:
                totalvalue = totalvalue + int(x[1])
                val0 = x[0]
            yield(val0, ('{},{}'.format(totalvalue, date)))


if __name__ == '__main__':
    ForktheChain.JOBCONF = {'mapreduce.job.reduces': '25'}
    ForktheChain.run()

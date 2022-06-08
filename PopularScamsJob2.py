from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class PARTDscams3(MRJob):
    def mapper1(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                address1 = fields[2]
                yield address1, (1,0)

            else:
                line = json.loads(lines)
                keys = line["result"]
                for i in keys:
                    record = line["result"][i]
                    category = record["category"]
                    addresses = record["addresses"]
                    status = record["status"]

                    for j in addresses:
                        yield j, (2, category,status)
        except:
            pass

    def reducer1(self, key, values):
        tvalue=0
        category=None
        status = None
        for k in values:
            if k[0] == 1:
                tvalue = tvalue + k[0]
            else:
                category = k[1]
                status = k[2]
        if category is not None and status is not None:
            yield (status,category), tvalue

    def mapper2(self,key,value):
        yield(key,value)
    def reducer2(self, key, value):
        yield(key,sum(value))
    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
    PARTDscams3.JOBCONF = {'mapreduce.job.reduces' :'20'}
    PARTDscams3.run()

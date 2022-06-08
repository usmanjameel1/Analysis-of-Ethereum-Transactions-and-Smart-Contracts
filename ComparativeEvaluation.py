import pyspark

def transact(line):
        try:
                fields = line.split(",")
                if len(fields)!=7:
                        return False
                int(fields[3])
                return True

        except:
                return False



def contract(line):
        try:
                fields = line.split(",")
                if len(fields)!=5:
                        return False
                return True
        except:
                return False




sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions")
validtransaction = transactions.filter(transact)
maptransaction = validtransaction.map(lambda i : (i.split(",")[2], int(i.split(",")[3])))
aggregatetransaction = maptransaction.reduceByKey(lambda c,d : c+d)
contracts = sc.textFile("/data/ethereum/contracts")
validcontracts = contracts.filter(contract)
mapcontracts = validcontracts.map(lambda j: (j.split(",")[0], None))
joined = aggregatetransaction.join(mapcontracts)

t10 = joined.takeOrdered(10, key = lambda l: -l[1][0])


with open('output.txt', 'w') as f:
        for value in t10:
                f.write("{}:{}\n".format(value[0],value[1][0]))

import pyspark
import time

sc = pyspark.SparkContext()


def is_good_line_transact(line):
        try:
                fields = line.split(",")
                if len(fields)!= 7:
                        return False
                float(fields[5])
                float(fields[6])
                return True
        except:
                return False


def is_good_line_contract(line):
        try:
                fields = line.split(",")
                if len(fields) != 5:
                        return False
                float(fields[3])
                return True

        except:
                False


def is_good_line_block(line):
        try:
                fields = line.split(",")
                if len(fields)!=9:
                        return False

                float(fields[0])
                float(fields[3])
                float(fields[7])
                return True

        except:
                return False


transaction_data_set = sc.textFile('/data/ethereum/transactions')
contract_data_set = sc.textFile('/data/ethereum/contracts')
block_data_set = sc.textFile('/data/ethereum/blocks')

clean_transaction_data_set = transaction_data_set.filter(is_good_line_transact)
clean_contract_data_set = contract_data_set.filter(is_good_line_contract)
clean_block_data_set = block_data_set.filter(is_good_line_block)

time_1 = clean_transaction_data_set.map(lambda a: (float(a.split(',')[6]), float(a.split(',')[5])))
date_X = time_1.map(lambda(x,y): (time.strftime("%y.%m", time.gmtime(x)), (y,1)))
time_2 = date_X.reduceByKey(lambda (x1, y1), (x2, y2): (x1+x2, y1+y2)).map(lambda b: (b[0], (b[1][0]/b[1][1])))

agout = time_2.sortByKey(ascending=True)
agout.saveAsTextFile('AverageGas')


blocks = clean_contract_data_set.map(lambda m: (m.split(',')[3], 1))


blockdifference = clean_block_data_set.map(lambda x: (x.split(',')[0], (int(x.split(',')[3]), int(x.split(',')[6]), time.strftime("%y.%m", time.gmtime(float(x.split(',')[7]))))))
results = blockdifference.join(blocks).map(lambda (id, ((w, x, y), z)): (y, ((w,x), z)))
timeout = results.reduceByKey(lambda ((x1,y1), z1) , ((x2, y2), z2): ((x1 + x2, y1 + y2), z1+z2)).map(lambda w: (w[0], (float(w[1][0][0]/w[1][1]), w[1][0][1]/ w[1][1]))).sortByKey(ascending=True)
timeout.saveAsTextFile('TimeDifference')

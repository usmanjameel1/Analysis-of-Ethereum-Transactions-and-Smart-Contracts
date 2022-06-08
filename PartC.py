from mrjob.job import MRJob
from mrjob.step import MRStep


class PartC(MRJob):

	def mapper_1(self, _, line):
		fields = line.split(",")

		try:
			if len(fields) == 9:
				miner = fields[2]
				size = fields[4]
				yield (miner, int(size))

		except:
			pass



	def reducer_1(self, miner, size):
		try:
			yield(miner, sum(size))

		except:
			pass


	def mapper_2(self, miner, total_size):
		try:
			yield(None, (miner,total_size))

		except:
			pass



	def reducer_2(self, _, mine_size):
		try:
			sort_size = sorted(mine_size, reverse = True, key = lambda x:x[1])
			for x in sort_size[:10]:
				yield(x[0],x[1])

		except:
			pass


	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]



if __name__ == '__main__':
	PartC.run()

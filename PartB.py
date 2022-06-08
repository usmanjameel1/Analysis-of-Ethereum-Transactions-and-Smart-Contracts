from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEX = re.compile(r"\b\w+\b")

class PartB(MRJob):
	def mapper_1(self, _, line):
		fields = line.split(",")

		try:
			if len(fields) == 7:
				address = fields[2]
				value = int(fields[3])
				yield address, (1,value)

			elif len(fields) == 5:
				address_1 = fields[0]
				yield address_1, (2,1)

		except:
			pass



	def reducer_1(self, key, values):
		x = False
		all_values = []

		for y in values:
			if y[0]==1:
				all_values.append(y[1])

			elif y[0] == 2:
				x = True

		if x:
			yield key, sum(all_values)




	def mapper_2(self, key,value):
		yield None, (key,value)



	def reducer_2(self, _, keys):
		sorted_values = sorted(keys, reverse = True, key = lambda x: x[1])

		for y in sorted_values[:10]:
			yield y[0], y[1]




	def steps(self):
		return [MRStep(mapper = self.mapper_1, reducer=self.reducer_1), MRStep(mapper = self.mapper_2, reducer = self.reducer_2)]



if __name__ == '__main__':
	PartB.run()

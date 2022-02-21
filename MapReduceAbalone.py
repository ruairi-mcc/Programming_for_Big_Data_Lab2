from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")

class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_length_male,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_length_male(self, _, line):
        
        # Length of all of the Male samples
        
        data = DATA_RE.findall(line)
        if "M" in data:
            sep_W = float(data[1])
            yield ("length", sep_W)

    def reducer_get_avg(self, key, values):
        
        # return the average of the length of the male samples
        
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
        yield ("male length avg", round(total,1) / size)
if __name__ == '__main__':
    MRProb2_3.run()

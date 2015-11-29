'''
Below shows how to run line by line spark.
You can also run an entire python script as:
bin/spark-submit my_script.pyspark
'''

import pyspark as ps
import random

'''Using Spark to randomly select 100000 draws between 0 and 1
and look at how many are less than .51'''

sc = ps.SparkContext()
flips = 100000
heads = sc.parallelize(xrange(flips))
		.map(lambda x: random.random())
		.filter(lambda i: i< .51)
		.count()

ratio = float(heads)/float(flips)

print(heads)
print(ratio)

'''Function for creating prime numbers'''
def is_prime(num):
	fact_min = 2
	fact_max = int(num**.5)+1
	for fact in xrange(fact_min, fact_max):
		if num % fact == 0:
			return False
	return True

'''Function for Odds'''
def is_odd(num):
	if num % 2 == 0:
		return False
	return True

'''Without Spark Looking at Prime Values and Odds'''
la = []
la2 = []
for i in xrange(2,100):
	la.append(is_prime(i))

for i in xrange(2,100):
	la2.append(is_odd(i))

'''With Spark'''
numbers = xrange(2,100)
primes = sc.parallelize(numbers) \
			.filter(is_prime) \
			.collect()
print primes

odds = sc.parallelize(numbers) \
			.filter(is_odd) \
			.collect()
print odds

sc.parallelize([1,3,2,2,1]).distinct().collect()
sc.parallelize([1,3,2,2,1]).sortBy(lambda x: x).collect()

'''Create imput.txt file'''
%%writefile imput.txt
hello world
another line
yet another line
yet another another line

'''Counts how many lines are in the flie'''
sc.textFile('imput.txt') \
	.map(lambda x: x.split()) \
	.count()

'''Counts how many words are in the file'''
sc.textFile('imput.txt') \
	.flatMap(lambda x: x.split()) \
	.count()

'''We can see this here:'''
'''map vs flatMap'''
sc.textFile('imput.txt') \
	.map(lambda x: x.split()) \
	.collect()

'''Counts how many words are in the file'''
sc.textFile('imput.txt') \
	.flatMap(lambda x: x.split()) \
	.collect()

'''Famous wordcount example'''
data = sc.textFile('input.txt')

'''list of tuples'''
wordCount = data.flatMap(lambda line: line.split())\
				.map(lambda word: (word, 1))\
				.reduceByKey(lambda i,j: i+j)

'''default dictionary'''
wordCount2 = data.flatMap(lambda line: line.split())\
				.countByValue()


'''Another flatMap example'''
lines = sc.parallelize(['hello world', 'hi', 'oh hi there world'])
words = lines.flatMap(lambda line: line.split())

'''Actions'''
lines.count()
lines.collect()
lines.first()
lines.take(2)

words.count()
words.collect()
words.first()
words.take(2)


numb = sc.parallelize([1,2,3,3])
numbers = sc.parallelize([3,3,2,1,1,1,2,3,4,5])

'''Basic transformations'''
numb.map(lambda x: x+1)
#flatMap is common with text to extract words
numb.filter(lambda x: x != 1)
numb.distinct()
numb.sample(True, .5, 2) #as an action see takeSample
'''withReplacement=, proportion, seed'''

'''Basic actions'''
numb.collect()
numb.take(2)
numb.top(2)
numb.takeSample(True, 10, 3)
numbers.takeSample(True, 20, 3)
numbers.mean()
numbers.sum()
numbers.count()

'''Actions for Key Value pairs
Here I am using the imput.txt file
from above again.'''

'''Create imput.txt file'''
%%writefile imput2.txt
I have
another example
of some text stuff
that can
help us use some rdd 
like stuff 


'''more practice'''
data2 = sc.textFile('imput2.txt')
data2.map(lambda line: line.split()).collect()

#This is a default dict.  To use spark countByValue need as an RDD
datastuff2 = data2.flatMap(lambda line: line.split()) #.countByValue() 

#This returns a pair RDD - gives word count
datastuff2 = data2.flatMap(lambda line: line.split())\
					.map(lambda word: (word,1))\
					.reduceByKey(lambda i,j: i+j) 

#This doesn't make a lot of sense here, but this gives you 
#a list of all values for a particular key
#Getting Tired = Don't think this stuff here is right
datastuff2 = data2.flatMap(lambda line: line.split())
datastuff2 = data2.flatMap(lambda line: line.split())\
					.map(lambda word: (word,1))\
					.groupByKey() 

datastuff2.keys()
datastuff2.values()

'''
Working with sales.txt dataset
'''
%%writefile sales.txt
#ID    Date           Store   State  Product    Amount
101    11/13/2014     100     WA     331        300.00
104    11/18/2014     700     OR     329        450.00
102    11/15/2014     203     CA     321        200.00
106    11/19/2014     202     CA     331        330.00
103    11/17/2014     101     WA     373        750.00
105    11/19/2014     202     CA     321        200.00

data = sc.textFile('sales.txt')


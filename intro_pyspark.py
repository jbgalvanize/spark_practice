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



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

'''Combining rdds'''
rdd = sc.parallelize([1,1,2,3])
rdd1 = sc.parallelize([1,10,2,3,4,5])
rdd2 = sc.parallelize([1,6,2,3,7,8])

rdd1.intersection(rdd2).collect()
rdd1.union(rdd2).collect()
(rdd + rdd).collect()
(rdd1 + rdd2).collect()

'''Working with Key Value Pairs'''
#https://spark.apache.org/docs/1.1.1/api/python/pyspark.rdd.RDD-class.html
tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
sc.parallelize(tmp).sortByKey().first()

sc.parallelize(tmp).sortByKey(True, 1).collect()
#True indicates ascending, false indicates descending
sc.parallelize(tmp).sortByKey(True, 2).collect()
#The result of this is the same
#The second argument is the nnymber of partitions was done
#to compute.

#You can extend an rdd
tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
#This puts capital letter keys first
sc.parallelize(tmp2).sortByKey().collect()

#We can sort by key or value
sc.parallelize(tmp).sortBy(lambda x: x[0]).collect()
sc.parallelize(tmp).sortBy(lambda x: x[1]).collect()

#Can still do the same commands as in traditional (non pair) RDD
sc.parallelize(tmp).filter(lambda x: x[0] == '1').collect()
sc.parallelize(tmp2).filter(lambda x: x[1] < 5).collect()

#Apparently glom can helpful with computation speed? I see what is happening, but I 
#don't really get this...
rdd = sc.parallelize([1, 1, 2, 3], 2)
rdd = sc.parallelize([1, 1, 2, 3], 3)
sorted(rdd.glom().collect())

'''We can also join our key value pairs on the keys in a similar way to 
the way we join in SQL'''
rdd3 = sc.parallelize([(1,2),(3,4),(3,6),(2,5)])
rdd4 = sc.parallelize([(3,9),(5,9)])

#Gives all keyValue pairs for where key is in the first but not the second
rdd3.subtractByKey(rdd4).collect()
rdd4.subtractByKey(rdd3).collect()

#join on key give value as a list of values
rdd3.join(rdd4).collect()
rdd3.rightOuterJoin(rdd4).collect()
rdd3.leftOuterJoin(rdd4).collect()

#group data from both RDDs sharing the same key
rdd3.cogroup(rdd4)

'''Part 2'''
#Read in our data
file_rdd = sc.textFile('data/toy_data.txt')

#1
import json
a_rdd = file_rdd.map(lambda line: json.loads(line))\
                .map(lambda d: (d.keys()[0], int(d.values()[0])))
a_rdd.collect()

#2
rdd_more = a_rdd.filter(lambda x: x[1] > 5)
rdd_more.collect()

#3
max_cooks = a_rdd.reduceByKey(lambda x,y: max(x,y))

#4
vals = a_rdd.values().sum()

'''Part 3
This is in your notes in the black book. 
'''

'''Part 4'''


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


import sys

from pyspark import SparkContext
from operator import add
import numpy as np
import mmh3


#round like java
def roundRating(val):
    if val % 1 == 0.5:
        val = int(val) + 1
    else:
        val = round(val)
    return val

#compute hash functions depending on k value (= number of hash function)
def hash(filters, rating, movie_id):
    #computing k
    k = int(-(np.log(float(pvalue)) / np.log(2)))
    movie_id = movie_id.encode('utf-8')
    size = list_m[rating - 1]
    #1s are set, in k positions based on number of hash functions
    for i in range(0, k):
        #seed is done to change the hash function
        position = mmh3.hash(movie_id, 50 * i) % size
        filters[rating][position] = 1

#insert m values in a list
def fillM(alist, pvalue):
    tlist = list(zip(*alist))
    m = []
    listCount = tuple([int(x) for x in tlist[1]])
    tlist2 = list(listCount)
    for index in range(len(tlist2)):
        m.append(int((-(tlist2[index]) * np.log(float(pvalue)) / (pow(np.log(2), 2))) + 1))
    listm = list(m)
    return listm

#logical OR between two filters
def orFilter(filtro1, filtro2):
    count = -1
    for i in filtro2:
        count = count + 1
        if i == 1:
            filtro1[count] = 1
    return filtro1

#Bloom Filter initialitation with all 0s and the correct size calculated by the function above
def initFilter():
    f1 = []
    for i in range(list_m.__getitem__(0)):
        f1.append(0)
    f2 = []
    for i in range(list_m.__getitem__(1)):
        f2.append(0)
    f3 = []
    for i in range(list_m.__getitem__(2)):
        f3.append(0)
    f4 = []
    for i in range(list_m.__getitem__(3)):
        f4.append(0)
    f5 = []
    for i in range(list_m.__getitem__(4)):
        f5.append(0)
    f6 = []
    for i in range(list_m.__getitem__(5)):
        f6.append(0)
    f7 = []
    for i in range(list_m.__getitem__(6)):
        f7.append(0)
    f8 = []
    for i in range(list_m.__getitem__(7)):
        f8.append(0)
    f9 = []
    for i in range(list_m.__getitem__(8)):
        f9.append(0)
    f10 = []
    for i in range(list_m.__getitem__(9)):
        f10.append(0)
    filters = {1: f1, 2: f2, 3: f3, 4: f4, 5: f5, 6: f6, 7: f7, 8: f8, 9: f9, 10: f10}
    return filters

#we read each line of the input file and put the movie title in the right bloom based on the rating
def mapper(keyValueRDD):
    filters = initFilter()
    for i in keyValueRDD:
        hash(filters, i[0], i[1])
    return filters.items()

#used in the tests, check if the film is present in the bloom
def isMember(title,array,m):
    pvalue=sys.argv[3]
    k= int(-(np.log(float(pvalue)) / np.log(2)))
    for i in range(k):
        position = mmh3.hash(title, 50 * i) % m
        if(int(array[position])!=1):
            return 0
    return 1



if __name__ == "__main__":

    # # parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: Cloud-Computing project <input file> <output file> <pvalue>", file=sys.stderr)
        sys.exit(-1)
    inputFile = sys.argv[1]
    pvalue = sys.argv[3]
    # connect to Hadoop Cluster
    master = "yarn"
    sc = SparkContext(master, "Cloud-Computing project")

    # create input file RDD and  divide it into 4 parts
    inputRDD = sc.textFile(inputFile, 4)

    #create rdd with key value (film_id-rating) with the approximate rating
    #this value is the input for the mapper
    keyValueRDD = inputRDD.map(lambda x: (roundRating(float(x.split("\t")[1])), x.split("\t")[0]),
                               preservesPartitioning=True)

    #for each movie in the rating assigns 1, and then counts how many movies there are for each rating. We will need this value to size the blooms
    #map: create map rating:1 --> reducebykey: aggregate all film with rating and do sum --> sortByKey : sort
    #we work with 4 partitions
    valuesM = keyValueRDD.distinct().keys().map(lambda x: (x, 1)).reduceByKey(add).sortByKey()
    #we calculate the sizing for the bloom filters, given the number of elements and the pvalue
    list_m = fillM(valuesM.collect(), float(pvalue))

    #mapPartition : applies the function mapper to each partition of the RDD
    #the mapper creates the bloom filters with the given rdd partitions, then the reducer applies the logical OR function and aggregates the results
    finalRDD = keyValueRDD.mapPartitions(mapper, preservesPartitioning=True).reduceByKey(lambda x, y: orFilter(x, y),
                                                                                         numPartitions=1).saveAsTextFile(
        sys.argv[2])
    sc.stop()

    #test begin





    #connect to Hadoop Cluster
    master = "yarn"
    sc = SparkContext(master, "Cloud-Computing project")
    #I take as input the output of mapreduce
    rdd = sc.textFile("/cloudproject/Spark/part-00000")
    llist = rdd.collect()
    arr = []

    # "creation of bloom" from work done by spark
    #now we have 10 bloom filter derived from spark output
    for line in llist:
        tot = line.split('[')
        tot = tot[1].split(']')
        tmp = tot[0].split(',')
        arr.append(tmp)


    FP = [0] * 10
    TN = [0] * 10
    N = [0] * 10
    totale = 0
     #read from input file data.tsv
    rdd2 = sc.textFile("/cloudproject/data.tsv")
    inputRdd = rdd2.collect()

    for line in inputRdd:
         #read each line of the file and divide the rating by the title of the movie with regex
         tot=line.split('\t')
         title = tot[0]
         rating=roundRating(float(tot[1]))
         #films that match the rating
         N[rating - 1] = N[rating - 1] + 1
         totale = totale + 1
         #for each film with a rating, check that it is not present in the other blooms, if there is an increase in the FP counter else the TN counter
         for i in range(10):
             if (rating != i+1):
                 if (isMember(title, arr[i], len(arr[i]) - 1) == 1):
                     FP[i] = FP[i] + 1
                 else:
                     TN[i] = TN[i] + 1
    print("P-value = "+pvalue + "\n")

    print("############ BEGIN TEST #############")
    for m in range(10):
         print("\nRating" + str(m)+ " FPR: " + str(FP[m] / ((totale - N[m]))) + " FP: " + str(FP[m])+ "\n")
    print("############ TEST ENDED #############")
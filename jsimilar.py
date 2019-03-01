from itertools import combinations
from pyspark.ml.feature import RegexTokenizer
from pyspark.ml.feature import NGram

filesList = []
shringleList = []
output = []

def similarity(s1,s2):
    #convert to rdd
    x1 = sc.parallelize(getList(shringleList[s1]))
    x2 = sc.parallelize(getList(shringleList[s2]))
    # union and intersection
    u = float(x1.union(x2).count())
    i = float(x1.intersection(x2).count())
    # get jaqards similarity
    output.append(filesList[s1]+"#"+filesList[s2]+"\t"+str(float(i/u)))

def getTwoFileCombo(l):
    pairs = combinations(range(len(l)),2)
    for p in pairs:
        similarity(p[0],p[1])

def read(fileName):
    # read file
    return spark.read.text(fileName)

def shringles(x,fileName):
    # tokenize and ngrams
    tokenizer = RegexTokenizer(inputCol="value",outputCol="words",pattern="\\W")
    ngrams = NGram(n=x,inputCol="words",outputCol="kshringles")
    shringleList.append(ngrams.transform(tokenizer.transform(read(fileName))))

def getList(df):
    # get list from ngrams
    tmp = []
    for a in df.select("kshringles").collect():
        for b in a:
            for c in b:
                tmp.append(c)
    return tmp

outputLoc = '/home/ubuntu/A2/output'
filesList.append("/home/ubuntu/A2/book1.txt")
filesList.append("/home/ubuntu/A2/book2.txt")
filesList.append("/home/ubuntu/A2/book3.txt")
kshringle = 2
for i in filesList:
    shringles(kshringle,i)

getTwoFileCombo(shringleList)
sc.parallelize(output).saveAsTextFile(outputLoc)

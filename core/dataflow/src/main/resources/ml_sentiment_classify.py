import sys
import pickle
import csv
from nltk.tokenize import TweetTokenizer

modelFullPathFileName = sys.argv[1]
dataFullPathFileName = sys.argv[2]
resultFullPathFileName = sys.argv[3]
inputDataMap = {}
recordLabelMap = {}

# This program should be called by edu.uci.ics.texera.dataflow.nlp.sentiment.MLSentimentOperator
# call format:
# python3 ml_sentiment_classify dataFullPathFileName resultFullPathFileName

def debugLine(strLine):
	f = open("python_classifier_loader.log","a+")
	f.write(strLine)
	f.close()

def main():
	readData()
	classifyData()
	writeResults()

# Write recordLabelMap (the classified result) to a csv file
def writeResults():
	with open(resultFullPathFileName, 'w', newline='') as csvfile:
		resultWriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
		resultWriter.writerow(["TupleID", "ClassLabel"])
		for id, classLabel in recordLabelMap.items():
			resultWriter.writerow([id, classLabel])

# This is the function to load model and classify data
def classifyData():
	# load model from modelFullPathFileName, using pickle
	CLASSIFIER = pickle.load(open(modelFullPathFileName, 'rb'))

	tknzr = TweetTokenizer()

	for key, value in inputDataMap.items():
		# call classify and write the result to recordLabelMap
		recordLabelMap[key] = 1 if CLASSIFIER.classify({x: True for x in tknzr.tokenize(value)}) == "POS" else -1

def readData():
	with open(dataFullPathFileName, newline='') as csvfile:
		dataReader = csv.reader(csvfile, delimiter=',', quotechar='"')
		for record in dataReader:
			inputDataMap[record[0]] = record[1]
			
if __name__ == "__main__":
	main()
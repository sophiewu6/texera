import sys,pickle,csv
from nltk.tokenize import TweetTokenizer
# from collections import Counter

pickleFullPathFileName = sys.argv[1]
dataFullPathFileName = sys.argv[2]
resultFullPathFileName = sys.argv[3]
inputDataList = []
recordLabelMap = {}


# call format:
# python3 nltk_NaiveBayes_classify pickleFullPathFileName dataFullPathFileName resultFullPathFileName
def debugLine(strLine):
    f = open("python_classifier_loader.log", "a+")
    f.write(strLine)
    f.close()


def main():
    readData()
    classifyData()
    writeResults()


def writeResults():
    with open(resultFullPathFileName, 'w', newline='') as csvfile:
        resultWriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        resultWriter.writerow(["TupleID", "ClassLabel"])
        for id, classLabel in recordLabelMap.items():
            resultWriter.writerow([id, classLabel])


def classifyData():
    tknzr = TweetTokenizer()
    # prediction_cnt = Counter()
    pickleFile = open(pickleFullPathFileName, 'rb')
    naiveBayesClassifierModel = pickle.load(pickleFile)  # for text in sys.argv[2:]:
    for i in range(len(inputDataList)):
        inputData=inputDataList[i]
        recordLabelMap[inputData[0]]='POS' if naiveBayesClassifierModel.classify({x: True for x in tknzr.tokenize(inputData[1])}) == 'POS' else 'NEG'
        # prediction_cnt[(inputData[2],recordLabelMap[inputData[0]])]+=1
    pickleFile.close()
    # precision = 100.0 * prediction_cnt[('POS', 'POS')] / (
    #             prediction_cnt[('POS', 'POS')] + prediction_cnt[('NEG', 'POS')])
    # recall = 100.0 * prediction_cnt[('POS', 'POS')] / (prediction_cnt[('POS', 'POS')] + prediction_cnt[('POS', 'NEG')])
    # f1 = 2.0 * precision * recall / (precision + recall)
    # print('Validation Precision = {}'.format(precision), file=sys.stderr)
    # print('Validation Recall    = {}'.format(recall), file=sys.stderr)
    # print('Validation F1        = {}'.format(f1), file=sys.stderr)

def readData():
    with open(dataFullPathFileName, newline='') as csvfile:
        dataReader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for record in dataReader:
            inputDataList.append([record[0], record[1]])
            # inputDataList.append([record[0],record[1],record[2]])
            # inputDataMap[record[0]] = record[1]


if __name__ == "__main__":
    main()

import os

filePath = '/Users/mac/ScanCSVToMysql/'

fileList = os.listdir(filePath)

for fileName in fileList:
    f = open(filePath + fileName, encoding="gbk")
    s = f.read().replace('null', '0')
    f.close()
    f = open(filePath + fileName, "w",encoding="gbk")
    f.write(s)
    f.close()
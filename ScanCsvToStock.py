# 导入需要使用到的模块
import urllib
import re
import pandas as pd
import pymysql
import os
import csv

# # 爬虫抓取网页函数
# def getHtml(url):
#    html = urllib.request.urlopen(url).read()
#    html = html.decode('gbk')
#    return html
#
#
# # 抓取网页股票代码函数
# def getStackCode(html):
#    s = r'<li><a target="_blank" href="http://quote.eastmoney.com/\S\S(.*?).html">'
#    pat = re.compile(s)
#    code = pat.findall(html)
#    return code
#
#
# # #########################开始干活############################
Url = 'http://quote.eastmoney.com/stocklist.html'  # 东方财富网股票数据连接地址
filepath = '/Users/mac/ScanCSVToMysql/'  # 定义数据文件保存路径
# # 实施抓取
# code = getStackCode(getHtml(Url))
# # 获取所有股票代码（以6开头的，应该是沪市数据）集合
# CodeList = []
# for item in code:
#    if item[0] == '6':
#        CodeList.append(item)
# # 抓取数据并保存到本地csv文件
# for code in CodeList:
#    print('正在获取股票%s数据' % code)
#    url = 'http://quotes.money.163.com/service/chddata.html?code=0' + code + \
#         '&end=20161231&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP'
#    urllib.request.urlretrieve(url, filepath + code + '.csv')

##########################将股票数据存入数据库###########################

# 数据库名称和密码
name = 'root'
password = 'root'  # 替换为自己的账户名和密码
# 建立本地数据库连接(需要先开启数据库服务)
db = pymysql.connect('localhost', name, password, db='stockDataBase',charset='utf8')
cursor = db.cursor()
#创建数据库stockDataBase
# sqlSentence1 = "create database stockDataBase"
# cursor.execute(sqlSentence1)  # 选择使用当前数据库
#sqlSentence2 = "use stockDataBase;"
#cursor.execute(sqlSentence2)

# 获取本地文件列表
fileList = os.listdir(filepath)
# print(fileList)
# 依次对每个数据文件进行存储
for fileName in fileList:
    listtuple = list()
    f = open(filepath + fileName, encoding="gbk")
    f.readline()
    data = csv.reader(f)
    # 创建数据表，如果数据表已经存在，会跳过继续执行下面的步骤print('创建数据表stock_%s'% fileName[0:6])

    try:
        sqlSentence3 = "create table stock_%s" % fileName[0:6] + "(日期 date, 股票代码 VARCHAR(10),     名称 VARCHAR(10) CHARACTER SET utf8mb4,\
                       收盘价 float,    最高价    float, 最低价 float, 开盘价 float, 前收盘 float, 涨跌额    float, \
                       涨跌幅 float, 换手率 float, 成交量 bigint, 成交金额 bigint, 总市值 bigint, 流通市值 bigint)"
        cursor.execute("DROP TABLE IF EXISTS stock_%s;" % fileName[:6])
        cursor.execute(sqlSentence3)#迭代读取表中每行数据，依次存储
    except Exception as e:
        print(e)

    print('正在存储stock_%s' % fileName[0:6])

    for i in data:
        i[1] = i[1][1:]
        # 插入数据语句
        listtuple.append(tuple(i))
    sqlSentence4 = "insert into stock_%s" % fileName[
                                            0:6] + '(日期, 股票代码, 名称, 收盘价, 最高价, 最低价, 开盘价,前收盘, 涨跌额, 涨跌幅, 换手率, 成交量, 成交金额, 总市值, 流通市值)values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'

    try:
        cursor.executemany(sqlSentence4, listtuple)
    except Exception as e:
        # 如果以上插入过程出错，跳过这条数据记录，继续往下进行
        print(e)
        continue



# 关闭游标，提交，关闭数据库连接
cursor.close()
db.commit()
db.close()

###########################查询刚才操作的成果##################################

#重新建立数据库连接
db = pymysql.connect('localhost', name, password, 'stockDataBase')
cursor = db.cursor()
#查询数据库并打印内容
sqlSentence2 = "use stockDataBase;"
cursor.execute(sqlSentence2)
cursor.execute('select * from stock_600000')
results = cursor.fetchall()
for row in results:
    print(row)
#关闭
cursor.close()
db.commit()
db.close()
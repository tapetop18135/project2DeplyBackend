import numpy as np
import numpy.ma as ma
from .mongoDB import MongoDB_lc
import time
import warnings

class Average_service():

    def __init__(self, aryYear, collection, month_init, month_end):
        self.aryYear = aryYear
        self.col = collection
        self.month_init = month_init
        self.month_end = month_end
        self.__mongoConnect()

    def __mongoConnect(self):
        self.obj = MongoDB_lc()
        self.obj.collection(self.col)
        self.resultFormMongo = self.obj.mongo_find(self.aryYear)
        # self.resultMask = self.__getMask(obj)

    def getMask(self, maskAry):
        for result in self.obj.mongo_findMask(maskAry):
            result = result['mask']
        return result
        
    def getAverageMap(self, month):
        start = time.time()
        print("getAverageMap")
        tempData = self.resultFormMongo
        tempAry = []
        for i in tempData:
            tempAry.append(i['data'][month])
        
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            tempAry = np.array(tempAry, dtype=np.float)
            tempAry = np.nanmean(tempAry, axis= 0)
        print(time.time() - start)
        # if(keyMask != None):
        #     resultMask = self.getMask(keyMask)
        #     return ma.array(tempAry, mask = resultMask)
        # else:
        return tempAry

    def getAverageGraph(self,month_state = None):
        start = time.time()
        print("getAverageGraph")
        tempData = self.resultFormMongo
        tempAry = []
        date = []
        if(month_state == None):
            yearinit = self.aryYear[0]
            yearend = self.aryYear[len(self.aryYear)-1]
            state = 0
            for y in tempData:
                dataYear = y['data']
                dataYear = np.array(dataYear, dtype=np.float)
                for m in range(1,len(dataYear)):
                    if(y['year'] == yearinit and self.month_init == m):
                        state = 1
                    elif(y['year'] == yearend and self.month_end == m):
                        date.append(f"{y['year']}-{m}")
                        tempAry.append(np.nanmean(dataYear[m]))
                        state = 0
                    if(state == 1):
                        date.append(f"{y['year']}-{m}")
                        tempAry.append(np.nanmean(dataYear[m]))
        else:
            tempAry = []
            for y in tempData:
                date.append(y['year'])
                dataTemp = np.array(y['data'][month_state], dtype=np.float)
                tempAry.append(np.nanmean(dataTemp))
            tempAry = np.array(tempAry, dtype=np.float)
            print(tempAry.shape)

        
        tempAry = np.array(tempAry)
        print(time.time() - start)
        return tempAry, date
    
    def getSeasonal(self):
        start = time.time()
        print("getSeasonal")
        tempData = self.resultFormMongo
        tempAry = []
        # for i in tempData:
        #     tempAry.append(i['data'])

        # with warnings.catch_warnings():
        #     warnings.simplefilter("ignore", category=RuntimeWarning)
        #     tempAry = np.array(tempAry, dtype=np.float)
        #     tempAry = np.nanmean(tempAry, axis= 0)
        
        # meanSS = []
        # for tm in tempAry:
        #     meanSS.append(np.nanmean(tm))
        # meanSS = np.array(meanSS)
        # print(time.time() - start)
        print("SSSSSSSSSSSSSSSSSSSSSSSSSS Long Code SSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        print("--------------NO Custom")
        for y in tempData:
            dataYear = y['data']
            dataYear = np.array(dataYear, dtype=np.float)
            month = []
            for m in range(1,len(dataYear)):
                temp = dataYear[m]
                value = np.nanmean(temp)
                month.append(value)
            tempAry.append(month)
            
        tempAry = np.array(tempAry, dtype=np.float)
        tempAry = np.nanmean(tempAry, axis=0)
        print(tempAry.shape)
        
        return tempAry


# def year_ary(init,end):
#     init = init.split("-")
#     yearinit = int(init[0])
#     end = end.split("-")
#     yearend = int(end[0])
#     countYear = yearend-yearinit+1
#     year = []
#     for i in range(0,countYear):
#         year.append(yearinit+i)

#     return year, [int(init[1]),int(end[1])]

# yearinit = "1951-1"
# yearend = "2017-12"

# ary, month_IE = year_ary(yearinit, yearend)
# collection = 'ghcndex_TXx'

# import matplotlib.pyplot as plt
# # SERVICE GET Average Graph
# a = Average_service(ary, collection, month_IE[0], month_IE[1])
# dataG = a.getAverageGraph()
# plt.plot(dataG)
# plt.ylabel('temperature C degree')
# plt.show()
# # SERVICE GET Seasonal Graph
# b = Average_service(ary, collection, month_IE[0], month_IE[1])
# dataS = b.getSeasonal()[1:]
# plt.plot(dataS)
# plt.ylabel('temperature C degree')
# plt.show()
# SERVICE GET Average Map
# c = Average_service(ary, collection, month_IE[0], month_IE[1])
# dataM = c.getAverageMap(0)
# print(dataM.shape)
# print(np.nanmean(dataM))

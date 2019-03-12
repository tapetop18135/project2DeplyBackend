import numpy as np
import numpy.ma as ma
from .mongoDB import MongoDB_lc
import time
import warnings
# import matplotlib.pyplot as plt
class SelectCus_service():

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

    def getAverageGraphCus(self, custom , month_state = None):
        start = time.time()
        print("getAverageGraphCus")
        tempData = self.resultFormMongo
        indexMulti = np.array(custom).T
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
                        temp = np.array(dataYear[m], dtype=np.float)
                        temp = temp[[indexMulti[0],indexMulti[1]]]
                        tempAry.append(np.nanmean(temp))
                        state = 0
                    if(state == 1):
                        date.append(f"{y['year']}-{m}")
                        temp = np.array(dataYear[m], dtype=np.float)
                        temp = temp[[indexMulti[0],indexMulti[1]]]
                        tempAry.append(np.nanmean(temp))

        else:
            tempAry = []
            for y in tempData:
                date.append(y['year'])

                temp = np.array(y['data'][month_state], dtype=np.float)
                temp = temp[[indexMulti[0],indexMulti[1]]]
                tempAry.append(np.nanmean(temp))

        tempAry = np.array(tempAry)
        print(time.time() - start)
        return tempAry, date

    def getSeasonalCus(self, custom):
        start = time.time()
        print("getSeasonal")
        indexMulti = np.array(custom).T
        tempData = self.resultFormMongo
        tempAry = []
        for y in tempData:
            dataYear = y['data']
            dataYear = np.array(dataYear, dtype=np.float)
            month = []
            for m in range(1,len(dataYear)):
                temp = dataYear[m][indexMulti[0],indexMulti[1]]
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


# thai = [[44, 40], [42, 42], [43, 41], [43, 40], [41, 41], [42, 41], [42, 40], [41, 40], [39, 40]]
# obj = SelectCus_service(ary, collection, month_IE[0], month_IE[1])
# dataA, year  = obj.getAverageGraphCus(thai)
# print(dataA)
# plt.plot(dataA)
# plt.show()
# dataS  = obj.getSeasonalCus(thai)
# plt.plot(dataS)
# plt.show()
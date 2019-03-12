import numpy as np
import numpy.ma as ma
from .mongoDB import MongoDB_lc
import time
import warnings

# from netCDF4 import Dataset
# import matplotlib.pyplot as plt
# from mpl_toolkits.basemap import Basemap

class Trend_service():

    def __init__(self, aryYear, collection, month_init, month_end):
        self.aryYear = aryYear
        self.col = collection
        self.month_init = month_init
        self.month_end = month_end
        self.__mongoConnect()

    def __mongoConnect(self):
        self.obj = MongoDB_lc()
        self.obj.collection(self.col)

    def __trendTime(self, diabetes_y_train):
        # import matplotlib.pyplot as plt
        import numpy as np
        from sklearn import datasets, linear_model
        from sklearn.metrics import mean_squared_error, r2_score

        diabetes_X_train = []
        for i in range(0,len(diabetes_y_train)):
            diabetes_X_train.append([i])
        diabetes_X_train = np.array(diabetes_X_train)

        regr = linear_model.LinearRegression()
        regr.fit(diabetes_X_train, diabetes_y_train)

        x = np.arange(len(diabetes_y_train))
        y = np.array(diabetes_y_train.values.tolist()).T[0]
        r_value = np.corrcoef(x, y)[0][1]

        hypo = self.__Check_hypo(r_value, len(diabetes_y_train))

        return regr.coef_, hypo

    def __Check_hypo(self, r, n, alpha=0.05):
        from scipy import stats
        t_score = r/np.sqrt((1-r**2)/(n-2))
        t_cri = stats.t.ppf(1-alpha/2, n)
        if(t_score >= t_cri*-1 and t_score <= t_cri):
            return True # fail to reject H0 ไม่มีนัยยะสัมคัญ
        return False # reject H0 มีนัยยะสัมคัญ

    def getData(self, month):
        self.resultFormMongo = self.obj.mongo_find(self.aryYear)
        tempData = self.resultFormMongo
        tempAry = []
        for i in tempData:
            tempAry.append(i['data'][month])
        tempAry = np.array(tempAry, dtype=np.float32)
        return tempAry

    def trendAndHypo(self, dataTemp):
        import pandas as pd
        nt, nlat, nlon = dataTemp.shape

        aryLat = []
        hypoLat = []
        sumhypo = 0
        sumhypoN = 0
        for i in range(0, nlat):
            aryLon = []
            hypoLon = []
            for j in range(0, nlon):
                diabetes_y_train = dataTemp[:,i,j]
                diabetes_y_train = pd.DataFrame(diabetes_y_train).dropna()
                if(not diabetes_y_train.values.tolist()):
                    aryLon.append(-99.9) ###############
                    hypoLon.append(True)
                else:
                    slope, hypo  = self.__trendTime(diabetes_y_train)
                    aryLon.append(slope)
                    if(hypo == False):
                        sumhypo += 1
                    else:
                        sumhypoN += 1
                    hypoLon.append(hypo)
            hypoLat.append(hypoLon)
            aryLat.append(aryLon)
        
        print(sumhypo)
        print(sumhypoN)
        hypoLat = np.array(hypoLat, dtype=np.float)
        aryLat = np.array(aryLat, dtype=np.float)
        aryLat[aryLat==-99.9] = 0 ###############
        print("fin trend and hypo")
        return aryLat, hypoLat

    def trendAndHypoFromDB(self):
        yInit = self.aryYear[0]
        yEnd = self.aryYear[len(self.aryYear)-1]
        print(f"trendAndHypoFromDB : {yInit}-{yEnd}")
        tempData = self.obj.mongo_findTrend(f"{yInit}-{yEnd}")
        dataTrend = 0 
        dataHypo = 0
        for i in tempData:
            dataTrend = i['dataTrend']
            dataHypo = i['dataHypo']

        dataTrend = np.array(dataTrend, dtype=np.float)
        dataHypo = np.array(dataHypo, dtype=np.float)
        print("fin form db")
        return dataTrend, dataHypo

    def insertTomongo(self, tempR, hypoLat, col):
        yInit = self.aryYear[0]
        yEnd = self.aryYear[len(self.aryYear)-1]
        print(f"insertTomongo: {yInit}-{yEnd}")
        a = MongoDB_lc()
        colec = f"{col}_trend"
        a.collection(colec)
        post = {
                "duration" : f"{yInit}-{yEnd}",
                "dataTrend" : tempR,
                "dataHypo" : hypoLat
            }
        a.mongo_insert(post)


# def map(data,pointAry,fname):
#     temp = data
#     parallels = np.arange(-90,90,30.)
#     meridians = np.arange(0,357.5,30)
#     lat = np.linspace(-90,90,73)
#     lon = np.linspace(0,357.5,144)
#     temp = np.reshape(temp,(73,144))
    
#     plt.title(fname)
#     m = Basemap(projection='cyl', llcrnrlon=min(lon), llcrnrlat=min(lat), urcrnrlon=max(lon), urcrnrlat=max(lat))    
#     x, y = m(*np.meshgrid(lon, lat))
#     clevs = np.linspace(np.min(temp.squeeze()), np.max(temp.squeeze()), 21)
#     cs = m.contourf(x, y, temp.squeeze(), clevs, cmap=plt.cm.RdBu_r)
#     m.drawcoastlines()  
#     m.drawparallels(parallels, labels=[1,0,0,0])
#     m.drawmeridians(meridians, labels=[1,0,0,1])
#     m.colorbar()

#     lons = pointAry[1]
#     lats = pointAry[0]
#     x,y = m(lons, lats)
#     m.plot(x, y, 'ko', markersize=2)
    
#     # plt.savefig(fname)
#     plt.show()

######################
# location = "../dataset/ghcndex_current/GHCND_TXx_1951-2018_RegularGrid_global_2.5x2.5deg_LSmask.nc"
# nc = Dataset(location, 'r')
# lon = nc.variables["lon"][:]
# lat = nc.variables["lat"][:]
# latPoint = []
# lonPoint = []
# print(lon)
######################

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
# yearend = "1951-12"

# ary, month_IE = year_ary(yearinit, yearend)
# collection = 'ghcndex_TXx_trend'

# c = Trend_service(ary, collection, month_IE[0], month_IE[1])
# dataRaw = c.getData(0)
# import time
# start = time.time()
# tempR, hypoLat = c.trendAndHypo(dataRaw)
# print(time.time() - start)
# print(tempR.shape)
# print(hypoLat.shape)

# tempR, hypoLat = trendGrid(dataM['Ann'])
# for i in range(0,len(hypoLat)):
#     for j in range(0,len(hypoLat[0])):
#         if(hypoLat[i][j] == False):
#             lonPoint.append(lon[j])
#             latPoint.append(lat[i])
# print(latPoint)
# print(lonPoint)

# # map(temp['Ann'][0],[latPoint,lonPoint],'TEEER')
# map(tempR, [latPoint,lonPoint],'TEEER')

# yearinit = "2000-1"
# yearend = "2017-12"

# ary, month_IE = year_ary(yearinit, yearend)
# collection = 'ghcndex_TXx_trend'

# c = Trend_service(ary, collection, month_IE[0], month_IE[1])
# tempR, hypoLat = c.trendAndHypoFromDB()
# print(tempR.shape)
# print(hypoLat.shape)
# if(tempR.shape):
#     print("Have a data")
# else:
#     print("No have a data : Gen trend and save")
#     col = f"ghcndex_TXx"
#     obj = Trend_service(ary, col, month_IE[0], month_IE[1])
#     dataRaw = obj.getData(0)
#     start = time.time()
#     tempR, hypoLat = obj.trendAndHypo(dataRaw)
#     print(tempR.shape)
#     print(hypoLat.shape)
#     tempR[tempR == 0] = np.nan
#     obj.insertTomongo(tempR.tolist(),hypoLat.tolist(), col)

#------------------------ obj.insertTomongo(tempR.tolist(),hypoLat.tolist(), collectionDB)
import numpy as np
import numpy.ma as ma
from .mongoDB import MongoDB_lc
import time
from scipy import signal
import warnings

# from netCDF4 import Dataset
# import matplotlib.pyplot as plt
# from mpl_toolkits.basemap import Basemap
# import xarray as xr

class Pca_service():

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

    def __gendata(self, month = None):
        tempData = self.resultFormMongo
        tempAry = []
        self.date = []
        if(month == None):
            for y in tempData:
                dataYear = y['data']
                dataYear = np.array(dataYear, dtype=np.float)
                for m in range(1,len(dataYear)):
                    self.date.append(f"{y['year']}-{m}")
                    tempAry.append(dataYear[m])
        else:
            for y in tempData:
                self.date.append(y['year'])
                tempAry.append(y['data'][month])

        tempAry = np.array(tempAry, dtype=np.float32)
        return tempAry
            
    def __pca_fn(self, datatemp, n_com):
        print("==================== START PCA ===============================")
        from sklearn.decomposition import PCA
        test_diff = datatemp.reshape((self.nt,self.nlat*self.nlon))
        print(test_diff.shape)
        pca = PCA(n_components=n_com)
        pca.fit(test_diff)
        principalComponents = pca.transform(test_diff)
        EOFs = pca.components_
        variance_ratio = pca.explained_variance_ratio_
        return [principalComponents.T, EOFs, variance_ratio*100]

    # def getPCA_service(self, com):
    #     data = self.__prepareData()
    #     pca_pc, pca_eofs, pca_va_ratio = self.__pca_fn(data, com)
    #     eof_final = []
    #     for i in range(0, com):
    #         temp = pca_eofs[i,:]
    #         temp = np.reshape(temp, (self.nlat, self.nlon))
    #         temp[temp == 0] = -99.99
    #         eof_final.append(temp.tolist())

    #     return pca_pc, eof_final, pca_va_ratio

    def getPCA_service(self, com):
        data = self.__gendata()
        self.nt, self.nlat, self.nlon = data.shape 
        data[np.isnan(data)] = 0
        print(f"data : {data.shape}")
        pca_pc, pca_eofs, pca_va_ratio = self.__pca_fn(data, com)
        eof_final = []
        for i in range(0, com):
            temp = pca_eofs[i,:]
            temp = np.reshape(temp, (self.nlat, self.nlon))
            temp = np.array(temp, dtype=np.float64)
            temp[temp == 0] = -99.9
            eof_final.append(temp.tolist())
        return pca_pc, eof_final, pca_va_ratio
    
    # def __prepareData(self):
    #     tempdata = self.__gendata()

    #     self.nt, self.nlat, self.nlon = tempdata.shape 
    #     print(self.nt, self.nlat, self.nlon)
    #     data_detrend=np.empty((self.nt,self.nlat,self.nlon))
    #     data_detrend[:,:,:] = np.nan
    #     x = np.arange(self.nt).reshape(self.nt,1)
    #     for i in range(0,self.nlat):
    #         for j in range(0,self.nlon):
    #             y = tempdata[:,i,j]
    #             if not np.isnan(y).all(): # ถ้าไม่ nan ทั้งหมดให้เข้า if เช่น [[np.nan, np.nan],[np.nan, 1]]
    #                 b = ~np.isnan(y)
    #                 data_detrend[b,i,j] = signal.detrend(y[b])

    #     print(data_detrend.shape)
    #     data_season = data_detrend.reshape(self.nt//12,12,self.nlat,self.nlon)
    #     print(data_season.shape)
    #     data_mean = np.nanmean(data_season, axis=(0))
    #     print(data_mean.shape)
    #     data_diff = data_season - data_mean
    #     print(data_diff.shape)
    #     data_diff[np.isnan(data_diff)] = 0
    #     return data_diff

    def getVarianceMap(self, month=None):
        start = time.time()
        print("getVariance")
        tempData = self.resultFormMongo
        tempAry = []

        if(month == None):
            for y in tempData:
                dataYear = y['data']
                dataYear = np.array(dataYear, dtype=np.float)
                for m in range(1,len(dataYear)):
                    tempAry.append(dataYear[m])

        else:
            for i in tempData:
                tempAry.append(i['data'][month])

        tempAry = np.array(tempAry, dtype=np.float)
        print(f"shape : {tempAry.shape}")
        tempAry = np.nanvar(tempAry, axis=0)
        tempAry = np.array(tempAry, dtype=np.float64)
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

# def map(data,fname):
#     temp = data
#     parallels = np.arange(-90,90,30.)
#     meridians = np.arange(0,356.25,30) # 356.25 96 357.5 144
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
    
#     # plt.savefig(fname)
#     plt.show()

# yearinit = "1951-1"
# yearend = "2017-12"

# ary, month_IE = year_ary(yearinit, yearend)
# collection = 'ghcndex_TXx'

# obj = Pca_service(ary, collection, month_IE[0], month_IE[1])
# pca_pc, pca_eofs, pca_va_ratio =  obj.getPCA_service(6)

# for i in range(0,1):
#     temp = np.reshape(pca_eofs[i],(obj.nlat,obj.nlon))
#     map(temp, f"eofs {i} {yearinit} to {yearend} {collection}")

# obj = Pca_service(ary, collection, month_IE[0], month_IE[1])
# data = obj.getVarianceMap()
# data[np.isnan(data)] = 0
# map(data, f"variance {yearinit} to {yearend} {collection}")
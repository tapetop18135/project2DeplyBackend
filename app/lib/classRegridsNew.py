import numpy as np
import xarray as xr
import time

class Regrids():

    def __init__(self):
        np.seterr(divide='ignore', invalid='ignore')
    
    # def getRawdata(self, locafile):
    #     return xr.open_dataset(locafile)

    def regrids(self, dts,n=2):
        lat, lon = dts.shape
        if(lat % n != 0):
            dts = np.delete(dts, lat-1, 0)
        if(lon % n != 0):
            dts = np.delete(dts, lon-1, 1)

        ary2d = np.array(dts)
        temp = ary2d.reshape([lat//n,n,lon//n,n])
        result = np.nanmean(temp, axis=(1,3))
        print(f"map : {ary2d.shape} => {result.shape}")
        return np.nan_to_num(result)

    # def reGrid_1x1np(self, dts):
    #     print("regrid 1x1 numppy")
    #     lat = len(dts) # 360
    #     lon = len(dts[0]) # 720
    #     ary2d = np.array(dts)
    #     temp = ary2d.reshape([lat//2,2,lon//2,2])
    #     m3 = np.nanmean(temp, axis=(1,3))
    #     print(m3.shape)
    #     return np.nan_to_num(m3).tolist()

    # def reGride_1x1(self, dts):
    #     print("regrid 1x1")
    #     print(f"lat : {int(len(dts)/2)}")
    #     print(f"lon : {int(len(dts[0])/2)}")
    #     dts = np.nan_to_num(dts)
    #     lenEnd_lat = int(len(dts))
    #     lenEnd_lon = int(len(dts[0]))
    #     array_lat = []
    #     for i in range(0,lenEnd_lat,2):
    #         ary_lon = []
    #         for j in range(0,lenEnd_lon,2):
    #             ary_lon.append((dts[i][j]+dts[i][j+1]+dts[i+1][j]+dts[i+1][j+1])/4)

    #             # count = np.count_nonzero([dts[i][j],dts[i][j+1],dts[i+1][j],dts[i+1][j+1]])
    #             # ary_lon.append((dts[i][j]+dts[i][j+1]+dts[i+1][j]+dts[i+1][j+1])/count)
            
    #         array_lat.append(ary_lon)

    #     return np.nan_to_num(np.array(array_lat)).tolist()

    def getLatLon_regrid(self, lats, lons, n=2):
        if(len(lats)%n != 0):
            lats = lats[:-1]
        if(len(lons)%n != 0):
            lons = lons[:-1]

        Rlats = np.nanmean(np.reshape(lats, (n,len(lats)//n), order='F') ,0)
        print(f"lats : {lats.shape[0]} => {Rlats.shape[0]}")
        Rlons = np.nanmean(np.reshape(lons, (n,len(lons)//n), order='F') ,0)
        print(f"lons : {lons.shape[0]} => {Rlons.shape[0]}")

        return {'lat':Rlats,'lon':Rlons}



# # locafile = 'D:/Project/webService/netCDF/rawdata/tmax.1979.nc'
# reg = Regrids() # Creat obj
# dts = reg.getRawdata(locafile)

# start = time. time() # detect time
# tmax = reg.reGrid_1x1np(dts['tmax'][0]) # INPUT 2D lat and lon
# lat, lon = reg.getLatLon_1x1(dts)
# print(f"time = {time.time() - start}") # detect time

# ##################### result #####################
# print(np.array(tmax).shape)
# print(len(lat))
# print(len(lon))
#################################################


################## TEST REGRID #######################
# reg = Regrids() # Creat obj
# dts = np.array([
#     [np.nan,1,1,1, 1,1,1,1],
#     [2,2,2,2, 2,2,2,2],
#     [3,3,3,3, 3,3,3,3],
#     [4,4,4,4, 4,4,4,np.nan]
# ])
# test2 = reg.regrids(dts)
# print(test2)

# from netCDF4 import Dataset 
# locafile = 'ecearth_rcp45_TXx'
# ncin = Dataset(f'netCDF4/{locafile}.nc', 'r')
# dataz = ncin.variables['Ann'][:]
# print(f"SHAPE : {dataz.shape}")
# test = dataz[0][:] 

# seting = {
#     'lat': ncin.variables['latitude'][:],
#     'lon': ncin.variables['longitude'][:]
# }

# from mapGenerate import My_mapGenerate

# pic = My_mapGenerate(test,seting,'Original') 
# pic.getMap()

# reg = Regrids() # Creat obj
# fin = reg.regrids(test) # <----------------------- Use

# lats = ncin.variables['latitude'][:]
# lons = ncin.variables['longitude'][:]
# # lats = np.array([1,2,3,4,5,6,7,8,9,10])
# ulatlon = reg.getLatLon_regrid(lats, lons)  # <----------------------- Use

# seting = {
#     'lat': ulatlon['lat'],
#     'lon': ulatlon['lon']
# }

# from mapGenerate import My_mapGenerate

# pic = My_mapGenerate(fin,seting,'regrid x4') 
# pic.getMap()
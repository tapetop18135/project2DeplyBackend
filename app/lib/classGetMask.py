import numpy as np
import pandas as pd 
from netCDF4 import Dataset

class MaskFromText:
    def __init__(self):
        pass

    def readCSV(self, name, loc="./app/staticFile/"):
        data = pd.read_csv(f"{loc}{name}.csv", header=None) 
        return data.values

    def dumtoCSV(self, arrayU, name, loc="../staticFile/"):
        arr = np.array(arrayU)
        np.savetxt(f"{loc}{name}.csv", arr, delimiter=",")

    def readNc(self, name, loc="../dataset/netcdf4Sea/"):
        ncin = Dataset(f"{loc}{name}.nc", 'r')
        return ncin


# obj = MaskFromText()
# # obj.dumtoCSV([[1,2,3],[4,5,5]],"mask_sea_AJ")
# data = obj.readCSV("mask_sea_AJ")
# print(data)
# data[data == 0] = 2
# data[data == 1] = 0
# data[data == 2] = 1
# print(data)
# name = "ecearth_rcp45_CSDI"
# data = obj.readNc(name)
# tempdata = data.variables["Ann"][0].mask
# tempdata = tempdata.tolist()
# tempdata = np.array(tempdata)
# print(tempdata)
# print("*//////////////////////////////////////////*")
# tempdata[tempdata == True] = 2
# data[data == True] = 0
# data[data == 2] = 1
# print(tempdata)
# print(data.variables["Ann"][0].mask)
# obj.dumtoCSV(data.variables["Ann"][0].mask,"mask_sea_AJ")

    
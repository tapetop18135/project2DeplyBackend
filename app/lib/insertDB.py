import numpy as np
from netCDF4 import Dataset
from mongoDB import MongoDB_lc
import pandas as pd
import os

def get_data(filez):
    location = filez
    ncin = Dataset(location, 'r')
    name = ['Ann','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    # name = ['Annual','January','February','March','April','May','June','July','August','September','October','November','December']
    dicm = {}

    for i in range(0,len(name)):
        try:
            dicm[name[i]] = ncin.variables[name[i]][:]
        except:
            break
    lat = ncin.variables['lat'][:]
    lon = ncin.variables['lon'][:]
    return dicm, [lat,lon]

def insertTomongo(data,col,detail,aryLatLon,yearInit):
    name = ['Ann','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    nt, nlat, nlon = data['Ann'].shape
    a = MongoDB_lc()
    a.collection(col)
    # yearInit = yearInit
    for year in range(0, nt):
        dataAry = []
        for month in name:
            # pdAry = pd.DataFrame(data[month][year]).fillna(-99)
            # dataAry.append(pdAry.values.tolist())
            try:
                dataAry.append(data[month][year].tolist())
            except:
                break

        npAry = np.array(dataAry)
        post = {
            "year" : yearInit+year,
            "data" : dataAry
        }
        a.mongo_insert(post)     
        print(str(yearInit+year))
    
    maskAry = data['Ann'][0].mask
    maskAry = maskAry.tolist()
    post = {
            "detail" : detail,
            "lat" : aryLatLon[0].tolist(),
            "lon" : aryLatLon[1].tolist(),
            "key__" : f"{detail['dataset']}_{detail['index_name']}",
            "mask" : maskAry
        }
    a.mongo_insert(post)  
    

    

########################### GHCNDEX ###########################

compare = {
    "TXn":["Min Tmax","temperature","Intensity","°C","Coldest daily maximum temperature"],
    "TNn":["Min Tmin","temperature","Intensity","°C","Coldest daily minimum temperature"],
    "TXx":["Max Tmax","temperature","Intensity","°C","Warmest daily maximum temperature"],
    "TNx":["Max Tmin","temperature","Intensity","°C","Warmest daily minimum temperature"],
    "DTR":["Diurnal temperature range","temperature","Intensity","°C","Mean difference between daily maximum and daily minimum temperature"],
    
    "GSL":["Growing season length","Duration","temperature","days","Annual number of days between the first occurrence of 6 consecutive days with Tmean > 5°C and first occurrence of consecutive 6 days with Tmean < 5°C. For the Northern Hemisphere, this is calculated from 1 Jan to 31 Dec while for the Southern Hemisphere it is calculated from 1 Jul to 30 Jun."],
    "CSDI":["Cold spell duration indicator","temperature","Duration","days","Annual number of days with at least 6 consecutive days when Tmin < 10th percentile"],
    "WSDI":["Warm spell duration indicator","temperature","Duration","days","Annual number of days with at least 6 consecutive days when Tmax > 90th percentile"],
    
    "TX10p":["Cool days","temperature", "Frequency", "percen of days", "Share of days when Tmax < 10th percentile"],
    "TN10p":["Cool nights","temperature", "Frequency", "percen of days", "Share of days when Tmin < 10th percentile"],
    "TX90p":["Warm days","temperature", "Frequency", "percen of days", "Share of days when Tmax > 90th percentile"],
    "TN90p":["Warm nights","temperature", "Frequency", "percen of days", "Share of days when Tmin > 90th percentile"],
    "FD":["Frost days","temperature", "Frequency", "days", "Annual number of days when Tmin < 0°C"],
    "ID":["Icing days","temperature", "Frequency", "days", "Annual number of days when Tmax < 0°C"],
    "SU":["Summer days","temperature", "Frequency", "days", "Annual number of days when Tmax > 25°C"],
    "TR":["Tropical nights","temperature", "Frequency", "days", "Annual number of days when Tmin > 20°C"],

    "Rx1day":["Max 1-day precipitation","precipitation", "Intensity", "mm", "Maximum 1-day precipitation total"],
    "Rx5day":["Max 5-day precipitation","precipitation", "Intensity", "mm", "Maximum 5-day precipitation total"],
    "SDII":["Simple daily intensity index","precipitation", "Intensity", "mm/day", "Annual total precipitation divided by the number of wet days (i.e., when precipitation ≥ 1.0 mm)"],
    "R95p":["Annual contribution from very wet days","precipitation", "Intensity", "mm", "Annual sum of daily precipitation > 95th percentile"],
    "R99p":["Annual contribution from extremely wet days","precipitation", "Intensity", "mm", "Annual sum of daily precipitation > 99th percentile"],
    "PRCPTOT":["Annual contribution from from wet days","precipitation", "Intensity", "mm", "Annual sum of daily precipitation > 99th percentile"],
    
    "CWD":["Consecutive wet days","precipitation", "Duration", "days", "Maximum annual number of consecutive wet days (i.e., when precipitation ≥ 1 mm)"],
    "CDD":["Consecutive dry days","precipitation", "Duration", "days", "Maximum annual number of consecutive dry days (i.e., when precipitation ≥ 1 mm)"],
   
    "R10mm":["Heavy precipitation days","precipitation", "Frequency", "days", "Annual number of days when precipitation ≥ 10 mm"],
    "R20mm":["Very heavy precipitation days","precipitation", "Frequency", "days", "Annual number of days when precipitation ≥ 20 mm"],
}
########################### GHCNDEX ###########################
########################### GHCNDEX ###########################
########################### GHCNDEX ###########################
basepath = "../dataset/ghcndex_current/"
files = os.listdir(basepath)
index = 0
month = ['Ann','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']


    
for i in files:
    name = i.split(".")
    collect = name[0].split("_")[1]
    print(collect)
    dataset = 'ghcndex'
    data, aryLatLon = get_data(f"{basepath}{i}")
    if(collect in compare):
        detail = {
            "index_name": collect, # TXx
            "short_name": compare[collect][0], # Max Tmax
            "type_measure": compare[collect][1], # temperature
            "method": compare[collect][2], # intensity
            "unit": compare[collect][3], # °C def
            "description": compare[collect][4], # °C def
            "dataset": dataset, # ghcendex
        }
    else:
        detail = {
            "index_name": collect, # TXx
            "short_name": None, # Max Tmax
            "type_measure": None, # temperature
            "method": None, # intensity
            "unit": None, # °C def
            "description": None, # °C def
            "dataset": dataset, # ghcendex
        }
    insertTomongo(data,f'ghcndex_{collect}', detail, aryLatLon, 1951)
    index+=1


########################### HADEX2 ###########################
########################### HADEX2 ###########################
########################### HADEX2 ###########################
basepath = "../dataset/hadex2_current/"
files = os.listdir(basepath)
index = 0
month = ['Ann','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    
for i in files:
    name = i.split(".")
    collect = name[0].split("_")[1]
    print(collect)
    dataset = 'hadex2'
    data, aryLatLon = get_data(f"{basepath}{i}")
    if(collect in compare):
        detail = {
            "index_name": collect, # TXx
            "short_name": compare[collect][0], # Max Tmax
            "type_measure": compare[collect][1], # temperature
            "method": compare[collect][2], # intensity
            "unit": compare[collect][3], # °C def
            "description": compare[collect][4], # °C def
            "dataset": dataset, # ghcendex
        }
    else:
        detail = {
            "index_name": collect, # TXx
            "short_name": None, # Max Tmax
            "type_measure": None, # temperature
            "method": None, # intensity
            "unit": None, # °C def
            "description": None, # °C def
            "dataset": dataset, # ghcendex
        }
    insertTomongo(data,f'hadex2_{collect}', detail, aryLatLon, 1901)
    index+=1

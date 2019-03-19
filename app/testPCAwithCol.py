import numpy as np
import matplotlib.pyplot as plt
from scipy import signal

from lib.pcaservice import Pca_service
# from lib.mapGenrate import My_mapGenerate
from lib.mongoDB import MongoDB_lc


def year_ary(init,end):
    init = init.split("-")
    yearinit = int(init[0])
    end = end.split("-")
    yearend = int(end[0])
    countYear = yearend-yearinit+1
    year = []
    for i in range(0,countYear):
        year.append(yearinit+i)

    return year, [int(init[1]),int(end[1])]


def genData(arr):
        N = len(arr)
        std = np.std(arr)
        mean = np.mean(arr)
        z = []
        for i in arr:
            z.append( (i - mean)/std )
        return z

def corr_coef_fn(dataset1, dataset2, date, name1, name2, lat, lon, map1eof, map2eof):
    arr = [] 
    print("in Coef")
    setting = {
        "lat": lat,
        "lon": lon
    }

    # print(dataset1.shape)
    for i1 in range(6):
        for i2 in range(6):
            
            cor = round(np.corrcoef([dataset1[i1], dataset2[i2]])[0][1],2)
            if(cor >= 0.3 or cor <= -0.3):
                print(f"{name1} {i1} {name2} {i2} corr:", cor)  
            

                
                dataset1_ = signal.detrend(dataset1[i1])
                dataset1_ = np.array(genData(dataset1_))   
                
                dataset2_ = signal.detrend(dataset2[i2])
                dataset2_ = np.array(genData(dataset2_))   

                plt.plot(dataset1_)
                plt.plot(dataset2_)
                
                plt.title(f"{name1} {i1} {name2} {i2} corr: {cor}")
                
                # a = My_mapGenerate(map1eof[i1], setting, f"{name1} {i1}")            
                # a.getMap(True)
                # b = My_mapGenerate(map2eof[i2], setting, f"{name2} {i2}")  
                # b.getMap(True)          
                # plt.legend()
                # print(round(np.corrcoef([dataset1[i], dataset2[i]])[0][1],2))
                # pr
                plt.savefig(f"./pig/graph/{name1}_{i1} {name2}_{i2}")
                plt.show()



def getDetail(collection):
    objDB= MongoDB_lc()
    objDB.collection(collection)
    return objDB.mongo_findDetail(collection)

type_dataset = "ghcndex"
type_index = "TXx"

def find_pca(type_index, type_dataset="ghcndex", yearInit="1951-1", yearEnd="2017-12"):
    # yearInit = "1951-1" 
    # yearEnd = "2017-12"
    comp = 6


    # print(f"getmapAverage : {type_dataset, yearInit, yearEnd, type_index}")

    ary, month_IE = year_ary(yearInit, yearEnd)

    collection = f"{type_dataset}_{type_index}"
    # print(collection)
    detail = getDetail(collection)
    # print(detail["lat_list"])
    # return
    obj = Pca_service(ary, collection, month_IE[0], month_IE[1])

    pca_pc, pca_eofs, pca_va_ratio =  obj.getPCA_service(comp)
    ratioX = np.linspace(1, comp, num=comp)
    date = obj.date
    # print(pca_pc.shape)
    # print(date)
    # print(len(date))
    return [pca_pc, pca_eofs, date, detail]



# detail = {}
# indexofPca1 = "TXx"
# indexofPca2 = "TX10p"
# pca1, sss,date, detail= find_pca(indexofPca1)
# pca2, ddd,date, detail= find_pca(indexofPca2)
# print(detail)
# corr_coef_fn(pca1, pca2, date, indexofPca1, indexofPca2, detail["lat_list"], detail["lon_list"], sss, ddd)

indexS = ['TXx', 'CDD', 'CSDI', 'CWD', 'DTR', 'FD', 'GSL', 'ID', 'PRCPTOT', 'R10mm', 'R20mm', 'R95pT', 'R95p', 'R99p', 'Rx1day', 'Rx5day', 'SDII', 'SU', 'TN10p', 'TN90p', 'TNn', 'TNx', 'TR', 'TX10p', 'TX90p', 'TXn', 'WSDI']
# for k in compare:
#     print(k)
print(len(indexS))
##############################################################################
i = 0
while(i < len(indexS)):
    j = i+1
    while(j < len(indexS)):
        try:
            # print("------------------")
            
            pca1, map1, date, detail = find_pca(indexS[i])
            pca2, map2, date, detail = find_pca(indexS[j])


            print(f"{indexS[i]} and {indexS[j]} {i} {j}")
            corr_coef_fn(pca1, pca2, date, indexS[i], indexS[j], detail["lat_list"], detail["lon_list"], map1, map2)
            
            # print(f"{indexS[i]} and {indexS[j]} {i} {j}")

            # break
            # print(indexS[i], indexS[j])
               
            # break
        except:
            pass
            # print(f"{indexS[i]} and {indexS[j]} is Error")
            # j+=1   
            # break
        j+=1
            
    # print("--------------------------------")
    i+=1
    break


from flask_cors import CORS
from app import app
from flask import Flask, jsonify, request
import numpy as np
from .lib.mongoDB import MongoDB_lc
import json
import pandas as pd
from .lib.linearRegressGen import Linear_regression
from .lib.classRegridsNew import Regrids

app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/api/*": {"origins": ["*"]}})


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

def getDetail(collection):
    objDB= MongoDB_lc()
    objDB.collection(collection)
    return objDB.mongo_findDetail(collection)

def checkSize(arr):
    print("incheck")
    print(arr)
    if arr[0] * arr[1] > 20000:
        return True
    return False

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"

@app.route('/api/getDataset')
def getDataset():
    objDB= MongoDB_lc()
    objDB.collection("dataset")
    result = objDB.mongo_findDataset("haveDataset")
    print(result)
    for data in result:
        r = data["haveDataset"]
    return jsonify({
        "datasets" : r
    })

@app.route('/api/getdetailDataset/<dataset>')
def getdetailDataset(dataset):
    objDB= MongoDB_lc()
    objDB.collection("dataset")
    result = objDB.mongo_findDataset(dataset)
    # print(result)
    for data in result:
        r = data[dataset]
    return jsonify({
        dataset : r
    })


@app.route('/api/getgeocountry')
def getGeojson():
    with open('./app/dataset/geojson/countries.json') as f:
        data = json.load(f)
    return jsonify(data)

@app.route('/api/getmap/mapAVG/<type_dataset>/<yearInit>/<yearEnd>/<type_index>/')
def getmapAverage(type_dataset, yearInit, yearEnd, type_index):
    from .lib.average import Average_service
    print(f"getmapAverage : {type_dataset, yearInit, yearEnd, type_index}")
    ary, month_IE = year_ary(yearInit, yearEnd)
    collection = f"{type_dataset}_{type_index}"

    obj = Average_service(ary, collection, month_IE[0], month_IE[1])
    dataM = obj.getAverageMap(0) # Ann = 0 Jan = 1 ..... Dec = 12
    print("----------------------------------------------------",dataM)
    # df = pd.DataFrame(dataM)
    # df.fillna(-99.99, inplace=True)
    
    # print(np.count_nonzero(~np.isnan(dataM)))
    dataM[np.isnan(dataM)] = -99.99
    # print(dataM.shape)
    # count = 0
    # for i in range(0,len(dataM)):
    #     for j in range(0, len(dataM[i])):
    #         if(dataM[i][j] != -99.99):
    #             count += 1

    # print(count)
    # print("ssssssssVCVVVVVVVVVVVVVVVVVV",dataM.shape)
    deatail = getDetail(collection)
    if(checkSize(dataM.shape)):
        regridOBJ = Regrids()
        dataM = regridOBJ.regrids(dataM)
        ulatlon = regridOBJ.getLatLon_regrid(np.array(deatail["lat_list"]), np.array(deatail["lon_list"]))
        deatail["lat_list"] = ulatlon["lat"].tolist()
        deatail["lon_list"] = ulatlon["lon"].tolist()

    # print(deatail["lat_list"])
    # print(deatail["lon_list"])
    # print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
    # print(len(deatail["lat_list"]))
    return jsonify(
        {
            "detail": deatail,
            "map": { 
                "mapAVG": dataM.tolist()
            }
        }
    )

@app.route('/api/getData/Graph/<type_dataset>/<yearInit>/<yearEnd>/<type_index>/')
def getSeasonalandAVG(type_dataset, yearInit, yearEnd, type_index):
    from .lib.average import Average_service
    print(f"getSeasonalandAVG : {type_dataset, yearInit, yearEnd, type_index}")
    
    ary, month_IE = year_ary(yearInit, yearEnd)
    collection = f"{type_dataset}_{type_index}"

    # SERVICE GET Average Graph Ann
    a = Average_service(ary, collection, month_IE[0], month_IE[1])
    dataA, year  = a.getAverageGraph(0)
   
    # regAVG_ann = Linear_regression(dataA)
    # dataTrend_ann = regAVG_ann.predict_linear()
    
    # SERVICE GET Seasonal Graph
    b = Average_service(ary, collection, month_IE[0], month_IE[1])
    dataS = b.getSeasonal()
    print(dataS)
    # SERVICE GET Average Graph all
    c = Average_service(ary, collection, month_IE[0], month_IE[1])
    dataAll, yearAll  = c.getAverageGraph()

    # regAVG_all = Linear_regression(dataAll)
    # dataTrend_all = regAVG_all.predict_linear()

    deatail = getDetail(collection)
    month = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    # pop lat lon
    deatail.pop('lat_list', None)
    deatail.pop('lon_list', None)

    try:
        regAVG_all = Linear_regression(dataAll)
        dataTrend_all = regAVG_all.predict_linear()
    except:
        dataTrend_all = np.array([])

    regAVG_ann = Linear_regression(dataA)
    dataTrend_ann = regAVG_ann.predict_linear()


    return jsonify(
        {
            "detail": deatail,
            "graph":{
                "graphAVGAnn": {
                    "TaxisY": dataTrend_ann.tolist(),
                    "axisX":year,
                    "axisY":dataA.tolist()
                },
                "graphSeasonal": {
                    "axisX":month,
                    "axisY":dataS.tolist()
                },
                "graphAVG": {
                    "TaxisY": dataTrend_all.tolist(),
                    "axisX":yearAll,
                    "axisY":dataAll.tolist()
                },
            }
        }
    )

@app.route('/api/getmap/mapPCA/<type_dataset>/<yearInit>/<yearEnd>/<type_index>/')
def getmapPCA(type_dataset, yearInit, yearEnd, type_index):
    from .lib.pcaservice import Pca_service
    print(f"getmapPCA : {type_dataset, yearInit, yearEnd, type_index}")
    ary, month_IE = year_ary(yearInit, yearEnd)
    collection = f"{type_dataset}_{type_index}"

    pca_eofs = []
    comp = 6
    # SERVICE GET PCA map and graph
    
    try:
        obj = Pca_service(ary, collection, month_IE[0], month_IE[1])
        pca_pc, pca_eofs, pca_va_ratio =  obj.getPCA_service(comp)
        ratioX = np.linspace(1, comp, num=comp)
        date = obj.date
        print(pca_va_ratio.shape)
        print(ratioX)

        objVar = Pca_service(ary, collection, month_IE[0], month_IE[1])
        dataVar = objVar.getVarianceMap()
        dataVar[np.isnan(dataVar)] = -99.99
    except:
        obj = Pca_service(ary, collection, month_IE[0], month_IE[1])
        pca_pc, pca_eofs, pca_va_ratio =  obj.getPCA_service(comp, month=0)
        ratioX = np.linspace(1, comp, num=comp)
        date = obj.date
        print(pca_va_ratio.shape)
        print(ratioX)

        objVar = Pca_service(ary, collection, month_IE[0], month_IE[1])
        dataVar = objVar.getVarianceMap(month=0)
        dataVar[np.isnan(dataVar)] = -99.99

        # pca_pc, pca_va_ratio, ratioX = np.array([[],[],[]])
        # date = []
        # dataVar = np.array([])
    
    
    # dataM[np.isnan(dataM)] = -99.99
    pca_eofs = np.array(pca_eofs)
    deatail = getDetail(collection)
    pca_arr = []
    if(checkSize(pca_eofs[0].shape)):
        regridOBJ = Regrids()
        for i in range(6):
            pca_arr.append(regridOBJ.regrids(pca_eofs[i]).tolist())
        print("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")

        dataVar = regridOBJ.regrids(dataVar)
        # t = np.array(pca_arr)
        # print(t.shape)
        # print(len(pca_arr[0]))
        pca_eofs = pca_arr
        ulatlon = regridOBJ.getLatLon_regrid(np.array(deatail["lat_list"]), np.array(deatail["lon_list"]))
        deatail["lat_list"] = ulatlon["lat"].tolist()
        deatail["lon_list"] = ulatlon["lon"].tolist()


        # print(pca_eofs)
    else:
        pca_eofs = pca_eofs.tolist()    

    return jsonify(
        {
            "detail": deatail,
            "graph":{
                "time":{
                    "axisX":date, # chnage
                    "axisY":pca_pc.tolist()
                },
                "ratio":{
                    "axisX":ratioX.tolist(),
                    "axisY":pca_va_ratio.tolist()
                }
            },
            "map": { 
                "mapPCA": pca_eofs,
                "mapVar": dataVar.tolist()
            }
        }
    )

@app.route('/api/getmap/mapTrend/<type_dataset>/<yearInit>/<yearEnd>/<type_index>/')
def getmapHypoTrend(type_dataset, yearInit, yearEnd, type_index):
    from .lib.hypotasisTest import Trend_service
    print(f"getmapHypoTrend : {type_dataset, yearInit, yearEnd, type_index}")
    ary, month_IE = year_ary(yearInit, yearEnd)
    collection = f"{type_dataset}_{type_index}"
    deatail = getDetail(collection)

    collection = f"{type_dataset}_{type_index}_trend"
    obj = Trend_service(ary, collection, month_IE[0], month_IE[1])
    tempR, hypoLat = obj.trendAndHypoFromDB()
    
    if(tempR.shape == ()):
        print("warning : No have data trend in mongo")
        # SERVICE GET Hypo and Trend
        import time
        collectionDB = f"{type_dataset}_{type_index}"
        obj = Trend_service(ary, collectionDB, month_IE[0], month_IE[1])
        dataRaw = obj.getData(0)
        print("************************************************************")
        arr = []
        regridOBJ = Regrids()
        for i in range(dataRaw.shape[0]):
            
            arr.append(regridOBJ.regrids(dataRaw[i]))
        arr = np.array(arr)
        print(arr.shape)
        print("tempR", arr.shape)
        if checkSize(tempR.shape):
            regridOBJ = Regrids()
            ulatlon = regridOBJ.getLatLon_regrid(np.array(deatail["lat_list"]), np.array(deatail["lon_list"]))
            deatail["lat_list"] = ulatlon["lat"].tolist()
            deatail["lon_list"] = ulatlon["lon"].tolist()
            print("SSSSS")
        # ulatlon = regridOBJ.getLatLon_regrid(np.array(deatail["lat_list"]), np.array(deatail["lon_list"]))
        # deatail["lat_list"] = ulatlon["lat"].tolist()
        # deatail["lon_list"] = ulatlon["lon"].tolist()

        print("////////////////////////////////////////////////////////////")
        start = time.time()
        tempR, hypoLat = obj.trendAndHypo(dataRaw)
        print(time.time() - start)
        print(tempR.shape)
        print(hypoLat.shape)
        tempR[tempR == 0] = -99.99
        obj.insertTomongo(tempR.tolist(),hypoLat.tolist(), collectionDB)
    else:
        if checkSize(tempR.shape):
            regridOBJ = Regrids()
            ulatlon = regridOBJ.getLatLon_regrid(np.array(deatail["lat_list"]), np.array(deatail["lon_list"]))
            deatail["lat_list"] = ulatlon["lat"].tolist()
            deatail["lon_list"] = ulatlon["lon"].tolist()
            print("SSSSS")
            
        print("warning :Have data trend in mongo")
        tempR[np.isnan(tempR)] = -99.99
    
    tempData = tempR.copy()
    tempHypo = hypoLat.copy()
    tempData[tempData > 0] = 1
    tempData[tempData < 0] = -1
    tempData[tempHypo == 1] = -99.99

    return jsonify(
        {
            "detail": deatail,
            "map": { 
                "mapTREND": tempR.tolist(),
                "hiypo": tempData.tolist(),
            }
        }
    )


@app.route('/api/getdata/selectGraph/', methods=['POST'])
def getSlectGraph():
    print(f"getSlectGraph")
    from .lib.selectservice import SelectCus_service
    if(request.method == 'POST'):
        data = request.data
        data = json.loads(data)
        type_dataset = data['type_dataset']
        yearInit = data['yearInit']
        yearEnd = data['yearEnd']
        type_index = data['type_index'].lower()
        custom = data['custom']
        print("////////////////////////////////////")
        
        print(data)
        # print(custom)
        print("////////////////////////////////////")
        print(f"getmapAverage : {type_dataset, yearInit, yearEnd, type_index}")

        ary, month_IE = year_ary(yearInit, yearEnd)
        collection = f"{type_dataset}_{type_index}"

        obj = SelectCus_service(ary, collection, month_IE[0], month_IE[1])
        # print(obj)
        # for i in obj:
        #     print(i)
        dataAll, yearAll  = obj.getAverageGraphCus(custom)
        tempMedian = np.nanmedian(dataAll)
        dataAll[np.isnan(dataAll)] = tempMedian

        obj1 = SelectCus_service(ary, collection, month_IE[0], month_IE[1])
        dataA, year  = obj1.getAverageGraphCus(custom, 0)
        tempMedian = np.nanmedian(dataA)
        dataA[np.isnan(dataA)] = tempMedian

        print(dataA)
        print("AAAAAAAAAAAAA")
        obj2 = SelectCus_service(ary, collection, month_IE[0], month_IE[1])
        dataS  = obj2.getSeasonalCus(custom)
        tempMedian = np.nanmedian(dataS)
        dataS[np.isnan(dataS)] = tempMedian

        month = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
        deatail = getDetail(collection)
        # pop lat lon
        deatail.pop('lat_list', None)
        deatail.pop('lon_list', None)

        try:
            regAVG_all = Linear_regression(dataAll)
            dataTrend_all = regAVG_all.predict_linear()
        except:
            dataTrend_all = np.array([])

        regAVG_ann = Linear_regression(dataA)
        dataTrend_ann = regAVG_ann.predict_linear()

        return jsonify(
            # {"ssss":"ddddddddd"}
            {
                "detail": deatail,
                "graph":{
                    "graphAVGAnn": {
                        "TaxisY": dataTrend_ann.tolist(),
                        "axisX":year,
                        "axisY":dataA.tolist()
                    },
                    "graphSeasonal": {
                        "axisX":month,
                        "axisY":dataS.tolist()
                    },
                    "graphAVG": {
                        "TaxisY": dataTrend_all.tolist(),
                        "axisX":yearAll,
                        "axisY":dataAll.tolist()
                    },
                }
            }
        )
    else:
        return jsonify(
            {
                "status": "Error"
            }
        )
    

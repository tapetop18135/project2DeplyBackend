import pymongo

class MongoDB_lc():

    def __init__(self, url='localhost', port=27017):
        self.client = pymongo.MongoClient(url, port)
        self.__database()

    def __database(self, name='climate_db'):
        # Getting a Database
        self.db = self.client[name]

    def collection(self, name):
        # Getting a Collection
        self.col = self.db[name]

    def mongo_insert(self, data):
        # Getting a Collection
        self.col.insert(data)

    def mongo_find(self, AryYear):
        result = self.col.find(
            { "year": { "$in":  AryYear}}
        )
        return result

    def mongo_findDataset(self, dataset):
        result = self.col.find({dataset : { "$exists" : True } })
        return result

    def mongo_findDetail(self, keyfind):
        result = self.col.find(
            { "key__" : keyfind }
        )
        for re in result:
            detail = re['detail']
        return detail
    
    def mongo_findDetail(self, keyfind):
        result = self.col.find({"detail" : { "$exists" : True }}
        )
        
        # result = self.col.find(
        #     { "key__" : keyfind }
        # )
        for re in result:
            detail = re['detail']
            lat = re['lat']
            lon = re['lon']
        detailall = {
            "detail": detail,
            "lat_list" : lat,
            "lon_list" : lon
        }
        return detailall
    
    def mongo_findTrend(self, keyfind):
        result = self.col.find(
            { "duration" : keyfind }
        )
        return result

# a = MongoDB_lc()
# a.collection('ghcndex_TXx')
# post = {"author": "Mike","text": "My first blog post!","tags": ["mongodb", "python", "pymongo"]}
# a.mongo_insert(post)
# year = [1951, 1952, 1953, 1954]
# for d in a.mongo_find(year):
#     print(d['year'])
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score

class Linear_regression():

    def __init__(self,data):
        self.dataAxisY = data
        self.__genAxisX()
    
    def __genAxisX(self):
        tempX = []
        for i in range(0,len(self.dataAxisY)):
            tempX.append([i])
        tempX = np.array(tempX)
        self.dataAxisX = tempX

    def predict_linear(self):
        regr = linear_model.LinearRegression()
        regr.fit(self.dataAxisX, self.dataAxisY)
        return regr.predict(self.dataAxisX)

# data = [1,2,3,4,5]
# obj = Linear_regression(data)
# dataTrend = obj.predict_linear()
# print(dataTrend)
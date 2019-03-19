import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np

class My_mapGenerate():

    def __init__(self, data, seting, title):
        self.fname = title
        self.data = data
        self.seting = seting
    def getMap(self, save= False):
        temp = self.data
        seting = self.seting
        nlat = len(seting['lat'])
        nlon = len(seting['lon'])
        vlatmin = seting['lat'][0]
        vlatmax = seting['lat'][nlat-1]
        vlonmin = seting['lon'][0]
        vlonmax = seting['lon'][nlon-1]
        parallels = np.arange(vlatmin,vlatmax,30.)
        meridians = np.arange(vlonmin,vlonmax,30)
        lat = np.linspace(vlatmin,vlatmax,nlat)
        lon = np.linspace(vlonmin,vlonmax,nlon)
        temp = np.reshape(temp,(nlat,nlon))
        
        plt.title(self.fname)
        m = Basemap(projection='cyl', llcrnrlon=min(lon), llcrnrlat=min(lat), urcrnrlon=max(lon), urcrnrlat=max(lat))    
        x, y = m(*np.meshgrid(lon, lat))
        clevs = np.linspace(np.min(temp.squeeze()), np.max(temp.squeeze()), 21)
        cs = m.contourf(x, y, temp.squeeze(), clevs, cmap=plt.cm.RdBu_r)
        m.drawcoastlines()  
        m.drawparallels(parallels, labels=[1,0,0,0])
        m.drawmeridians(meridians, labels=[1,0,0,1])
        m.colorbar()
        if(save):
            plt.savefig(self.fname)
        plt.show()

# a = My_mapGenerate('test')
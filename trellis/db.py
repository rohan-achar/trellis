import psycopg2 as db
import json, re
from urllib2 import Request, urlopen, HTTPError, URLError, unquote
from httplib import HTTPException
import time
import pygeocoder
from threading import Thread

geocoder = pygeocoder.Geocoder()
geocoder.api_key = 'AIzaSyCs2_XqCA7gDOF5DX8zffaM1cSgGmD4mpQ'
MAX_UPVOTES_ALLOWED = 1000000
file = open("../creds.json", "r")
creds = json.load(file)
file.close()
matches = re.compile(".*(<a.*?id=\"download_button_link\".*?>).*")
urlmatches = re.compile(".*(https://www.dropbox.com/.*?)\".*")
DLinkCache = {}

def extractLink(link):
    urlreq = Request(link, None, {"User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36"})
    try:
        urldata = urlopen(urlreq)
        data = urldata.read()
        return urlmatches.findall(matches.findall(data)[0])[0]
    except HTTPError:
        return ""
    except URLError:
        return ""
    except IndexError:
        return ""
    except HTTPException:
        return ""

def GetQuery(lat, lon, max_distance, number_of_results):
    try:
        start = time.clock()
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
        cursor = connection.cursor()
        cursor.execute("select vlink, rating, availability, thumb, lat, lon, ST_Distance(location, Geography(ST_MakePoint(" + str(lon) + ", " + str(lat) + "))), address, dlink from grapes where ST_Dwithin(location, Geography(ST_MakePoint(" + str(lon) + ", " + str(lat) + ")), " + str(max_distance) + ");")
        results = cursor.fetchall()
        if max_distance > 300:
            results.sort(key = lambda x: x[6])
            buckets = [[],[],[]]
            for x in results:
                buckets[int(float(x[6])/(max_distance/3))].append(x)
            results = []
            for i in buckets:
                #print(i)
                if i != []:
                    results.extend(sorted(i, key = lambda x: -float(x[2])))
        else:
            results.sort(key = lambda x: float(x[2]), reverse = True)
        
        output = []
        count = 0
        for link, rating, availability, thumb, lat, lon, dist, address, dlink in results:
            if availability:
                entry = {}
                if dlink == "":
                    continue
                count += 1
                entry["link"] = dlink
                entry["original"] = link
                entry["rating"] = float(rating)
                entry["thumbnail"] = thumb
                entry["lat"] = float(lat)
                entry["lon"] = float(lon)
                entry["distance"] = float(dist)
                entry["address"] = address
                entry["time_ret"] = time.clock() - start
                output.append(entry)
            if count >= number_of_results:
                break
        connection.close()
        return True, output
    except StandardError, e:
        connection.rollback()
        connection.close()
        print e
        return False, 

def AddNewLink(lat, lon, link, thumbnail):
    try:
        link = unquote(link)
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
        cursor = connection.cursor()
        cursor.execute("insert into grapes values (\'" + link + "\', 0, true, \'" + thumbnail + "\', " + str(lat) + ", " + str(lon) + ", Geography(ST_MakePoint(" +  str(lon) + ", " + str(lat) +")), \'\', 0, \'\');")
        connection.commit()
        connection.close()
        Thread(target = UpdateExtraFields, args = (link, lat, lon)).start()
        return True, "Success"
    except StandardError, e:
        connection.rollback()
        connection.close()
        print(e)
        return False, 

def UpdateExtraFields(link, lat, lon):
    try:
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
        cursor = connection.cursor()
        addr = geocoder.reverse_geocode(lat, lon, 0)
        addr = addr.formatted_address
        addparts = addr.split(", ")
        if (len(addparts) > 3):
            addparts = addparts[-4:-1]
            addparts[-1] = addparts[-1].split()[0]
            addr = ", ".join(addparts)

        cursor.execute("update grapes set address = \'" + addr + "\' where vlink = \'" + link + "\';")
        connection.commit()
        connection.close()
        UpdateLink(link, 0)
    except pygeocoder.GeocoderError:
        print("Middle of nowhere")
    except db.DatabaseError:
        connection.rollback()
        connection.close()

def UpdateRating(link, amount):
    try:
        link = unquote(link)
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
        cursor = connection.cursor()
        cursor.execute("update grapes set rating = rating + " + str(amount) + " where vlink = \'" + link + "\';")
        connection.commit()
        connection.close()
        return True, "Success"
    except StandardError, e:
        connection.rollback()
        connection.close()
        print(e)
        return False,
                        
def UpdateLink(link, nacount):
    try:
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
        cursor = connection.cursor()
            
        url = extractLink(link)
        if url != "":
            cursor.execute("update grapes set dlink = \'" + url + "\' where vlink = \'" + link + "\';")
            cursor.execute("update grapes set availability = true where vlink = \'" + link + "\';")
        else:
            if nacount > 23:
                cursor.execute("delete from grapes where vlink = \'" + link + "\';")
            else:
                cursor.execute("update grapes set availability = false where vlink = \'" + link + "\';")
                cursor.execute("update grapes set nacount = nacount + " + str(nacount + 1) + " where vlink = \'" + link + "\';")
        connection.commit()
    except StandardError, e:
        connection.rollback()
        connection.close()
        print(e)
        

def UpdateDlinks():
    try:    
        while True:
            connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = creds["port"])
            cursor = connection.cursor()
            cursor.execute("select vlink, availability, nacount from grapes;")
            results = cursor.fetchall()
            count = 0
            for result in results:
                UpdateLink(result[0], result[2]);
                time.sleep(1)
                count += 1
            if count < 3600:
                time.sleep(3600 - count)

    except StandardError, e:
        print(e)
        file = open("../error.txt", "a")
        file.write("e, " + time.strftime("%c") + ", Refresh Thread shutting down, " + str(e))
        file.close()
        
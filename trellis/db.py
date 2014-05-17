import psycopg2 as db
import json, re
from urllib2 import Request, urlopen, HTTPError, URLError, unquote
from httplib import HTTPException
MAX_UPVOTES_ALLOWED = 1000000
file = open("../creds.json", "r")
creds = json.load(file)
file.close()
matches = re.compile(".*(https://dl\.dropboxusercontent\.com/s/.*?\.mp4.*?)\".*")

def extractLink(link):
    urlreq = Request(url, None, {"User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36"})
    try:
        urldata = urlopen(urlreq)
        data = urldata.read()
        return matches.findall(data)[0]
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
        connection = db.connect(database = "trellis", user = creds["name"], password = creds["pass"], host = '127.0.0.1', port = 5433)
        cursor = connection.cursor()
        cursor.execute("select vlink, rating, availability, thumb, lat, lon, ST_Distance(location, Geography(ST_MakePoint(" + str(lon) + ", " + str(lat) + "))) from grapes where ST_Dwithin(location, Geography(ST_MakePoint(" + str(lon) + ", " + str(lat) + ")), " + str(max_distance) + ");")
        results = cursor.fetchall()
        if max_distance > 300:
            results.sort(key = lambda x: x[6])
            buckets = [[],[],[]]
            for x in results:
                buckets[int(float(x[6])/(max_distance/3))].append(x)
            results = []
            for i in buckets:
                print(i)
                if i != []:
                    results.extend(sorted(i, key = lambda x: -float(x[2])))
        else:
            results.sort(key = lambda x: float(x[2]), reverse = True)
        
        output = []
        count = 0
        for link, rating, availability, thumb, lat, lon, dist in results:
            entry = {}
            dlink = extractLink(link)
            if dlink == "":
                continue
            count += 1
            entry["link"] = dlink
            entry["rating"] = float(rating)
            entry["thumbnail"] = thumb
            entry["lat"] = float(lat)
            entry["lon"] = float(lon)
            entry["distance"] = float(dist)
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
        connection = db.connect(5433, creds["name"], creds["pass"], '127.0.0.1', 'trellis')
        cursor = connection.cursor()
        cursor.execute("insert into grapes values (\'" + link + "\', 0, true, \'" + thumbnail + "\', " + str(lat) + ", " + str(lon) + ", Geography(ST_MakePoint(" +  str(lon) + ", " + str(lat) +")));")
        connection.commit()
        connection.close()
        return True, "Success"
    except StandardError, e:
        connection.rollback()
        connection.close()
        print(e)
        return False, 

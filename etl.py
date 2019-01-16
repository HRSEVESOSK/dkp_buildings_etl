import os,ogr,json,pgsql,datetime,argparse
ap = argparse.ArgumentParser()
ap.add_argument("-input",required=False,dest="input",help="D:\gis_data\DKP\KOPRIVNICA")
args = vars(ap.parse_args())
if args['input']:
    INPUT_DIR = args['input']
else:
    INPUT_DIR = 'D:\gis_data\DKP'

print(INPUT_DIR)
INSERTED=0
WASTHERE=0
UPDATE=0
DELETED=0
connection = pgsql.PGSql()
start = datetime.datetime.now()
print("*** ETL PROCESS START ***")
for root, dirs, files in os.walk(INPUT_DIR, topdown = False):
   for name in files:
        if name.endswith('_3010.shp'):
           fnArray = name.split("_")
           cadDistrName = fnArray[1]
           cadDistrId = fnArray[0]
           filePath = os.path.join(root, name)
           print("Cadastral district '{0}' with cadastral district identifier '{1}' has buildings dataset '{2}'".format(cadDistrName, cadDistrId,filePath))
           ogrOpen = ogr.Open(filePath)
           fts = ogrOpen.GetLayer()
           srs = fts.GetSpatialRef()
           epsg = srs.GetAttrValue("AUTHORITY", 1)
           print("Shapefile '{0}' has '{1}' features. ".format(os.path.basename(filePath),len(fts)))
           for feature in fts:
               geom = feature.GetGeometryRef()
               EPSG = '3765'
               jsonData = feature.ExportToJson()
               dictData = json.loads(jsonData)
               id = os.path.basename(filePath) + "_" + str(dictData['id'])
               geom = dictData['geometry']
               geom['crs'] = {"type":"name","properties":{"name":"EPSG:"+EPSG}}
               data = dictData['properties']
               sql = "SELECT oid FROM dkp_zgrada WHERE id='%s'" % id
               connection.connect()
               objExists = connection.query(sql, False)
               connection.close()
               if not objExists:
                   sql = "INSERT INTO dkp_zgrada(id,geom,data) " \
                         "VALUES ('%s',ST_GeomFromGeoJSON('%s'),('%s') )" \
                         "RETURNING oid" % (id,json.dumps(geom),json.dumps(data,ensure_ascii=False))
                   connection.connect()
                   insObj = connection.query(sql, False)
                   connection.close()
                   if insObj:
                       INSERTED = INSERTED + 1
                       print("SUCESSS: Object id '%s' has been inserted into database with oid '%s'" % (id,insObj[0][0]))
                   else:
                       print("ERROR: Something went wrong with insertion of object id '%s'" % id)
               else:
                   WASTHERE = WASTHERE + 1
                   print("INFO: Object id '%s' is already in database with oid '%s'. Checking the content. TBD" % (id,objExists[0][0]))
end = datetime.datetime.now()
print("*** ETL PROCESS FINISHED IN '%s' WITH STATS: INSERTED '%s' WAS THERE '%s' ***" % ((end-start),INSERTED,WASTHERE))
import os,ogr,json,pgsql
INPUT_DIR = 'D:\gis_data\DKP'
connection = pgsql.PGSql()
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
               sql = "SELECT id FROM dkp_zgrada WHERE id='%s'" % id
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
                       print("SUCESSS: Object has been inserted into database with oid '%s'" % insObj[0][0])
                   else:
                       print("ERROR: Something went wrong with insertion of object id '%s'" % id)
               else:
                   print("INFO: Object id '%s' is already in database with oid '%s'. Checking the content. TBD" % (id,objExists[0][0]))
           break


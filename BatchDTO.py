#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pymssql
import psycopg2
import sys
import importlib

importlib.reload(sys)
from decimal import Decimal


class TableProps:
    def __init__(self, host, port, user, pwd, database, table, geom):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.database = database
        self.geom = geom
        self.table = table


def GetType(value):
    return {
        int: "INTEGER",
        str: "VARCHAR(50)",
        Decimal: "double  precision",
        bytes: "bytes",
    }.get(type(value), "VARCHAR(50)")


def standGeometryType(type):
    return {
        "ST_MULTISTRING": "MULTILINESTRING",
        "ST_MULTIPOLYGON": "MULTIPOLYGON"
    }.get(type, type[3:])


def fetchMSSQLGeometryTypeAndSrid(src):
    with pymssql.connect(src.host + ":" + str(src.port), src.user, src.pwd, src.database) as srcCon:
        geoUsageCursor = srcCon.cursor(as_dict=True)
        sql = '''SELECT * from ST_GEOMETRY_COLUMNS g left join ST_SPATIAL_REFERENCE_SYSTEMS s on g.srs_id=s.srs_id where table_name =\'%s\'; ''' % (
            src.table.strip())
        print(sql)
        geoUsageCursor.execute(sql)
        geometryInfos = geoUsageCursor.fetchone()
        if geometryInfos:
            print(geometryInfos["type_name"])

            print(geometryInfos["organization_coordsys_id"])
            return standGeometryType(geometryInfos["type_name"]), geometryInfos["organization_coordsys_id"]


def pgDDL(columnsRow, src, dist):
    deletstr = "drop table if exists " + dist.table.strip()
    ddlStr = "create table if not EXISTS " + dist.table.strip() + "("
    insertStr = "insert into  " + dist.table.strip() + "("
    keyList = []
    keyType = []
    src.columDescriber = {}
    print(columnsRow)
    for i, key in enumerate(columnsRow.keys()):
        print(key + " is " + GetType(columnsRow[key]))
        src.columDescriber[key] = GetType(columnsRow[key])
        if key == "srid":
            srid = columnsRow["srid"]
        elif key == "geom":

            continue
            # keyList.append("geom")
            # typeIndex=columnsRow["geom"].find("(")
            # geometryType=columnsRow["geom"][:typeIndex]
            # print(geometryType)
        elif key.upper() == "SHAPE":
            continue
        else:
            keyList.append("\""+key+"\"")
            keyType.append("\""+key+"\"" + "  " + GetType(columnsRow[key]))
    keyList.append("geom")
    geometryType, srid = fetchMSSQLGeometryTypeAndSrid(src)
    src.geomtryType = geometryType
    keyType.append(dist.geom + "  geometry(" + geometryType + "," + str(srid) + ")")
    # set the primary key
    keyType.append("CONSTRAINT %s_pk  PRIMARY KEY (\"OBJECTID\")" % src.table)
    ddlStr += ",".join(keyType)
    ddlStr += ")"
    insertStr += ",".join(keyList)
    insertStr += ") values"

    createIndex = "create UNIQUE INDEX  IF NOT EXISTS " + src.table.strip() + "pkey ON " + dist.table + " USING btree(\"OBJECTID\")"
    createSpatialIndex = "create INDEX IF NOT EXISTS idx_on_" + src.table.strip() + "_geom on " + dist.table + " using gist(geom)"
    return deletstr, ddlStr, insertStr, createIndex, createSpatialIndex, keyList, srid


def batchDTO(src, dist):
    # connect to the mssql
    with pymssql.connect(src.host + ":" + str(src.port), src.user, src.pwd, src.database) as srcCon:
        with srcCon.cursor(as_dict=True) as colCursor:
            colsql = "select top 1  *," + src.geom + ".STAsText() as geom," + src.geom + ".STSrid as srid from " + src.table.strip()
            print(colsql)
            colCursor.execute(colsql)
            colRow = colCursor.fetchone()
            if not colRow:
                print("无法获取列信息")
            else:
                print(colRow)
                print("begin to  ddl in the dist database")
                # 得到创建表语句、插入语句前半段、创建唯一索引、空间索引语句
                deletstr, ddlStr, insertStr, pkIndx, spatialIndex, keysList, projectID = pgDDL(colRow, src,
                                                                                               dist)
                print(deletstr)
                print(ddlStr)
                print(insertStr)
                print(pkIndx)
                print(spatialIndex)
                with psycopg2.connect(database=dist.database, host=dist.host, port=dist.port, user=dist.user,
                                      password=dist.pwd) as distCon:
                    with distCon.cursor() as distCursor:
                        distCursor.execute(deletstr)
                        distCursor.execute(ddlStr)
                        distCon.commit()

                        srcCursor = srcCon.cursor(as_dict=True)
                        # 排除空几何
                        srcCursor.execute("select   *," + src.geom + ".STAsText() as geom from " + src.table)
                        currentRow = srcCursor.fetchone()
                        counter = 1
                        insertArr = []
                        count = 0
                        while currentRow:
                            # print(currentRow)
                            # form the 100 pieces of  data
                            item = []
                            insertList = []
                            count = count + 1
                            for key in keysList:
                                key=key.replace("\"","")
                                if key.lower() == "objectid":
                                    insertList.append(key)
                                    item.append(str(count))
                                    continue
                                if key != "geom":
                                    if src.columDescriber[key] == GetType(str):
                                        insertList.append(key)
                                        item.append("\'" + str(currentRow[key]) + "\'" if currentRow[key] else "\'\'")
                                    elif src.columDescriber[key] == GetType(type(None)):
                                        continue
                                    else:
                                        insertList.append(key)
                                        item.append(str(currentRow[key]) if currentRow[key] else "NULL")
                            # temp=""" %s (%s,ST_GeomFromText(\'%s\',%d))""" % (insertStr,",".join(item),currentRow["geom"],projectID)

                            # handle the the inheritance in the mssql where  a multipolygon may include polygon,which is prohibited from postgis
                            # empty geom
                            if currentRow["geom"] is None:
                                currentRow = srcCursor.fetchone()
                                continue
                            if currentRow["geom"] == "MULTILINESTRING(Y)":
                                currentRow = srcCursor.fetchone()
                                continue
                            geoStrIndex = currentRow["geom"].find("(")
                            currentType = currentRow["geom"][:geoStrIndex].strip()
                            # geostr=(""+currentRow["geom"]).encode("utf-8")
                            # # s="POINT (0 0)".encode("UTF-8")
                            # geom=loads(currentRow["Shape"])
                            # ar=array(geom)
                            # k=0
                            # pairs=[]
                            # for cr in ar:
                            #     k=k+1
                            #     if k % 2 == 1:
                            #         x = cr
                            #     if k % 2 == 0:
                            #         y = cr
                            #         crsxy="[%s,%s]"%(str(x),str(y))
                            #         pairs.append(crsxy)
                            # headers = {
                            #     "Content-Type": "application/json; charset=UTF-8"
                            # }
                            # formdata = "{\"pairs\":[%s],\"pipe\":{\"transforms\":[{\"transform\":{\"code\":\"BJ54/120\",\"projWkt\":\"+proj\\u003dtmerc +lat_0\\u003d0 +lon_0\\u003d120 +k\\u003d1 +x_0\\u003d500000 +y_0\\u003d0 +ellps\\u003dkrass  +units\\u003dm +no_defs\",\"type\":\"wkt\",\"inverse\":false}},{\"transform\":{\"dx\":-4712.64423228786,\"dy\":190501.847141129,\"rot\":6.269433043807027,\"m\":0.99977072061039,\"type\":\"4\",\"inverse\":false}}],\"inverse\":true}}"%("".join(pairs))
                            # response = requests.post("http://192.168.2.77:7999/api/coordinateConverter", data=formdata,headers=headers)
                            #
                            # print (response.text)

                            # PipeLineParam=json.loads("{\"transforms\":[{\"transform\":{\"dx\":199.497,\"dy\":219.576,\"dz\":151.7,\"rotX\":0.22852078,\"rotY\":5.49545135,\"rotZ\":-5.08238545,\"m\":-8.58397,\"startEllipsoid\":\"wgs84\",\"endEllipsoid\":\"krass\",\"inverse\":false,\"type\":\"7\"}},{\"transform\":{\"code\":\"BJ54/117.8\",\"projWkt\":\"+proj=tmerc +lat_0=0 +lon_0=117.8 +k=1 +x_0=500000 +y_0=0 +ellps=krass  +units=m +no_defs\",\"inverse\":false,\"type\":\"wkt\"}},{\"transform\":{\"dx\":-449999.872,\"dy\":-3400000.142,\"inverse\":false,\"type\":\"offset\"}}],\"inverse\":true}")
                            # LngLatLike=json.loads(array(geom).dumps())
                            # xy84=PipeLineTransform.Transform(,PipeLineParam)
                            if currentType.upper() != src.geomtryType:
                                # we need wrap the wkt string
                                currentRow["geom"] = """%s(%s)""" % (src.geomtryType, currentRow["geom"][geoStrIndex:])
                                # print(currentRow["geom"])
                            temp = """(%s,ST_GeomFromText(\'%s\',%d))""" % (
                                ",".join(item), currentRow["geom"], projectID)
                            debugSql = """ %s (%s,ST_GeomFromText(\'%s\',%d))""" % (
                                insertStr, ",".join(item), currentRow["geom"], projectID)

                            print(insertStr)
                            print(debugSql)
                            insertArr.append(temp)
                            currentRow = srcCursor.fetchone()
                            if counter % 100 == 0 or not currentRow:
                                insertBatchStr = ",".join(insertArr)
                                # print(insertBatchStr)
                                insertBatchStr = insertStr + insertBatchStr
                                # print(insertBatchStr)
                                try:
                                    distCursor.execute(insertBatchStr)
                                except (psycopg2.DatabaseError) as e:
                                    # sys.exit(e)
                                    print(e)
                                distCon.commit()
                                print(counter)
                                item = []
                                insertArr = []

                                if currentRow == None:
                                    # no need ,自动建立的
                                    # distCursor.execute(pkIndx)

                                    distCursor.execute(spatialIndex)
                                    break
                            counter += 1
                    # create index


if __name__ == "__main__":

    with open('c.txt', 'r') as f:
        for line in f.readlines():
            print(line)

            if line.startswith("#"):
                continue
            # a =TableProps("192.168.1.64","1433","sa","abc.123","LM_XS_PipeNet_102",line,"Shape")

            a = TableProps("192.168.1.31", "2433", "sa", "m?~9nfhqZR%TXzY", "LM_GY_ARC_DXT", line, "Shape")
            line="\""+line+"\""
            b = TableProps("10.10.10.70", "5432", "postgres", "gis3857", "GuYuan", line, "geom")
            try:
                batchDTO(a, b)
            finally:
                print("finished!!!!!")

    print("finished!!!!!")
    # break;
    # a =TableProps("192.168.1.53",1433,"sa","sa123","LM_XS_ARC_DXT","SP_VALVE","SHAPE")
    # b = TableProps("192.168.1.107",9216,"postgres","gis3857","xiaoshan","SP_VALVE","geom")
    # batchDTO(a,b)

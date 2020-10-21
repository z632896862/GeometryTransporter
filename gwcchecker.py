import os
import xml.etree.ElementTree as ET
import json



GEOSERVER_DATA_HOME='/data/tomcat/dockerGeoServer2134/geoserver/data';

gwcLayers={
    "layer": {},
    "layerGroup": {}
}

workspaceData={

}
newData=[i for i in range(0,5)]


def handleWorkspace(path,type):
    tempworkspaceName= path.split("/")[-2:-1][0]
    newData[0]=tempworkspaceName
    if workspaceData.get(tempworkspaceName)==None:
       workspaceData[tempworkspaceName]={} 
    if type== "namespace.xml":
        with open(path,'r') as f:
            root=ET.parse(f).getroot()
            workspaceData[tempworkspaceName]["sid"]=root.find("id").text
    elif type =="workspace.xml":
        with open(path,'r') as f:
            root=ET.parse(f).getroot()
            workspaceData[tempworkspaceName]["id"]=root.find("id").text
            workspaceData[tempworkspaceName]["name"]=root.find("name").text


        

def dfs(rootdir,checker):
    list = os.listdir(rootdir)
    for i in range(0,len(list)):
        path=os.path.join(rootdir,list[i])
        checker(path,list[i])

# GEOSERVER_DATA_HOME
def checker(path,fileName):
    if fileName.startswith("LayerInfoImpl"):
        # print(fileName)
        fName=fileName.split(".")[0]
        layerName =ET.parse(path).getroot().find("name").text
        print(fName,"-------------------------------",layerName)
        gwcLayers["layer"][layerName]=fName
        # parts=layerName.split(":")
        # ws =parts[0]
        # layer=parts[1]
        # tpath=os.path.join(GEOSERVER_DATA_HOME,"workspaces",ws,layer)
        # with open(tpath,'r') as f:
        #     root=ET.parse(tpath)
        #     textName=root.getroot().find("name").text
        #     print(textName)

    elif fileName.startswith("LayerGroupInfoImpl"):
        # print(fileName)
        pass
    else:
        print("else"+fileName) 
        # pass        


def featureChecker(path,fileName):
    if os.path.isfile(path):
        with open(path,'r') as f:
            root=ET.parse(path).getroot()
            if fileName=="featuretype.xml":
                id=root.find("id").text
                name=root.find("name").text
                srs=root.find("srs").text
                namespaceid =root.find("namespace").find("id").text
                datastoreId =root.find("store").find("id").text
                sid=workspaceData[newData[0]]["sid"]
                if sid != namespaceid:
                    print("this layer ",name,"has no right namespace",namespaceid,"!=",sid,path)
                sid=workspaceData[newData[0]]["stores"][newData[1]]["id"]
                if sid !=datastoreId:
                    print("this layer",name,"not in its datastore",datastoreId,"!=",sid,path)
                if workspaceData[newData[0]]["stores"][newData[1]].get("layers")==None:
                    workspaceData[newData[0]]["stores"][newData[1]]["layers"]={}
                newData[2]=name    
                workspaceData[newData[0]]["stores"][newData[1]]["layers"][name]={
                    "fid":id,
                    "srs":srs,
                    "name":name
                }        
                # if workspaceData[newData[0]]["stores"].sid !== id:
                #     print("this layer ",name,"has no right namespace",id,"!=",sid,path)    
            elif fileName=="layer.xml":
                lid=root.find("id").text
                name=root.find("name").text
                fid=root.find("resource").find("id").text
                stid=root.find("defaultStyle").find("id").text
                item=workspaceData[newData[0]]["stores"][newData[1]]["layers"][name]
                if fid != item["fid"]:
                    print("layer",name,"has no right featureType id",fid ,"!=",item["fid"],path)
                item["lid"]=lid
                item["stid"]=stid     


    elif os.path.isdir(path):
        print("there has a strange dir",path)    


def handleDataStore(path,fileName):
    sn=newData[0]
    if workspaceData[sn].get("stores") == None:
        workspaceData[sn]["stores"]={}
    d={}
    with open(path,'r') as f:
        root =ET.parse(f).getroot()
        d["name"]=root.find("name").text
        newData[1]=root.find("name").text
        d["id"]=root.find("id").text
    workspaceData[sn]["stores"][newData[1]]=d
def layerChecker(path,fileName):
    if os.path.isfile(path):
        handleDataStore(path,fileName)
    elif os.path.isdir(path):
        dfs(path,featureChecker)    

def storeChecker(path,storeName):
    if os.path.isfile(path):
        handleWorkspace(path,storeName)
    elif os.path.isdir(path) and storeName.startswith("styles"):
        print("styles")
    elif os.path.isdir(path) and storeName.startswith("layergroups"):
        print("layergroups")
    else :
        print("-----store--------------------"+storeName)
        dfs(path,layerChecker)    

def workspacechecker(path,workspaceName):
    if os.path.isfile(path):
        return
    elif os.path.isdir(path):
        print("-----------------",workspaceName,"-----------------------")
        dfs(path,storeChecker)
def getAllData():
    dfs(GEOSERVER_DATA_HOME+"/workspaces",workspacechecker)
    # print(workspaceData)
    with open("data.json","w+") as f:
        json.dump(workspaceData,f)

def getAllLayers(data,sname):
    alllayers={}
    for k,v in data[sname]["stores"].items():
        if v.get("layers")!=None:
            for o,p in v["layers"].items():
                # print(p.get("name"),p.get("lid"),p.get("srs"))
                alllayers[p.get("lid")]=p.get("name")
    # print(alllayers)            
    return alllayers            

def CheckCache():
    # 检查gwc-layers文件和data中是否一杨
    dfs(GEOSERVER_DATA_HOME+"/gwc-layers",checker)
    if not os.path.exists("data.json"):
        getAllData()
    errors=[]
    with open("data.json","r") as d:
        workspaceData=json.loads(d.read())
        # print(workspaceData["test"])
        # alllayers=getAllLayers(workspaceData,"cangshan")
        # for k in alllayers:
        #     print(k,len(k))
        # print(len("LayerInfoImpl--71947f1_166e7f29a39_-7fe7"))    
        # print(alllayers["LayerInfoImpl--71947f1_166e7f29a39_-7fe7".replace("_",":")])
        for k,v in gwcLayers["layer"].items():
            sname,lname=k.split(":")[0],k.split(":")[1]
            # print(sname,lname,v)
            if workspaceData.get(sname)==None:
                errmsg="this is wrong %s %s not found in the workspaces" %(sname,v)
                print(errmsg)
                errors.append(errmsg)

                return
            alllayers=getAllLayers(workspaceData,sname)
            newV =v.replace("_",":")
            if alllayers.get(newV)==lname:
                print("no wrong")
                continue
            else:
                errmsg="%s  %s wrong,assert  %s = %s " % (k,v,alllayers.get(newV),lname)
                print(errmsg)    
                errors.append(errmsg)
    with open("errors.log","w+") as f2:
        print("总数",len(gwcLayers["layer"].items()),"错误",len(errors))
        for e in errors:
            f2.write(e+"\n")    

if __name__=="__main__":
    CheckCache()
   
    

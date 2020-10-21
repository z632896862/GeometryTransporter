#/bin/python37
import  os
import xml.etree.ElementTree as ET


tdict={}
styledict={

}

def dfs(rootdir):
    list = os.listdir(rootdir)
    for i in range(0,len(list)):
        path=os.path.join(rootdir,list[i])
        if os.path.isfile(path):
            # print(path)
            if list[i]=="layer.xml":
                # print(path)
                with open(path,'r') as f:
                    tree=ET.parse(f)
                    id=tree.getroot().find("id").text
                    # print(path.split("/")[-2:-1])
                    tdict[id]=path.split("/")[-2:-1][0]
            elif path.split("/")[-2:-1][0]=="styles" and list[i].endswith("xml"):
                with open(path,'r') as stylexml:
                    root=ET.parse(stylexml).getroot()
                    styleId=root.find("id").text
                    styledict[styleId]=root.find("name").text


        elif os.path.isdir(path):
            dfs(path)


def checklayergroup(rootdir,spaceName):
    tpath=os.path.join(rootdir,"layergroups")
    if not os.path.exists(tpath) :
        return
    for i in os.listdir(tpath):
        print("------------------------------",spaceName,"---process layergroup --------------------------------",i)
        ok=os.path.join(tpath,i)
        with open(ok,'r') as f:
            tree=ET.parse(f)
            publishables=tree.getroot().find("publishables")
            publishlist=publishables.findall("published")
            stylelist=tree.getroot().find("styles").findall("style")
            # print(stylelist)
            for i in range(0,len(publishlist)):
                publishitem,styleItem=publishlist[i],stylelist[i]
                tid=publishitem.find("id").text
                sid=styleItem.find("id").text
                # print(sid,styledict)
                print(tdict[tid],styledict.get(sid))


def allworkspace(rootdir):
    list=os.listdir(rootdir)
    for i in list:
        # print(i)
        workspace=os.path.join(rootdir,i)
        if os.path.isdir(workspace):
            dfs(workspace)
            checklayergroup(workspace,i)
if __name__=="__main__":
    rootdir='/data/tomcat/apache-tomcat-7.0.91/webapps/geoserver/data/workspaces'
    allworkspace(rootdir)
    # dfs(rootdir)
    # checklayergroup(rootdir)


import overpy
import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
from functions import *
from time import gmtime, strptime

def main(nodes,cursor,al,inserted):

    typeList = [] #список типов (тип,группа типов)
    typeChar = {}#тип:id типа
    for i in range(len(nodes)):
        typegroup = getTypegroup(nodes[i])
        t = (nodes[i].tags[typegroup])
        if  (typeList.count(t) == 0):#формирование списка типов
            typeList.append((t,typegroup))
#print(typeList)

    for i in range(len(typeList)):
        typeChar[typeList[i]] = insertType(typeList[i],cursor)
    
    #print('types added')
#print(typeChar)




#=================================================
    tagList = {}
    for i in range(len(nodes)):
    #print(j["features"][a],'features')
        k = (nodes[i].tags)
        typegroup = getTypegroup(nodes[i])
        for j in k:
            d = tagList.get((nodes[i].tags[typegroup],typegroup))
            if d == -1:
                tagList[((nodes[i].tags[typegroup],typegroup))] = []
        #print(i)
            try:
                if  (tagList[((nodes[i].tags[typegroup],typegroup))].count(j) == 0):
                #print(i)
                    tagList[((nodes[i].tags[typegroup],typegroup))] = tagList[((nodes[i].tags[typegroup],typegroup))] + [j]
            except:
                tagList[((nodes[i].tags[typegroup],typegroup))]=[]
#print(tagList)


    for i in tagList:
        tag = tagList[i]
        for j in tag:
           insertTag(i,j,cursor,typeChar)




        
#print('tags added')
#print(typeChar)
    for el in nodes:
        a = '''select * from get_id_osm({},0)'''.format(el.id)
        cursor.execute(a)
        ed = cursor.fetchall()[0][0]
    #print(a,ed)
        al = al + 1
    
        for h in el.tags:
                if typegroup_list.count(h)!=0: #поиск группы типов
                    typegroup = h
                    break
        if ed == -1:
        
            
            
            try:
                name = el.tags['name'].replace("'","''")
                print(name)
                a = '''INSERT INTO public.objects (id_osm,id_object_type,name,latitude,longitude)
            VALUES ({},{},'{}',{},{});
            select currval('objects_id_object_seq');'''.format(el.id,typeChar[(el.tags[typegroup],typegroup)],name,el.lat,el.lon)
            except:
                a = '''INSERT INTO public.objects (id_osm,id_object_type,latitude,longitude)
            VALUES ({},{},{},{});
            select currval('objects_id_object_seq');'''.format(el.id,typeChar[(el.tags[typegroup],typegroup)],el.lat,el.lon)
    #print(a)
            
                cursor.execute(a)
        
                inserted = inserted + 1
                id_object = cursor.fetchall()[0]
                for tag in tagList[(el.tags[typegroup],typegroup)]:
        
        
        
        #cursor.execute('''SELECT id_characteristic
#FROM public."characteristics"
#where characteristic = '{}';'''.format(tag))
        #print(el["properties"]["amenity"])
        #print(typeChar[el["properties"]["amenity"]])
                    a = '''select * from get_id_object_type('{}','{}',0)'''.format(typegroup,el.tags[typegroup])

        #print(a)
                    cursor.execute(a)
                    type_id = cursor.fetchall()[0]
        #print(type_id)
        ##print(typeChar[el["properties"]["amenity"]])
                    a = '''select * from get_id_characteristic('{}',{},0)'''.format(tag,type_id[0])
        #print(a)
                    cursor.execute(a)
                    tag_id = cursor.fetchall()[0]



        #print (tag_id)
                    if tag_id[0] != -1:
                        try:
                            a = '''
INSERT INTO public.object_characteristics (id_object,id_characteristic,value)
	VALUES ({},{},'{}');'''.format(id_object[0],tag_id[0],el.tags[tag])
                #print(a)
                            cursor.execute(a)
                        except:
                        
                #print(a)
                #print(type_id)
                #print (tag_id)
                            adseecv = 1
            
                    d=1
                    print('Ошибка добавления объекта')
        else:
            a=1
        #print(1)
    return(al,inserted)
"""
nwr[amenity](around:1000,59.9454597,30.3271546);
out;
"""
rq1 = """
nwr[amenity](around:3000,51.766654, 55.102026);
out;
"""
rq2 = '''(nwr[amenity](around:300,51.766654, 55.102026);
 nwr[leisure](around:300,51.766654, 55.102026););
out;'''
rq3 = """[out:json];
node[leisure](around:700,, 55.102026);
/*added by auto repair*/
(._;>;);
/*end of auto repair*/
out;"""

al = 0
inserted=0
LATc = 20
LONc=30
LAT = 51.691181
LON= 54.951078
start_time = time.time()
connection = psycopg2.connect(
user="postgres",
password="postgres",
host="192.168.0.105",
port="5432",
database = "geojson")
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()
hj = (al,inserted)
for i in range(LATc):
    for j in range(LONc):
        rq3 = """[out:json];
    node({},{},{},{})(if:count_tags() > 0);
    /*added by auto repair*/
    (._;>;);
    /*end of auto repair*/
    out;""".format(LAT,LON,LAT+0.01,LON+0.01)
        
        
        for k in range(1,21):
            try:
                api = overpy.Overpass()
                r = api.query(rq2)
                
                print("Запрос выполнен",LAT,LON)
                break
            except:
                print('Попытка запроса ',k)
        nodes = r.nodes
        hj= main(nodes,cursor,hj[0],hj[1])
        print(hj)
        LON = LON+0.01
    LAT = LAT + 0.01
        











endtime = (time.time() - start_time)
print('All =',al,'inserted =',inserted,'time =',"{}:{}:{:.2f}".format(int(endtime//3600),int(endtime % 3600 // 60),endtime % 60))




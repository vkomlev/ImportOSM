import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


typegroup_list=['amenity','leisure','nature']
def getTypegroup(el):
    for i in el.tags:
        if typegroup_list.count(i)!=0: #поиск группы типов
            typegroup = i
            break
    return typegroup

def insertType(typel,cursor):
    req1 = '''select * from get_id_object_type('{}','{}',0)'''.format(typel[1],typel[0])#поиск типа в БД
    #print(h)
    #print(a) 
    cursor.execute(req1)
    r = cursor.fetchall()[0][0]
    if r == -1: #если не найден,добавить тип
        req2 = '''
        INSERT INTO public."object_types" (object_type,object_typegroup)
        VALUES ('{}','{}');
        select currval('object_types_id_object_type_seq')'''.format(typel[0],typel[1])
        cursor.execute(req2)
        return cursor.fetchall()[0][0]#наполнение typechar 
    else:
        return r
def insertTag(i,j,cursor,typeChar):
    h = '''select * from get_id_characteristic('{}',{},0)'''.format(j,typeChar[i])
    
    cursor.execute(h)
    r = cursor.fetchall()[0][0]
    #print(r)
    if r == -1:
        a = '''
        INSERT INTO public."characteristics" (characteristic,id_object_type)
	VALUES ('{}',{});'''.format(j,typeChar[i])
        cursor.execute(a)


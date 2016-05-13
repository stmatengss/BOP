import json
import urllib2
from multiprocessing.pool import ThreadPool

allAttr = 'Id,AA.AuId,AA.AfId,F.FId,C.CId,J.JId,RId'
branch = 'Id,AA.AuId,AA.AfId'

id_id_attr = 'Id,AA.AuId,F.FId,C.CId,J.JId,RId'
auid_auid_attr = 'Id,AA.AuId,AA.AfId,RId'
ref_attr = 'RId'

# create the pool
pool = None


def getJson(evaluate):
    order = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate?' \
            + evaluate + '&subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
    # print order
    try:
        tmp = urllib2.urlopen(order)
        ob = json.loads(tmp.read())
    except Exception as e:
        print e
    return ob


def toExpr(id, attributes):
    return 'expr=Id=' + id + '&attributes=' + attributes


def toRIdExpr(id, attributes):
    return 'expr=RId=' + id + '&count=10000&attributes=' + attributes


def toAuIdExpr(id, attributes):
    return 'expr=Composite(AA.AuId=' + id + ')&count=10000&attributes=' + attributes


def isId(id):
    ob = getJson(toAuIdExpr(id, auid_auid_attr))
    if ob['entities'] != []:
        return False, ob
    else:
        return True, None


def classify(id1, id2):
    global pool
    pool = ThreadPool(10)
    r2 = pool.apply_async(isId, (id1,))
    # st1 = isId(id1)
    st2, json2 = isId(id2)
    st1, json1 = r2.get()
    if st1 and st2:
        return dealIdToId(id1, id2)
    elif st1:
        return dealIdToAuId(id1, id2, json2)
    elif st2:
        return dealAuIdToId(id1, id2, json1)
    else:
        return dealAuIdToAuId(id1, id2, json1, json2)


def IdToAuId(id1, id2, json2):
    Set1 = {}
    Set2 = {}

    json1 = getJson(toExpr(id1, id_id_attr))
    entity2 = json2['entities']
    entity1 = json1['entities'][0]

    if 'AA' in entity1:
        AuId1 = [i['AuId'] for i in entity1['AA'] if 'AuId' in i]
        AfId1 = [i['AfId'] for i in entity1['AA'] if 'AfId' in i]
        Set1["AuId"] = frozenset(AuId1)
        Set1["AfId"] = frozenset(AfId1)
    if 'F' in entity1:
        FId1 = [i['FId'] for i in entity1['F']]
        Set1["FId"] = frozenset(FId1)
    if 'C' in entity1:
        CId1 = entity1['C']['CId']
        Set1["CId"] = CId1
    if 'J' in entity1:
        JId1 = entity1['J']['JId']
        Set1["JId"] = JId1
    if 'RId' in entity1:
        RId1 = [i for i in entity1['RId']]
        Set1["RId"] = frozenset(RId1)

    res = []
    intId = int(id1)
    intAuId = int(id2)
# 1-hop
# id -> auid	
    if intAuId in AuId1:
        res.append([intId, intAuId])
# 2-hop
# id -> id -> auid


# 3-hop
# id -> id -> id ->auid


# id -> fid/cid/jid -> id ->auid
# id -> auid -> id -> auid
# id -> auid -> afid -> auid
    AfId = []
    for i in entity2:
        if 'AA' in i:
            for j in i['AA']:
                if 'AuId' in j and 'AfId' in j:
                    if j['AuId'] == intAuId:
                        AfId.append(j['AfId'])
    AfId = frozenset(AfId)

    for i in entity2:
        tempId = i['Id']
        if 'AA' in i:
            for j in i['AA']:
                if 'AuId' in j:
                    tempAuId = j['AuId']
                    # id -> auid -> id -> auid
                    res.append([intId, tempAuId, tempId, intAuId])
                    if 'AfId' in j and j['AfId'] in Set1['AfId']:
                        # id -> auid -> afid -> auid
                        res.append([intId, tempAuId, j['AfId'], intAuId])
        if 'F' in i:
            for j in entity2['F']:
                # id -> fid -> id ->auid
                res.append([intId, j['AuId'], tempId, intAuId])
        if 'J' in i and i['J']['JId']==Set1('JId'):
            # id -> jid -> id ->auid
            res.append([intId, i['J']['JId'], tempId, intAuId])
        if 'C' in i and i['C']['CId']==Set1('CId'):
            # id -> cid-> id ->auid
            res.append([intId, i['C']['CId'], tempId, intAuId])

    return res


def dealIdToAuId(id1, id2, json2):
    return json.dumps(IdToAuId(id1, id2, json2))


def dealAuIdToId(id1, id2, json2):
    res = IdToAuId(id2, id1, json2)
    for i in range(len(res)):
        res[i].reverse()
    return json.dumps(res)


def dealAuIdToAuId(auid1, auid2, json1, json2):
    # j1 = pool.apply_async(getJson, (toAuIdExpr(auid1, auid_auid_attr),) )
    # json2 = getJson(toAuIdExpr(auid2, auid_auid_attr))
    entities2 = json2['entities']
    # json1 = j1.get()
    entities1 = json1['entities']

    res = []
    intAuid1 = int(auid1)
    intAuid2 = int(auid2)

    # afid1
    afid1 = set()
    # id1 = set()
    id2 = set()
    # auid->id->auid, find set for afid1, id1, id2

    id1_ref_tuples = []

    for entity1 in entities1:

        for i in entity1["AA"]:
            temp = i.get("AuId")
            if intAuid2 == temp:
                res.append([intAuid1, entity1['Id'], intAuid2])
            if intAuid1 == temp and "AfId" in i:
                afid1.add(i["AfId"])
        if "RId" in entity1:
            id1_ref_tuples += [(entity1['Id'], entity1['RId'])]
    # auid->afid->auid
    afid1 = frozenset(afid1)
    for entity2 in entities2:
        id2.add(entity2["Id"])
        for i in entity2["AA"]:
            temp = i.get("AuId")
            if intAuid2 == temp and "AfId" in i and i["AfId"] in afid1:
                print "myboy"
                res.append([intAuid1, i["AfId"], intAuid2])

    id2 = frozenset(id2)
    res_jsons = []
    # auid->id1->id2->auid

    for id1, refs in id1_ref_tuples:

        for inter in id2.intersection(refs):
            print "inter!"
            res.append([intAuid1, id1, inter, intAuid2])

    pool.close()
    pool.join()
    return json.dumps(res)


def dealIdToId(id1, id2):
    # json1 = getJson(toExpr(str(id1), allAttr))

    # get all articles that reference id2
    ref_id2_json_temp = pool.apply_async(getJson, (toRIdExpr(id2, id_id_attr),))
    j1 = pool.apply_async(getJson, (toExpr(id1, id_id_attr),))
    Set1 = {}
    Set2 = {}
    json2 = getJson(toExpr(id2, id_id_attr))
    entity2 = json2['entities'][0]
    json1 = j1.get()
    entity1 = json1['entities'][0]

    if 'AA' in entity1:
        AuId1 = [i['AuId'] for i in entity1['AA']]
        Set1["AuId"] = AuId1
    if 'AA' in entity2:
        AuId2 = [i['AuId'] for i in entity2['AA']]
        Set2["AuId"] = AuId2

    if 'F' in entity1:
        FId1 = [i['FId'] for i in entity1['F']]
        Set1["FId"] = FId1
    if 'F' in entity2:
        FId2 = [i['FId'] for i in entity2['F']]
        Set2["FId"] = FId2

    if 'C' in entity1:
        CId1 = entity1['C']['CId']
        Set1["CId"] = CId1
    if 'C' in entity2:
        CId2 = entity2['C']['CId']
        Set2["CId"] = CId2

    if 'J' in entity1:
        JId1 = entity1['J']['JId']
        Set1["JId"] = JId1

    if 'J' in entity2:
        JId2 = entity2['J']['JId']
        Set2["JId"] = JId2

    # For RId, check one is good enough
    if 'RId' in entity1:
        RId1 = [i for i in entity1['RId']]
        Set1["RId"] = RId1

    res = []
    intId1 = int(id1)
    intId2 = int(id2)

    # matching phase. Only rule1 apply here.
    if intId2 in Set1["RId"]:
        res.append([intId1, intId2])

    id_jsons = []

    # for those "id" entry in one_hop_match1
    if "RId" in Set1:
        for id in RId1:
            # eliminate cycle
            id_jsons.append(pool.apply_async(getJson, (toExpr(str(id), id_id_attr),)))

    # the Json of id2
    id_json_dict = {}

    # get all articles that reference id2
    ref_id2_json = ref_id2_json_temp.get()['entities']

    # Id->Id->Id, Id->Id->Id->Id
    for id_json, id in zip(id_jsons, RId1):
        id_json_dict[id] = id_json.get()['entities'][0]

        temp_entity = id_json_dict[id]
        # add matched pair

        if 'RId' in temp_entity:
            templist1 = frozenset(temp_entity['RId'])
            if intId2 in templist1:
                res.append([intId1, id, intId2])
                # print "New!"
            for entity in ref_id2_json:
                if entity['Id'] in templist1:
                    res.append([intId1, id, entity['Id'], intId2])
                    # print "New!!!!"

    def integrate1():
        # Id->FId->Id
        if "FId" in Set1 and "FId" in Set2:
            # print "Id->FId->Id"
            for fid1 in FId1:
                if fid1 in FId2:
                    res.append([intId1, fid1, intId2])

                    # Id->JId->Id
        if "JId" in Set1 and "JId" in Set2:
            # print "Id->Jid->Id"
            if JId1 == JId2:
                res.append([intId1, JId1, intId2])

        # Id->CId->Id
        if "CId" in Set1 and "CId" in Set2:
            # print "Id->CId->Id"
            if CId1 == CId2:
                res.append([intId1, cid1, intId2])

        # Id -> AuId -> Id
        if "AuId" in Set1 and "AuId" in Set2:
            for auid1 in AuId1:
                if auid1 in AuId2:
                    res.append([intId1, auid1, intId2])

    i1 = pool.apply_async(integrate1)
    '''3-hop'''

    def assembly1():
        for myid2, id2json in id_json_dict.iteritems():
            # Id->Id->FId->Id
            if 'FId' in Set2 and 'F' in id2json:
                templist2 = [i['FId'] for i in id2json['F']]
                for fid2 in FId2:
                    if fid2 in templist2:
                        res.append([intId1, myid2, fid2, intId2])

            # Id->Id->CId->Id
            if 'CId' in Set2 and 'C' in id2json['AA']:
                templist3 = id2json['C']['CId']

                if CId2 == templist3:
                    res.append([intId1, myid2, cid2, intId2])

            # Id->Id->JId->Id
            if 'JId' in Set2 and 'J' in id2json:
                if JId2 == id2json['J']['JId']:
                    res.append([intId1, myid2, JId2, intId2])

            # Id->Id->AuId->Id
            if "AuId" in Set2 and 'AA' in id2json:
                templist4 = frozenset([i['AuId'] for i in id2json['AA']])
                for auid2 in AuId2:
                    # AuId1 = [i['AuId'] for i in entity1['AA']]
                    if auid2 in templist4:
                        res.append([intId1, myid2, auid2, intId2])

    i2 = pool.apply_async(assembly1)

    for entity in ref_id2_json:
        tempid = entity['Id']
        # id->fid->id->id
        if 'FId' in Set1 and 'F' in entity:
            templist5 = frozenset([i['FId'] for i in entity['F']])
            for fid1 in FId1:
                if fid1 in templist5:
                    res.append([intId1, fid1, tempid, intId2])

        # id->cid->id->id
        if 'CId' in Set1 and 'C' in entity:
            temp_cid = entity['C']['CId']
            if CId1 == temp_cid:
                res.append([intId1, temp_cid, tempid, intId2])

        # id->jid->id->id
        if 'JId' in Set1 and 'J' in entity:
            temp_jid = entity['J']['JId']
            if temp_jid == JId1:
                res.append([intId1, temp_jid, tempid, intId2])

        # Id->AuId->Id->id
        if "AuId" in Set1 and 'AA' in entity:
            templist7 = frozenset([i['AuId'] for i in entity['AA']])
            for auid1 in AuId1:
                if auid1 in templist7:
                    res.append([intId1, auid1, tempid, intId2])
    pool.close()
    pool.join()
    return json.dumps(res)

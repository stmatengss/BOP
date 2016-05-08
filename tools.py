
import json
import urllib2

allAttr = 'Id,AA.AuId,AA.AfId,F.FId,C.CId,J.JId'
branch = 'Id,AA.AuId,AA.AfId'

def getJson(evaluate):
        order = 'https://oxfordhk.azure-api.net/academic/v1.0/evaluate?' \
                        + str(evaluate) + '&subscription-key=f7cc29509a8443c5b3a5e56b0e38b5a6'
        print order
        try:
                tmp = urllib2.urlopen(order)
                ob = json.loads(tmp.read())
        except Exception as e:
                print e
        return ob
        
def toExpr(id, attributes):
        return 'expr=Id=' + id + '&count=10000&attributes=' + attributes

def toCompositeExpr(id, attributes):
        return 'expr=Id=' + id + '&count=10000&attributes=' + attributes
        
def idOrAuId(id):
        ob = getJson('expr=Composite(AA.AuId=' + id + ')&attributes=Id')
        if b.has_key('entities'):
                return True
        else:
                return False

def classify(id1, id2):
        st1 = idOrAuId(id1)
        st2 = idOrAuId(id2)
        if st1 and st2:
                return dealIdToId(id1, id2)
        elif st1:
                return dealIdToAuId(id1, id2)
        elif st2:
                return dealAuIdToId(id1, id2)
        else:
                return dealAuIdToAuId(id1, id2)

def dealIdToAuId(id1, id2):
        pass

def dealAuIdToId(id1, id2):
	pass

def dealAuIdToAuId(id1, id2):
	global branch
	json1 = getJson(toCompositeExpr(str(id1), branch))
        json2 = getJson(toCompositeExpr(str(id2), branch))
        entity1 = json1['entities'][0]
        entity2 = json1['entities'][0]
        for i in entity1:
                pass

def dealIdToId(id1, id2):
        global allAttr
        json1 = getJson(toExpr(str(id1), allAttr))
        json2 = getJson(toExpr(str(id2), allAttr))
        Set1 = []
        Set2 = []
        entity1 = json1['entities'][0]
        entity2 = json1['entities'][0]
        if entity1.has_key('AA'):
                AuId1 = [i['AuId'] for i in entity1['AA']]
                Set1.append(AuId1)
        if entity2.has_key('AA'):
                AuId2 = [i['AuId'] for i in entity2['AA']]
                Set2.append(AuId2)
        
        if entity1.has_key('F'):
                FId1 = [i['FId'] for i in entity1['F']]
                Set1.append(FId1)
        if entity2.has_key('F'):
                FId2 = [i['FId'] for i in entity2['F']]
                Set2.append(FId2)
        
        if entity1.has_key('C'):
                CId1 = [i['CId'] for i in entity1['C']]
                Set1.append(CId1)
        if entity2.has_key('C'):
                CId2 = [i['CId'] for i in entity2['C']]
                Set2.append(CId2)
        
        if entity1.has_key('J'):
                JId1 = [entity1['J']['JId']]
                Set1.append(JId1)
        if entity2.has_key('J'):
                JId2 = [entity2['J']['JId']]
                Set2.append(JId2)

        intId1 = int(id1)
        intId2 = int(id2)
        
        intersection = []
        for i,j in zip(Set1,Set2):
		intersection = intersection + i and j
	res = [[intId1,intId2]]
	for i in intersection:
		res.append([intId1,i,intId2])
	return json.dumps(res)
        
        

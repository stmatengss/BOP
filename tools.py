
import json
import urllib2

allAttr = 'Id,AA.AuId,AA.AfId,F.FId,C.CId,J.JId'

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
        
def idOrAuId(id):
        ob = getJson('expr=Composite(AA.AuId='+id+')&attributes=Id')
        if b['entities']:
                return True
        else:
                return False

def dealIdToAuId(id1, id2):
        pass

def dealAuIdtoId(id1, id2):
	pass

def dealAuIdtoAuId(id1, id2):
	pass

def dealIdToId(id1, id2):
        global allAttr
        json1 = getJson(toExpr(str(id1), allAttr))
        json2 = getJson(toExpr(str(id2), allAttr))
        Set1 = []
        Set2 = []
        entity1 = json1['entities']
        entity2 = json1['entities']
        AuId1 = [i['AuId'] for i in entity1['AA'] ]
        AuId2 = [i['AuId'] for i in entity2['AA'] ]
        Set1.append(AuId1)
        Set2.append(AuId2)
        FId1 = [i['Fid'] for i in entity1['FId']]
        FId2 = [i['Fid'] for i in entity2['FId']]
        Set1.append(FId1)
        Set2.append(FId2)
        CId1 = [i['Cid'] for i in entity1['CId']]
        CId2 = [i['Cid'] for i in entity2['CId']]
        Set1.append(CId1)
        Set2.append(CId2)
        JId1 = [i['Jid'] for i in entity1['JId']]
        JId2 = [i['Jid'] for i in entity2['JId']]
        Set1.append(JId1)
        Set2.append(JId2)
        intersection = []
        for i,j in zip(Set1,Set2):
		intersection = intersection + i and j
	res = [[id1,id2]]
	for i in intersection:
		res.append([id1,i,id2])
	return json.dumps(res)
        
        

import inspect
import json

from py2neo import Graph, walk
from ER.Relationship import Relationship
from ER.Entity import Entity
from ER.Patient import Patient
from config.config import Neo4jConfig
from nlp.ERextractor import ER_extractor

graph = Graph(Neo4jConfig.bolt, auth=(Neo4jConfig.username, Neo4jConfig.password))


def mergeP(p):
    match = graph.run("match (p :" + p.label + " {name:$name}) return p ", name=p.name).data()
    if match:
        return match[0]["p"]
    create = graph.run("merge (p:" + p.label + " { name:$name }) "
                                               "set p.age=$age,"
                                               "p.gender=$gender,"
                                               "p.country=$country,"
                                               "p.home_town=$home_town "
                                               "return p",
                       name=p.name,
                       age=p.age,
                       gender=p.gender,
                       country=p.country,
                       home_town=p.home_town).data()
    return create[0]["p"]


def mergeEntity(e):
    merge = graph.run("merge (e :"
                      + e.label +
                      " {name: $name}) return e ", name=e.name).data()

    return merge[0]["e"]


def mergeER(p, r, e):
    mergeP(p),
    mergeEntity(e)

    merge = graph.run("match (p:" + p.label + " {name: $p_name }),(e:"
                      + e.label +
                      " {name:$e_name})"
                      " MERGE (p)-[r:" + r.label + " { time:$r_time, link:$r_link }]->(e) return p,r,e",
                      p_name=p.name,
                      e_name=e.name,

                      r_time=r.time,
                      r_link=r.link).data()
    return merge


def update_graph(doc, link):
    BN_list, triplets = ER_extractor(doc)
    for bn in BN_list:
        p = Patient()
        p.name = bn[0]
        p.age = bn[1]
        p.gender = bn[2]
        p.home_town = bn[3]
        p.country = bn[4]
        mergeP(p)

    for triplet in triplets:
        p = Patient()
        e = Entity()
        r = Relationship()

        p.name = triplet[0][0]
        r.label = triplet[1][0].replace(" ", "_")
        r.link = link
        if triplet[3] != None:
            r.time = triplet[3][0]
        e.name = triplet[2][0]
        e.label = triplet[2][1]
        mergeEntity(e)
        mergeER(p, r, e)


def matchPRE(p, r, e):
    match = graph.run("match (p:" + p.label + " {name: $p_name })-"
                                              "[r:" + r.label + " ]->(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,r,e",
                      p_name=p.name,
                      e_name=e.name).data()
    return match


def matchPR(p, r):
    match = graph.run("match (p:" + p.label + " {name: $p_name })-"
                                              "[r:" + r.label + " ]->(e)"
                                                                " return p,r,e",
                      p_name=p.name).data()
    return match


# return properties of patient
def matchP(p):
    match = graph.run("match (p:BN {name: $p_name }) return p",
                      p_name=p.name,
                      ).evaluate()
    if match is None:
        return None
    return dict(match)


# p = Patient()
# p.name= 'BN248'
# print( matchP(p)['name'])

def matchRE(r, e):
    match = graph.run("match (p)-"
                      "[r:" + r.label + " ]->(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,r,e",
                      e_name=e.name).data()
    return match


def matchPE(p, e):
    match = graph.run("match (p:" + p.label + " {name: $p_name })"
                                              ",(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,e",
                      p_name=p.name,
                      e_name=e.name,
                      ).data()
    return match


def matchAll_d3format():
    match = graph.run("MATCH (n)-[r]-(m) RETURN n,r, m").to_subgraph()
    nodes = []
    relastionships = []
    for node in match.nodes:
        node_fm = {
            "id": hash(node),
            "labels": [str(node.labels)[1:]],
            "properties": dict(node)
        }
        nodes.append(node_fm)
    for relationship in match.relationships:
        # print(dict(relationship))
        s, r, e = walk(relationship)

        relationship_fm = {
            "id": hash(relationship),
            "type": type(relationship).__name__,
            "startNode": hash(s),
            "endNode": hash(e),
            "properties": dict(relationship),
            "source": hash(s),
            "target": hash(e),
            "linknum": 1
        }
        relastionships.append(relationship_fm)
    d3format = {
        "nodes": nodes,
        "relationships": relastionships
    }
    return d3format


def match_neo4jformat(query="MATCH (n:BN)-[r]-(m) RETURN n,r, m"):
    match = graph.run(query).to_subgraph()
    nodes = []
    relastionships = []
    for node in match.nodes:
        node_fm = {
            "id": str(hash(node)),
            "labels": [str(node.labels)[1:]],
            "properties": dict(node)
        }
        nodes.append(node_fm)
    for relationship in match.relationships:
        # print(dict(relationship))
        s, r, e = walk(relationship)

        relationship_fm = {
            "id": str(hash(relationship)),
            "type": type(relationship).__name__,
            "startNode": str(hash(s)),
            "endNode": str(hash(e)),
            "properties": dict(relationship)
        }
        relastionships.append(relationship_fm)
    neo4jformat = {
        "results": [
            {
                "columns": ["n", "r", "m"],
                "data": [
                    {
                        "graph": {
                            "nodes": nodes,
                            "relationships": relastionships
                        }
                    }
                ]
            }
        ],
        "errors": []
    }
    return neo4jformat


def involve_info(ps, rs, es):
    where = ""
    if ps:
        where = "Where " + generateOrName(ps, "ps")
        if es:
            where += " or " + generateOrName(es, "es")
    else:
        if es:
            where = "Where " + generateOrName(es, "es")
        else:
            return None
    cypher = "MATCH (ps)-[rs]-(es) " \
             + where + \
             " RETURN ps,rs, es"

    return match_neo4jformat(cypher)


def generateOrName(es, name_e):
    s = ''
    if not es:
        return None
    name = name_e + ".name = '"
    for i in es:
        s += name + i.name + "' or "

    return "(" + s[:-3] + ") "


# ex:
# p1 = Patient()
# p1.name="thanh"
# p2 = Patient()
# p2.name ="fsdaf"
#
print(generateOrName([], "p"))

doc = """THÔNG BÁO VỀ CA BỆNH 223-227: BN223: nữ, 29 tuổi, địa chỉ: Hải Hậu, Nam Định, 
chăm sóc người thân tại Khoa Phục hồi chức năng, Bệnh viện Bạch Mai từ 11/3. 
Từ 11/3-24/3 bệnh nhân thường xuyên đi ăn uống và mua đồ tạp hoá ở căng tin, 
có tiếp xúc với đội cung cấp nước sôi của Công ty Trường Sinh; BN224: nam, 39 tuổi, 
quốc tịch Brazil, có địa chỉ tại phường Thảo Điền, Q.2, TP Hồ Chí Minh. Bệnh nhân có thời 
gian sống cùng phòng với BN158 tại chung cư Masteri, không có triệu chứng lâm sàng; BN225: 
nam, 35 tuổi, quê An Đông, An Dương, Hải Phòng, làm việc ở Matxcova Nga 10 năm nay, về nước trên 
chuyến bay SU290 ghế 50D ngày 25/3/2020 và được cách ly tại Đại học FPT- Hòa Lạc, Thạch Thất; 
BN226: nam, 22 tuổi, về nước cùng chuyến bay với BN 212 ngày 27/3 và được cách ly tại Trường Văn 
hóa Nghệ thuật Vĩnh Phúc; BN227: nam, 31 tuổi, là con của BN209, có tiếp xúc gần tại gia đình trong 
khoảng thời gian từ 16-25/3."""

# update_graph(doc,"http://google.com")
# p = Patient()
# p.name = "BN221"
# e = Entity()
# e.label = "Location"
# e.name = "HCM"
# r = Relationship()
# r.name = "đến"

# print(matchPR(p, r)[1])
# data = matchAll_neo4jformat()
# print(data)
# print(json.dumps(data))

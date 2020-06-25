import json

from py2neo import Graph, walk
from ER.Relationship import Relationship
from ER.Entity import Entity
from ER.Patient import Patient
from config.config import Neo4jConfig

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
                      " MERGE (p)-[r:" + r.label + " {name:$r_name}]->(e) return p,r,e",
                      p_name=p.name,
                      e_name=e.name,
                      r_name=r.name).data()
    return merge


def matchPRE(p, r, e):
    match = graph.run("match (p:" + p.label + " {name: $p_name })-"
                                              "[r:" + r.label + " {name:$r_name}]->(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,r,e",
                      p_name=p.name,
                      e_name=e.name,
                      r_name=r.name).data()
    return match


def matchPR(p, r):
    match = graph.run("match (p:" + p.label + " {name: $p_name })-"
                                              "[r:" + r.label + " {name:$r_name}]->(e)"
                                                                " return p,r,e",
                      p_name=p.name,
                      r_name=r.name).data()
    return match


def matchRE(r, e):
    match = graph.run("match (p)-"
                      "[r:" + r.label + " {name:$r_name}]->(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,r,e",
                      e_name=e.name,
                      r_name=r.name).data()
    return match


def matchPE(p, e):
    match = graph.run("match (p:" + p.label + " {name: $p_name })-"
                                              "[r]->(e:"
                      + e.label +
                      " {name:$e_name})"
                      " return p,r,e",
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


def match_neo4jformat(query="MATCH (n)-[r]-(m) RETURN n,r, m"):
    match = graph.run(query).to_subgraph()
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

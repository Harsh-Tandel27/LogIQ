"""Neo4j access: seed the service map and trace root causes (syllabus modules 3 and 6).

    python -m api.graph      # (re)create the service dependency graph
"""

from neo4j import GraphDatabase

from logiq import config, topology

_driver = None


def driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD))
    return _driver


def seed() -> None:
    with driver().session() as session:
        session.run("MATCH (n:Service) DETACH DELETE n")
        session.run("CREATE CONSTRAINT service_name IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE")
        session.run(
            "UNWIND $services AS s CREATE (:Service {name: s.name, kind: s.kind, tier: s.tier})",
            services=topology.services(),
        )
        session.run(
            """
            UNWIND $edges AS e
            MATCH (a:Service {name: e[0]}), (b:Service {name: e[1]})
            CREATE (a)-[:DEPENDS_ON]->(b)
            """,
            edges=topology.load()["depends_on"],
        )


def find_root_cause(service: str, failing: list[str]) -> dict:
    """Walk DEPENDS_ON edges downstream from `service`.

    The root cause is the failing service furthest down the chain whose own
    dependencies are all healthy. If nothing downstream is failing, the service
    itself is the root cause.
    """
    query = """
    MATCH path = (s:Service {name: $service})-[:DEPENDS_ON*0..]->(d:Service)
    WHERE d.name IN $failing
      AND all(n IN nodes(path) WHERE n.name IN $failing)
      AND NOT EXISTS {
        MATCH (d)-[:DEPENDS_ON]->(x:Service) WHERE x.name IN $failing
      }
    RETURN d.name AS root, [n IN nodes(path) | n.name] AS chain
    ORDER BY length(path) DESC
    LIMIT 1
    """
    with driver().session() as session:
        record = session.run(query, service=service, failing=list(set(failing) | {service})).single()
    if record is None:
        return {"root": service, "chain": [service]}
    return {"root": record["root"], "chain": record["chain"]}


def depends_on(service: str, other: str) -> bool:
    """True if `service` reaches `other` through DEPENDS_ON edges."""
    with driver().session() as session:
        record = session.run(
            "MATCH p = (:Service {name: $a})-[:DEPENDS_ON*1..]->(:Service {name: $b}) RETURN count(p) > 0 AS hit",
            a=service, b=other,
        ).single()
    return bool(record and record["hit"])


def full_graph() -> dict:
    with driver().session() as session:
        nodes = session.run("MATCH (s:Service) RETURN s.name AS name, s.kind AS kind, s.tier AS tier ORDER BY s.tier, s.name").data()
        edges = session.run("MATCH (a:Service)-[:DEPENDS_ON]->(b:Service) RETURN a.name AS source, b.name AS target").data()
    return {"nodes": nodes, "edges": edges}


if __name__ == "__main__":
    seed()
    graph = full_graph()
    print(f"Seeded {len(graph['nodes'])} services and {len(graph['edges'])} DEPENDS_ON edges")

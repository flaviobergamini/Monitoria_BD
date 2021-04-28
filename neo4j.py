import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

class Database(object):

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # -------------------- Criando relacionamento entre os nós --------------------
    def create_relationship(self, node1_name, node2_name, type):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_relationship, node1_name, node2_name, type)
            for record in result:
                print("Relacionamento criado entre: {p1}, {p2}".format(
                    p1=record['b'], p2=record['b1']))

    @staticmethod
    def _create_and_return_relationship(tx, node1_name, node2_name, type):

        query = (
            "MATCH(b:Band{name:$node1_name })" 
            "MATCH (b1:Band{name:$node2_name })"
            "CREATE(b)-[:RELACIONAMENTO{tipo:$type}]->(b1)"
            "RETURN b, b1"
        )
        result = tx.run(query, node1_name=node1_name, node2_name=node2_name, type=type)
        try:
            return [{"b": record["b"]["name"], "b1": record["b1"]["name"]}
                    for record in result]

        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    # --------------------------- Criando novo nó --------------------------
    def create_node(self, node_name, year, style):
        with self.driver.session() as session:
            results = session.write_transaction(self._create_and_return_node, node_name, year, style)
            for record in results:
                print("Novo nó criado:", record['b'])

    @staticmethod
    def _create_and_return_node(tx, node_name, year, style):
        query = (
            "CREATE(b:Band{name:$node_name,year:$year,style:$style})"
            "RETURN b"
        )
        result = tx.run(query, node_name=node_name, year=year, style=style)
        try:
            return [{"b": record["b"]["name"]}
                    for record in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    # ------------------------ Buscando dado em um nó -------------------------------
    def find(self, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find, node_name)
            for record in result:
                print(record)

    @staticmethod
    def _find(tx, name):
        data = []
        query = "MATCH (a:Album) WHERE a.name = $name RETURN a.faixas AS faixas"
        result = tx.run(query, name=name)
        for record in result:
            data.append(record["faixas"])
        return data

    # --------------------------- Excluindo um nó -----------------------------------
    def delete_node(self, node_name):
        with self.driver.session() as session:
            results = session.write_transaction(self._delete_node, node_name)
            for record in results:
                print(record)

    @staticmethod
    def _delete_node(tx, name):
        data = []
        query = "MATCH(b:Band{name:$name}) DETACH DELETE b RETURN b AS  excluido"
        result = tx.run(query, name=name)
        for record in result:
            data.append(record["excluido"])
        return data

    # --------------------------- Atualizando um nó -----------------------------------
    def update_node(self, node_name, new_name):
        with self.driver.session() as session:
            results = session.write_transaction(self._update_node, node_name, new_name)
            for record in results:
                print(record)

    @staticmethod
    def _update_node(tx, node_name, new_name):
        data = []
        query = "MATCH(b:Band{name:$node_name}) SET b.name = $new_name"
        result = tx.run(query, node_name=node_name, new_name=new_name)
        for record in result:
            data.append(record["excluido"])
        return data


if __name__ == "__main__":
    url = "bolt://localhost:7687"
    user = "neo4j"
    password = "/MS-DOSV.6.22b"

    db = Database(url, user, password)

    db.create_node("Van Halen", 1972, "Had Rock")
    db.create_node("Kiss", 1973, "Had Rock")
    db.create_relationship("Kiss", "Van Halen", "forte")
    db.find("Black Album")
    input('Pressione Enter para continuar...')
    db.delete_node("Van Halen")
    db.update_node("Iron Maiden", "Dama de Ferro")

import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class Database(object):
    def __init__(self, url, user, password):
        self.driver = GraphDatabase.driver(url, auth=(user, password))

    def close(self):
        self.driver.close()

    # ------------------- Consultando o banco de dados -------------------------
    # Encontrando nó com label e retornando duas propriedades
    def find_node(self, node_label):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_node, node_label)
            for aux in result:
                print('Nome:', aux[0])
                print('Cidade:', aux[1])
                print('------------------------')

    @staticmethod
    def _find_node(tx, node_label):
        data = []
        query = "MATCH(p:"+node_label+") RETURN p.name AS name, p.cidade AS cidade"
        result = tx.run(query)
        for aux in result:
            text = aux['name'], aux['cidade']
            data.append(text)
        return data

    # Encontrando nós por relacionamentos
    def find_sons(self, relationship, node_name):
        with self.driver.session() as session:
            result = session.read_transaction(self._find_sons, node_name, relationship)
            for aux in result:
                print('Nome:', aux[0])
                print('Cidade:', aux[1])
                print('------------------------')

    @staticmethod
    def _find_sons(tx, node_name, relationship):
        data = []
        query = "MATCH(p:Pessoa)-[:"+relationship+"]->(p1:Pessoa{name:$node_name}) RETURN p.name AS name, p.cidade AS cidade"
        result = tx.run(query, node_name=node_name, relationship=relationship)
        for aux in result:
            text = aux['name'], aux['cidade']
            data.append(text)
        return data

    # -----------------------Criando um novo Nó-------------------------
    def create_node(self, node_label1, node_label2, name, sexo, cidade):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_node, node_label1, node_label2, name, sexo, cidade)
            for aux in result:
                print('Novo nó criado', aux['p'])

    @staticmethod
    def _create_node(tx, node_label1, node_label2, name, sexo, cidade):
        query = "CREATE(p:"+node_label1+":"+node_label2+"{name:$name, sexo:$sexo, cidade:$cidade}) RETURN p"
        result = tx.run(query, name=name, sexo=sexo, cidade=cidade)
        try:
            return [{'p': aux['p']['name']} for aux in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}").format(query=query, exception=exception)

    # --------------------------- Criando Relacionamento entre Nós --------------------------
    def create_relationship (self,node_label1, node1_name, r_label, node_label2, node2_name):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_relationship,node_label1, node1_name, r_label, node_label2, node2_name)
            for aux in result:
                print('Relacionamento criado entre {b1} e {b2}'.format(b1=aux['b'], b2=aux['b1']))

    @staticmethod
    def _create_relationship(tx, node_label1, node1_name, r_label, node_label2, node2_name):
        query = ("MATCH(p1:"+node_label1+"{name:$node1_name}),(p2:"+node_label2+"{name:$node2_name})" 
        "CREATE (p1)-[:"+r_label+"]->(p2) RETURN p1, p2")
        result = tx.run(query, node1_name=node1_name, node2_name=node2_name)
        try:
            return [{'b': aux['p1']['name'], 'b1': aux['p2']['name']} for aux in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}").format(query=query, exception=exception)

    # --------------------------- Criando Relacionamento entre Nós com propriedades --------------------------
    def create_relationship_p(self, node_label1, node1_name, r_label, type, node_label2, node2_name):
        with self.driver.session() as session:
            result = session.write_transaction(self._create_relationship_p, node_label1, node1_name, r_label,
                                               node_label2, node2_name, type)
            for aux in result:
                print('Relacionamento criado entre {b1} e {b2}'.format(b1=aux['b'], b2=aux['b1']))

    @staticmethod
    def _create_relationship_p(tx, node_label1, node1_name, r_label, node_label2, node2_name, type):
        query = ("MATCH(p1:" + node_label1 + "{name:$node1_name}),(p2:" + node_label2 + "{name:$node2_name})"
                "CREATE (p1)-[:" + r_label + "{desde:$type}]->(p2) RETURN p1, p2")
        result = tx.run(query, node1_name=node1_name, node2_name=node2_name, type=type)
        try:
            return [{'b': aux['p1']['name'], 'b1': aux['p2']['name']} for aux in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}").format(query=query, exception=exception)

    # --------------------------- Atualizando um nó -----------------------------------
    def update_node(self, node_label, node_name, new_city):
        with self.driver.session() as session:
            results = session.write_transaction(self._update_node, node_label, node_name, new_city)
            for record in results:
                print(record)

    @staticmethod
    def _update_node(tx,node_label, node_name, new_city):
        data = []
        query = "MATCH(b:"+node_label+"{name:$node_name}) SET b.cidade = $new_city RETURN b AS atualizado"
        result = tx.run(query, node_name=node_name, new_city=new_city)
        for record in result:
            data.append(record["atualizado"])
        return data

    # --------------------------- Excluindo um nó -----------------------------------
    def delete_node(self,node_label, node_name):
        with self.driver.session() as session:
            results = session.write_transaction(self._delete_node,node_label, node_name)
            for record in results:
                print(record)

    @staticmethod
    def _delete_node(tx,node_label, name):
        data = []
        query = "MATCH(b:"+node_label+"{name:$name}) DETACH DELETE b RETURN b AS  excluido"
        result = tx.run(query, name=name)
        for record in result:
            data.append(record["excluido"])
        return data

    # --------------------------- Excluindo um relacionamento -----------------------------------
    def delete_relationship(self, node_label, node_name, r_label):
        with self.driver.session() as session:
            results = session.write_transaction(self._delete_relationship, node_label, node_name, r_label)
            for record in results:
                print(record)

    @staticmethod
    def _delete_relationship(tx, node_label, name, r_label):
        data = []
        query = "MATCH(n:"+node_label+"{name:$name})-[r:"+r_label+"]->() DELETE r RETURN r AS excluido"
        result = tx.run(query, name=name)
        for record in result:
            data.append(record["excluido"])
        return data
if __name__ == "__main__":
    url = 'bolt://localhost:7687'
    user = 'neo4j'
    password = '/MS-DOSV.6.22b'

    db = Database(url, user, password)

    db.create_node('Estudante','Pessoa', 'Flavio', 'M', 'Santa Rita do Sapucaí-MG')
    db.create_node('Operario', 'Pessoa', 'Fernando', 'M', 'Mococa-SP')
    db.create_node('Analista', 'Pessoa', 'Vania', 'F', 'Mococa-SP')
    db.create_node('Dog', 'Animal', 'Princesa', 'F', 'Mococa-SP')

    db.create_relationship('Pessoa', 'Flavio', 'FILHO_DE', 'Pessoa', 'Vania')
    db.create_relationship('Pessoa', 'Flavio', 'FILHO_DE', 'Pessoa', 'Fernando')
    db.create_relationship('Pessoa', 'Fernando', 'ESPOSO_DE', 'Pessoa', 'Vania')
    db.create_relationship('Pessoa', 'Fernando', 'DONO_DE', 'Animal', 'Princesa')
    db.create_relationship('Pessoa', 'Vania', 'DONO_DE', 'Animal', 'Princesa')

    db.create_node('Analista', 'Pessoa', 'Cida B', 'F', 'Mococa-SP')
    db.create_node('Aposentado', 'Pessoa', 'Antonio B', 'M', 'Mococa-SP')
    db.create_node('Operario', 'Pessoa', 'Edson B', 'M', 'Mococa-SP')
    db.create_node('DoLar', 'Pessoa', 'Filó', 'F', 'Mococa-SP')

    db.create_relationship('Pessoa', 'Fernando', 'IRMAO_DE', 'Pessoa', 'Cida B')
    db.create_relationship('Pessoa', 'Fernando', 'IRMAO_DE', 'Pessoa', 'Filó')
    db.create_relationship('Pessoa', 'Fernando', 'IRMAO_DE', 'Pessoa', 'Antonio B')
    db.create_relationship('Pessoa', 'Fernando', 'IRMAO_DE', 'Pessoa', 'Edson B')

    db.create_node('Motorista', 'Pessoa', 'Julio', 'M', 'Campinas-SP')
    db.create_node('Operario', 'Pessoa', 'Edson M', 'M', 'Mococa-SP')

    db.create_relationship('Pessoa', 'Vania', 'IRMAO_DE', 'Pessoa', 'Edson M')
    db.create_relationship('Pessoa', 'Vania', 'IRMAO_DE', 'Pessoa', 'Julio')

    print('------------Analistas--------------')
    db.find_node("Analista")
    print('---------Filhos de Vania-----------')
    db.find_sons("FILHO_DE", "Vania")
    print('--------Irmão de Antonio----------')
    db.find_sons("IRMAO_DE", 'Antonio B')
    print('-----------------------------------')

    print('-----------------TESTANDO OUTROS MÉTODOS-----------------------')
    input('Pressione ENTER para continuar...')
    db.delete_node('Pessoa', 'Flavio')
    db.delete_relationship('Pessoa', 'Fernando', 'DONO_DE')
    db.update_node('Pessoa', 'Julio', 'Mococa-SP')

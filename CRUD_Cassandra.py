from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.protocol import ProtocolException

class Database(object):
    def __init__(self, client_id, client_secret, keyspace):
        cloud_config= {
                'secure_connect_bundle': './secure-connect-spacefb.zip'
        }
        auth_provider = PlainTextAuthProvider(client_id, client_secret)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.session.set_keyspace(keyspace)
    
    def insert_star(self, id_estrela, nome_estrela, nome_galaxia, massa, tamanho, luminosidade):
        try:
            query = "INSERT INTO estrela(id_estrela, nome_estrela, nome_galaxia, massa, tamanho, luminosidade)"\
            "VALUES({0}, '{1}', '{2}', {3}, {4}, {5})".format(id_estrela, nome_estrela, nome_galaxia, massa, tamanho, luminosidade)
            self.session.execute(query)
        except ProtocolException as ex:
            print(ex)

    def delete_star(self, id_estrela, nome_estrela, nome_galaxia):
        try:
            query = "DELETE FROM estrela WHERE id_estrela = {0} AND nome_estrela = '{1}'"\
            " AND nome_galaxia = '{2}'".format(id_estrela, nome_estrela, nome_galaxia)
            self.session.execute(query)
        except ProtocolException as ex:
            print(ex)
    
    def update_star(self, id_estrela, nome_estrela, nome_galaxia,massa, tamanho, lumi):
        try:
            query = "UPDATE estrela SET luminosidade = {0} WHERE id_estrela = {1} AND nome_estrela = '{2}'"\
            " AND nome_galaxia = '{3}' AND massa = {4} AND tamanho = {5}".format(lumi, id_estrela, nome_estrela, nome_galaxia, massa, tamanho)
            self.session.execute(query)
        except ProtocolException as ex:
            print(ex)

    def research_star(self):
        try:
            query = "SELECT * FROM estrela"
            row = self.session.execute(query)
            if row:
                for i in row:
                    size = len(i)
                    for y in range(size):
                        print(i[y])
                    print('---------------')
            else:
                print("Está vazio")
        except ProtocolException as ex:
            print(ex)
    
    def create_index(self, name_index, name_attribute):
        query = "CREATE INDEX {0} ON estrela({1})".format(name_index, name_attribute)
        row = self.session.execute(query)

    def drop_index(self, name_index):
        query = "DROP INDEX " + name_index
        row = self.session.execute(query)

    def research(self, name_condition):
        query = "SELECT * FROM estrela WHERE " + name_condition
        row = self.session.execute(query)
        if row:
            for i in row:
                size = len(i)
                for y in range(size):
                    print(i[y])
                print('---------------')
        else:
            print("Está vazio")

if __name__ == "__main__":
    id_client = "oGxPMuygIwMbJvuisECMXPTB" 
    secret_client = "aRZ94f.Sc_1trCtw60A.y0FLSYyFwzt9nuN0mZh-ZgIQU_IwI6NU,RU++r3q.fyjswkcAfTq-tjlh,PiAIZsNZOonGW_5MKXJR_vLOgX+-Cd8fotG2S47K8fLFA,B+eG"
    keyspace = "universo"

    db = Database(id_client, secret_client, keyspace)
    db.insert_star(2, 'Kepler-438', 'Via Lactea', 1.082, 723.84, 0.044)
    db.research_star()
    print('-------------------INDEX--SELECT--------------------------')
    db.drop_index("tamanho")
    db.create_index('tamanho', 'tamanho')
    db.research('tamanho > 7')
    print('-----------------------UPDATE-----------------------------')
    db.update_star(2, 'Kepler-438', 'Via Lactea', 1.082, 723.84, 2)
    db.research('tamanho > 7')
    print('-----------------------DELETE-----------------------------')
    db.delete_star(2, 'Kepler-438', 'Via Lactea')
    db.research_star()
    print('----------------------------------------------------------')

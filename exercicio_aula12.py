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
    
    def research(self):
        try:
            query = "SELECT AVG(luminosidade), SUM(massa) FROM estrela"
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

if __name__ == "__main__":
    id_client = "oGxPMuygIwMbJvuisECMXPTB" 
    secret_client = "aRZ94f.Sc_1trCtw60A.y0FLSYyFwzt9nuN0mZh-ZgIQU_IwI6NU,RU++r3q.fyjswkcAfTq-tjlh,PiAIZsNZOonGW_5MKXJR_vLOgX+-Cd8fotG2S47K8fLFA,B+eG"
    keyspace = "universo"

    db = Database(id_client, secret_client, keyspace)
    db.insert_star(3, 'VY Canis Majoris', 'Via Lactea', 33.81, 987.89, 270000)

    db.research()


# Caso prefiram, pode fazer sem classe também -> UMA FORMA OU OUTRA!!!!
'''
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
        'secure_connect_bundle': './secure-connect-spacefb.zip'
}
auth_provider = PlainTextAuthProvider('oGxPMuygIwMbJvuisECMXPTB', 'aRZ94f.Sc_1trCtw60A.y0FLSYyFwzt9nuN0mZh-ZgIQU_IwI6NU,RU++r3q.fyjswkcAfTq-tjlh,PiAIZsNZOonGW_5MKXJR_vLOgX+-Cd8fotG2S47K8fLFA,B+eG')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('universo')

# inserindo nova coluna
query = "INSERT INTO estrela(id_estrela, nome_estrela, nome_galaxia, massa, tamanho, luminosidade)VALUES(3, 'VY Canis Majoris', 'Via Lactea', 33.81, 987.89, 270000)"
row = session.execute(query)

# buscando media de luminosidade e somatório de massas
row = session.execute("SELECT AVG(luminosidade), SUM(massa) FROM estrela")
if row:
    for i in row:
        size = len(i)
        for y in range(size):
            print(i[y])
        print('---------------')
else:
    print("Está vazio")
'''

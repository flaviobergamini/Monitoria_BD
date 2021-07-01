from pymongo import  MongoClient

def verifica(results):
    if results.acknowledged:
        print('Sucesso :)')
    else:
        print("Falha :(")

if __name__ == '__main__':
    client = MongoClient('mongodb://localhost:27017')
    db =client['concessionaria']
    veiculos = db.veiculos


    # BUSCANCO DOCUMENTOS

    results = veiculos.find({'nRodas':30})
    for aux in results:
        print('Modelo:', aux['modelo'], "---- nRodas:", aux['nRodas'])

    # INSERINDO UM DOCUMENTO
    
    novoDoc = {
        "modelo": "Triciclo",
        "nRodas": 3,
        "carga": False,
        "motor": 150,
        "ano": 1997,
        "fabriga": [{
            "nome": "Oficina do Inatel",
            "produzido": "Brasil",
            "cidade": "Santa Rita do Sabucaí",
            "matriz": "Brasil"
        }]
    }

    results = db.veiculos.insert_one(novoDoc)

    verifica(results)
    

    # ATUALIZANDO DOCUMENTO

    results = veiculos.update_one({'nRodas':3}, {'$set':{'nRodas':30}})

    verifica(results)
    

    # EXCLUINDO UM DOCUMENTO

    results = veiculos.delete_one({'modelo':'Triciclo'})
    verifica(results)
    
    # INSERINDO VÁRIOS DOCUMENTOS

    doc1 = {
        "motor": 250,
        "cilindro": "1 cilindro"
    }

    doc2 = {
        "motor": 1.8,
        "cilindro": "4 cilindros"
    }

    doc3 = {
        "motor": 1550,
        "cilindro": {
            "nome": "8 cilindros"
        }
    }

    results = db.cilindros.insert_many([doc1, doc2, doc3])
    verifica(results)

    cilindro = db.cilindros

    # CONFERINDO DOCUMENTOS INSERIDOS

    results = cilindro.find()
    for aux in results:
        print('Motor:', aux['motor'], '---- Cilindros:', aux['cilindro'])


    # AGREGAÇÃO

    ag1 = {
        "$lookup": {
            "from": "cilindros",
            "localField": "motor",
            "foreignField": "motor",
            "as": "Cilindros"
        }
    }

    ag2 = {
        "$project": {
            "_id": 0,
            "Modelo": "$modelo",
            "n_rodas": "$nRodas",
            "ano": "$ano",
            "motor_nc": {"$arrayElemAt": ["$Cilindros", 0]}
        }
    }

    ag3 = {
        "$project": {
            "_id": 0,
            "Modelo": "$Modelo",
            "n_rodas": "$n_rodas",
            "Ano": "$ano",
            "Motor": "$motor_nc.cilindro"
        }
    }

    results = db.veiculos.aggregate([ag1, ag2, ag3])

    for aux in results:
        print('Modelo:', aux["Modelo"])
        print('Motor:', aux["Motor"])
        print('Numero de Rodas:', aux['n_rodas'])
        print('Ano:', aux['Ano'])
        print('-----------------------------------------')
from pymongo import MongoClient

if __name__== '__main__':
    client = MongoClient('mongodb://localhost:27017')
    db = client['concessionaria']

    '''
    doc1 = {
    "motor":1.8,
    "preco":25000
    }

    doc2 = {
    "motor":250,
    "preco":7000
    }

    doc3 = {
    "motor":1550,
    "preco":485000
    }

    results = db.financeiro.insert_many([doc1, doc2, doc3])

    if results.acknowledged:
        print("Inserido com sucesso!!!")
    else:
        print("Falha ao inserir")
    '''

    ag1 = {
        "$lookup": {
            "from": "financeiro",
            "localField": "motor",
            "foreignField": "motor",
            "as": "Preco"
        }
    }

    ag2 = {
        "$project": {
            "_id": 0,
            "Modelo": "$modelo",
            "Ano": "$ano",
            "preco": {"$arrayElemAt": ["$Preco", 0]}
        }
    }

    ag3 = {
        "$project": {
            "_id": 0,
            "Modelo": "$Modelo",
            "Ano": "$Ano",
            "Preco": "$preco.preco"

        }
    }

    veiculos = db.veiculos
    results = db.veiculos.aggregate([ag1, ag2, ag3])

    for aux in results:
        print("Modelo:",aux["Modelo"])
        print("Ano:", aux["Ano"])
        print("Preco: R$", aux["Preco"])



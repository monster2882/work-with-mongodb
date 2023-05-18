from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

client = MongoClient('mongodb://91.190.239.132:27027/')
db = client['SHAD111_v8']


# Функция выполняет подсчет данных для каждого средства, подсчитывая кол-во поездок, гос.номер, тонну-киллометров и сумму
def check_work():
    autos_collections = db['autos']
    trips_collections = db['trips']

    autos = autos_collections.find()

    for auto in autos:
        auto_id = auto['auto_id']
        number = auto['nomber']
        empty_price = auto['st_pust']
        full_price = auto['st_grug']

        profit = 0.0
        distance = 0.0

        trips = trips_collections.find({'auto_id': auto_id})
        count_trips = trips_collections.count_documents({'auto_id': auto_id})

        for trip in trips:
            empty_count = trip['kol_pust']
            full_count = trip['kol_gruzh']
            weight = trip['weight']
            profit = empty_price * empty_count + full_count * full_price
            distance = weight * (empty_count + full_count)

        print(f'Гос.номер: {number}')
        print(f'Кол-во заказов {count_trips}')
        print(f'Тонно-киллометры: {distance}')
        print(f'Сумма: {profit}')
        print("==============================")


def scope_auto():
    autos_collections = db['autos']
    trips_collections = db['trips']
    clients_collections = db['clients']

    clients = clients_collections.find()

    numbers = []

    for client in clients:
        client_id = client['client_id']
        name = client['name']
        surname = client['surname']
        father_name = client['otchestvo']

        profit = 0.0
        distance = 0.0

        trips = trips_collections.find({'client_id': client_id})

        for trip in trips:
            auto_id = trip['auto_id']
            empty_count = trip['kol_pust']
            full_count = trip['kol_gruzh']
            weight = trip['weight']
            date = trip['date']

            autos = autos_collections.find({'auto_id': auto_id})
            count_trips = trips_collections.count_documents({'auto_id': auto_id})

            for auto in autos:
                auto_id = auto['auto_id']
                number = auto['nomber']
                empty_price = auto['st_pust']
                full_price = auto['st_grug']
                profit = empty_price * empty_count + full_count * full_price
                distance = weight * (empty_count + full_count)


        print(f'Имя: {name}')
        print(f'Фамилия: {surname}')
        print(f'Отчество: {father_name}')
        print(f'Кол-во заказов {count_trips}')
        print(f'Тонно-киллометры: {distance}')
        print(f'Сумма: {profit}')
        print("==============================")


print('Check_work function')
print()
check_work()
print("Scope_auto function")
print()
scope_auto()


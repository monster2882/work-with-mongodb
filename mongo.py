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
            profit += empty_price * empty_count + full_count * full_price
            distance += weight * (empty_count + full_count)

        print(f'Гос.номер: {number}')
        print(f'Кол-во заказов: {count_trips}')
        print(f'Тонно-киллометры: {distance}')
        print(f'Сумма: {profit}')
        print("==============================")

#Функция выводит группировку по фио
def scope_auto():
    autos_collections = db['autos']
    trips_collections = db['trips']
    clients_collections = db['clients']

    clients = clients_collections.find()

    for client in clients:
        client_id = client['client_id']
        name = client['name']
        surname = client['surname']
        father_name = client['otchestvo']

        profit = 0.0
        distance = 0.0

        trips = trips_collections.find({'client_id': client_id})
        number = set()
        for trip in trips:
            auto_id = trip['auto_id']
            empty_count = trip['kol_pust']
            full_count = trip['kol_gruzh']
            weight = trip['weight']
            date = trip['date']

            autos = autos_collections.find({'auto_id': auto_id})
            count_trips = trips_collections.count_documents({'client_id': client_id})
            for auto in autos:
                auto_id = auto['auto_id']
                number.add(auto['nomber'])
                empty_price = auto['st_pust']
                full_price = auto['st_grug']
                profit += empty_price * empty_count + full_count * full_price
                distance += weight * (empty_count + full_count)

        print(f'Имя: {name}')
        print(f'Фамилия: {surname}')
        print(f'Отчество: {father_name}')
        print(f'Кол-во заказов {count_trips}')
        print(f'Тонно-киллометры: {distance}')
        print(f'Номера: {[i for i in number]}')
        print(f'Сумма: {profit}')
        print("==============================")

#Функция выводит группировку по моделям
def scope_of_work():
    autos_collections = db['autos']
    trips_collections = db['trips']
    clients_collections = db['clients']

    autos = autos_collections.find()
    model_totals = {}  # Словарь для хранения сумм по моделям

    for auto in autos:
        auto_id = auto['auto_id']
        model = auto['model']
        empty_price = auto['st_pust']
        full_price = auto['st_grug']
        number = auto['nomber']

        profit = 0.0
        distance = 0.0
        count_trips = 0

        auto_data = autos_collections.find_one({'model': model})
        trips = trips_collections.find({'auto_id': auto_id})
        for trip in trips:
            client_id = trip['client_id']
            count_trips = trips_collections.count_documents({'auto_id': auto_id})
            empty_count = trip['kol_pust']
            full_count = trip['kol_gruzh']
            weight = trip['weight']
            profit += empty_price * empty_count + full_count * full_price
            distance += weight * (empty_count + full_count)

        # Добавляем значения в словарь сумм по моделям
        if model in model_totals:
            model_totals[model]['count_trips'] += count_trips
            model_totals[model]['distance'] += distance
            model_totals[model]['profit'] += profit
        else:
            model_totals[model] = {
                'count_trips': count_trips,
                'distance': distance,
                'profit': profit
            }

    # Выводим результаты суммирования по моделям
    for model, totals in model_totals.items():
        count_trips = totals['count_trips']
        distance = totals['distance']
        profit = totals['profit']

        print(f'Модель: {model}')
        print(f'Кол-во заказов: {count_trips}')
        print(f'Тонно-киллометры: {distance}')
        print(f'Сумма: {profit}')
        print("==============================")



print('Check_work function')
print()
check_work()
print("Scope_auto function")
print()
scope_auto()
print("Scope_of_work")
print()
scope_of_work()


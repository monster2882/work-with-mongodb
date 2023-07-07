from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo import MongoClient
import json

# Подключение к базе данных
client = MongoClient('mongodb://91.190.239.132:27027/')
db = client['SHAD111_v8']


def paste_info_clients(client_id, name, surname, otchestvo, balance):
    collection = db['clients']

    client_data = {
        'client_id': client_id,
        'name': name,
        'surname': surname,
        'otchestvo': otchestvo,

        'balance': balance
    }

    collection.insert_one(client_data)


# Функция добавления информации о водителях в коллекцию 'Drivers'
def paste_info_drivers(auto_id, model, nomber, max_weight, st_pust, st_grug):
    collection = db['autos']

    driver_data = {
        'auto_id': auto_id,
        'model': model,
        'nomber': nomber,
        'max_weight': max_weight,
        'st_pust': st_pust,
        'st_grug': st_grug
    }

    collection.insert_one(driver_data)


# Функция добавления информации о маршрутах в коллекцию 'Route'
def add_trip(id_poezdki, client_id, auto_id, kol_pust, kol_gruzh, date, weight):
    clients_collection = db['clients']
    drivers_collection = db['autos']
    trips_collection = db['trips']

    client_data = clients_collection.find_one({'client_id': client_id})
    driver_data = drivers_collection.find_one({'auto_id': auto_id})
    print(driver_data)
    if client_data and driver_data:
        if driver_data['max_weight'] > weight:
            cash = client_data['balance']
            cost = kol_pust * driver_data['st_pust'] + kol_gruzh * driver_data['st_pust']
            driver = client_data['name'] + ' ' + client_data['surname'] + ' ' + client_data['otchestvo']
            auto = str(driver_data['auto_id']) + ' : ' + str(driver_data['model']) + ' : ' + str(driver_data['nomber'])
            if cash >= cost:
                new_balance = cash - cost
                clients_collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})

                trip_data = {
                    'id_poezdki': id_poezdki,
                    'client_id': client_id,
                    'auto_id': auto_id,
                    'kol_pust': kol_pust,
                    'kol_gruzh': kol_gruzh,
                    'date': date,
                    'weight': weight
                }

                trips_collection.insert_one(trip_data)

                print(f"Добавлена поездка пассажира {driver} на авто {auto}, со стоимостью {cost}, дата поездки {date}")
                print('--------------------------------')
            else:
                print(f"У клиента {driver} недостаточно средств, текущий баланс: {cash}, стоимость поездки: {cost}")
                print()
        else:
            print('Недопустимый вес', driver_data['max_weight'], ' ', weight)
            print()
    else:
        print("Неправильные данные.")
        print()


def add_balance(money, client_id):
    collection = db['clients']

    client_data = collection.find_one({'client_id': client_id})
    if client_data:
        if money <= 99999.99:
            new_balance = client_data['balance'] + money
            collection.update_one({'client_id': client_id}, {'$set': {'balance': new_balance}})

            surname = client_data['name'] + ' ' + client_data['surname'] + ' ' + client_data['otchestvo']
            print(f"Баланс {surname} пополнен на сумму {money} руб.")
            print('--------------------------------')
            print()
        else:
            print("Вы ввели слишком большую сумму!")
            print()
    else:
        print(f"Клиент с ID {client_id} не найден.")
        print()


def initdb():
    with open("C://Users/alex1/Desktop/piton/read.json", encoding="utf8") as f:
        data_js = json.load(f)

        for i in data_js:
            for j in data_js[i]:
                if j['operation'] == 1:
                    slov = j
                    del slov['operation']
                    paste_info_clients(**slov)
                elif j['operation'] == 2:
                    slov = j
                    del slov['operation']
                    paste_info_drivers(**slov)


def makejob():
    with open("C://Users/alex1/Desktop/piton/read.json", encoding="utf8") as f:
        data_js = json.load(f)

        for i in data_js:
            for j in data_js[i]:
                if j['operation'] == 3:
                    slov = j

                    del slov['operation']
                    add_trip(**slov)
                elif j['operation'] == 4:
                    slov = j
                    del slov['operation']
                    add_balance(**slov)


def clear_collections():
    trips_collection = db['clients']
    trips_collection.delete_many({})

    trips_collection = db['autos']
    trips_collection.delete_many({})

    trips_collection = db['trips']
    trips_collection.delete_many({})

    print("Данные из коллекций успешно очищены.")


print('Введите цифру 1 для очистки базы, и её перезаполнения')
i = int(input())
while i != 1:
    print('Ошибка ввода')
    print()
    print('Введите цифру 1 для очистки базы, и её перезаполнения')
    i = int(input())
else:
    clear_collections()
    print("==============================")
    print()
    initdb()
    print()
    print('Даннык об авто и водителях обновлены')
    print()
    print("==============================")
    print()
    print('Введите цифру 2 для очистки базы, и её перезаполнения')
    i = int(input())
    makejob()
    print()
    print('Даннык о поездках обновлены')
    print()
    print("==============================")

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
        print(f'Кол-во заказов: {count_trips}')
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


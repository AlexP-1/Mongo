import pymongo
from pymongo import MongoClient
from datetime import datetime
import csv
import re
from pprint import pprint


def read_data(csv_file, db):
    with open(csv_file, encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for item in reader:
            item['Цена'] = int(item['Цена'])
            item['Дата'] = datetime.strptime(item['Дата']+'.20', "%d.%m.%y")
            concerts_collection.insert_one(item)


def find_cheapest(db):
    collection = list(concerts_collection.find().sort('Цена', pymongo.ASCENDING))
    return collection


def find_by_name(name, db):
    for letter in name:
        if not letter.isalpha():
            name = name.replace('\\'+letter, letter)
    pattern = re.compile(name, re.IGNORECASE)
    return list(concerts_collection.find({'Исполнитель': pattern}).sort('Цена', pymongo.ASCENDING))


def find_by_date(start, end, db):
    concerts = []
    start = datetime.strptime(start, '%d.%m.%y')
    end = datetime.strptime(end, '%d.%m.%y')
    for item in concerts_collection.find():
        if start <= item['Дата'] <= end:
            concerts.append(item)
    return concerts


if __name__ == '__main__':
    client = MongoClient()
    hw_db = client['homework']
    concerts_collection = hw_db['concerts']

    # read_data('artists.csv', concerts_collection)

    # client.drop_database(hw_db)

    # pprint(find_by_date('01.03.20', '30.05.20', concerts_collection))

    # pprint(find_cheapest(concerts_collection))

    # pprint(find_by_name('to', concerts_collection))

import os
import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Shop, Book, Stock, Sale

passw = os.getenv('postgres_passw')

DSN = f"postgresql://postgres:{passw}@localhost:5432/book_sale"
engine = sqlalchemy.create_engine(DSN)
create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def get_shop(publisher):
    subq = session.query(Book.id).join(Publisher.books).filter(Publisher.name == publisher).subquery()
    q = session.query(Shop).join(Stock.shop).join(subq, Stock.id_book == subq.c.id)
    print(q)
    for s in q.all():
        print(s.id, s.name)


publisher = input("Введите название издательства: ")
get_shop(publisher)

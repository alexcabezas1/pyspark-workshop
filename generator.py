#!/usr/bin/python2
import datetime
import testdata

import csv

from pprint import pprint


def save_csv(filename, header, objects):
    # save products
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for o in objects:
            writer.writerow(o)


sellers = [
    {"id": 1, "name": "Gigabyte"},
    {"id": 2, "name": "Apple"},
    {"id": 3, "name": "Samsung"},
    {"id": 4, "name": "Palm"},
    {"id": 5, "name": "Google"},
    {"id": 6, "name": "BLU"},
    {"id": 7, "name": "NETGEAR"},
    {"id": 8, "name": "Motorola"},
    {"id": 9, "name": "OnePlus"},
    {"id": 10, "name": "Acer"},
    {"id": 11, "name": "HP"},
    {"id": 12, "name": "IBM"},
    {"id": 13, "name": "Phillips"},
    {"id": 14, "name": "BenQ"},
    {"id": 15, "name": "Sony"}
]

sellers = {
    1: "Gigabyte",
    2: "Apple",
    3: "Samsung",
    4: "Palm",
    5: "Google",
    6: "BLU",
    7: "NETGEAR",
    8: "Motorola",
    9: "OnePlus",
    10: "Acer",
    11: "HP",
    12: "IBM",
    13: "Phillips",
    14: "BenQ",
    15: "Sony"
}

category_names = {
    "Baby": 21,
    "Camera": 22,
    "Clothing": 23,
    "Grocery": 24,
    "Consumer Electronics": 25,
    "Musical Instruments": 26,
    "Outdoors": 27,
    "Pets": 28,
    "Shows": 29,
    "Software": 30,
    "Tools": 31,
    "Toys": 32,
    "Video Games": 33,
    "Sports": 34,
    "Books": 35
}

products = {
    "Nintendo Switch Pro Controller": {"category": 33},
    "Oculus Rift S": {"category": 33},
    "Luigi's Mansion 3": {"category": 33},
    "Minecraft": {"category": 33},
    "TP-Link AC1750 Smart WiFi Router": {"category": 25},
    "Acer Aspire 5 Slim Laptop": {"category": 25},
    "Samsung Galaxy Tab A 8.0": {"category": 25},
    "Python Cookbook, Third edition": {"category": 35},
    "Learn Python 3 the Hard Way": {"category": 35},
    "Dell ChromeBook 11": {"category": 25},
    "Martin Smith UK-222-A Soprano Ukulele": {"category": 26},
    "LifeStraw Personal Water Filter": {"category": 27},
    "Apple Watch Series 5": {"category": 25},
    "Canon PowerShot SX420 IS": {"category": 22},
    "Sony DSCW800/B 20.1 MP": {"category": 22},
    "Canon PowerShot SX420": {"category": 22},
}


class Products(testdata.DictFactory):
    product_id = testdata.RandomInteger(60, 100)
    product_name = testdata.RandomSelection(products.keys())
    seller_id = testdata.RandomInteger(1, 15)


# Generar products
products_seller = dict()

# for seller_id lookup
products_map = dict()

products_list = list()
for p in Products().generate(15):
    p["category_id"] = products[p["product_name"]]["category"]

    try:
        products_seller[p["product_name"] + sellers[p["seller_id"]]].append(
            ()
        )
    except KeyError:
        products_seller[p["product_name"] + sellers[p["seller_id"]]] = [
            ()
        ]
    products_map[p["product_id"]] = p
    products_list.append(p)
    print(p)


# export the csv
save_csv("products.csv", p.keys(), products_list)


class ProductPrice(testdata.DictFactory):
    product_price_id = testdata.RandomInteger(100, 115)
    product_id = testdata.RandomInteger(60, 99)
    # seller_id = testdata.RandomInteger(1, 15)
    price = testdata.RandomInteger(10, 1000)

    start_date = testdata.RandomDateFactory(
        datetime.datetime(2018, 10, 1, 1, 1, 0, 0),
        datetime.datetime.now()
    )

    end_date = testdata.RelativeToDatetimeField(
        "start_time",
        datetime.timedelta(days=30)
    )


# Generar ProductPrices
past_dates = dict()
products_price_list = list()
for pp in ProductPrice().generate(15):
    # get the associated product
    p = products_list[pp["product_id"]]
    pp["seller_id"] = p["seller_id"]
    products_list.append(pp)
    print(pp)

# export the csv
save_csv("product_prices.csv", pp.keys(), products_price_list)

# for customer registration date lookup
customers_map = dict()


class Customer(testdata.DictFactory):
    customer_id = testdata.RandomInteger(300, 330)
    first_name = testdata.FakeDataFactory('firstName')
    last_name = testdata.FakeDataFactory('lastName')
    register_on = testdata.RandomDateFactory(
        datetime.datetime(2018, 10, 1, 1, 1, 0, 0),
        datetime.datetime.now()
    )
    customers_map[customer_id] = {"register_on": register_on}


customers_list = list()
for c in Customer().generate(15):
    customers_list.apend(c)
    print(c)

save_csv("customers.csv", c.keys())


class Orders(testdata.DictFactory):
    order_id = testdata.RandomInteger(500, 530)
    customer_id = testdata.RandomInteger(300, 330)
    register_at = testdata.RandomDateFactory(
        customers_list[customer_id]["register_on"],
        datetime.datetime.now()
    )


orders_list = list()
for o in Orders().generate(15):
    orders_list.append(o)
    print(o)

save_csv("orders.csv", o.keys())


class OrderDetails(testdata.DictFactory):
    order_detail_id = testdata.RandomInteger(600, 630)
    order_id = testdata.RandomInteger(500, 530)
    product_id = testdata.RandomInteger(60, 100)


order_details_list = list()
for od in OrderDetails().generate(15):
    order_details_list.append(od)
    print(od)


save_csv("order_details.csv", od.keys())


class ProductReviews(testdata.DictFactory):
    product_review_id = testdata.RandomInteger(700, 730)
    product_id = testdata.RandomInteger(60, 100)
    review_score = testdata.RandomInteger(1, 10)

products_reviews_list = list()
for pr in ProductReviews().generate(15):
    products_reviews_list.append(pr)
    print(pr)

save_csv("product_reviews.csv", pr.keys())

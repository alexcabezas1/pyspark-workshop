#!/usr/bin/python2
import csv
import datetime
import testdata

from random import randrange, choice

from constant import products

# globals

# for seller_id lookup
products_map = dict()

# for customer registration date lookup
customers_map = dict()

# for products prices
products_prices_map = dict()

# for order ids
orders_map = dict()


class Products(testdata.DictFactory):
    product_id = testdata.RandomInteger(60, 100)
    product_name = testdata.RandomSelection(products.keys())
    seller_id = testdata.RandomInteger(1, 15)


class ProductPrice(testdata.DictFactory):
    product_price_id = testdata.RandomInteger(100, 115)
    product_id = testdata.RandomInteger(60, 99)
    # seller_id = testdata.RandomInteger(1, 15)
    price = testdata.RandomInteger(10, 1000)

    start_date = testdata.RandomDateFactory(
        datetime.datetime(2018, 10, 1, 1, 1, 0, 0),
        datetime.datetime.now()
    )


class Customer(testdata.DictFactory):
    customer_id = testdata.RandomInteger(300, 330)
    first_name = testdata.FakeDataFactory('firstName')
    last_name = testdata.FakeDataFactory('lastName')


class Orders(testdata.DictFactory):
    order_id = testdata.RandomInteger(500, 530)
    customer_id = testdata.RandomInteger(300, 330)


class OrderDetails(testdata.DictFactory):
    order_detail_id = testdata.RandomInteger(600, 630)
    # order_id = testdata.RandomInteger(500, 530)
    # product_id = testdata.RandomInteger(60, 100)


class ProductReviews(testdata.DictFactory):
    product_review_id = testdata.RandomInteger(700, 730)
    product_id = testdata.RandomInteger(60, 100)
    review_score = testdata.RandomInteger(1, 10)


def format_date(date):
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)


def save_csv(filename, objects):
    # save products
    with open(filename, 'w') as csvfile:
        # writer = csv.DictWriter(csvfile, fieldnames=header)
        # writer.writeheader()
        writer = csv.writer(csvfile)
        for i, o in enumerate(objects):

            if i == 0:
                writer.writerow(o.keys())

            row = list()
            # format date objects
            for k in o.keys():
                if isinstance(o[k], datetime.date):
                    o[k] = format_date(o[k])
                row.append(o[k])

            writer.writerow(row)


def generate_products(generate_num):

    for p in Products().generate(generate_num):
        p["category_id"] = products[p["product_name"]]["category"]

        products_map[p["product_id"]] = p
        yield p


def generate_product_price(generate_num):
    # Generar ProductPrices
    for pp in ProductPrice().generate(generate_num):
        # get the associated product
        product_id = choice(products_map.keys())
        p = products_map[product_id]

        try:
            products_prices_map[p["product_id"]].append(pp["product_price_id"])
        except KeyError:
            products_prices_map[p["product_id"]] = [pp["product_price_id"]]

        pp["seller_id"] = p["seller_id"]
        yield pp


def generate_customers(generate_num):
    for c in Customer().generate(generate_num):
        c["register_on"] = random_date(
            datetime.datetime(2018, 10, 1, 1, 1, 0, 0),
            datetime.datetime.now()
        )
        customers_map[c["customer_id"]] = {"register_on": c["register_on"]}
        yield c


def generate_orders(generate_num):
    for o in Orders().generate(generate_num):
        customer = choice(customers_map.keys())
        o["register_at"] = random_date(
            customers_map[customer]["register_on"],
            datetime.datetime.now()
        )

        orders_map[o["order_id"]] = o

        yield o


def generate_order_details(generate_num):
    for od in OrderDetails().generate(15):
        od["order_id"] = choice(orders_map.keys())
        od["product_price_id"] = choice(products_prices_map.keys())
        yield od


def generate_products_reviews(generate_num):
    for pr in ProductReviews().generate(15):
        yield pr


if __name__ == "__main__":

    # generate products
    products_list = generate_products(15)
    save_csv("products.csv", products_list)

    # generate products_price
    products_price_list = generate_product_price(15)
    save_csv("product_prices.csv", products_price_list)

    # generate customers
    customers_list = generate_customers(15)
    save_csv("customers.csv", customers_list)

    # genereate orders
    orders_list = generate_orders(15)
    save_csv("orders.csv", orders_list)

    # generate order details
    order_details_list = generate_order_details(15)
    save_csv("order_details.csv", order_details_list)

    # generate product reviews
    product_reviews_list = generate_products_reviews(15)
    save_csv("product_reviews.csv", product_reviews_list)

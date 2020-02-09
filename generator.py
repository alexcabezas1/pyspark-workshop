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
    # customer_id = testdata.RandomInteger(300, 330)


class OrderDetails(testdata.DictFactory):
    order_detail_id = testdata.RandomInteger(600, 630)
    order_id = testdata.RandomInteger(500, 530)
    product_id = testdata.RandomInteger(60, 100)


class ProductReviews(testdata.DictFactory):
    product_review_id = testdata.RandomInteger(700, 730)
    product_id = testdata.RandomInteger(60, 100)
    review_score = testdata.RandomInteger(1, 10)


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)


def save_csv(filename, header, objects):
    # save products
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        for o in objects:
            writer.writerow(o)


def generate_products(generate_num):

    products_list = list()
    for p in Products().generate(generate_num):
        p["category_id"] = products[p["product_name"]]["category"]

        products_map[p["product_id"]] = p
        products_list.append(p)
        return products_list


def generate_product_price(generate_num):
    # Generar ProductPrices
    products_price_list = list()
    for pp in ProductPrice().generate(generate_num):
        # get the associated product
        # p = products_map[pp["product_id"]]
        product_id = choice(products_map.keys())
        p = products_map[product_id]
        pp["seller_id"] = p["seller_id"]
        products_price_list.append(pp)

    return products_price_list


def generate_customers(generate_num):
    customers_list = list()
    for c in Customer().generate(generate_num):
        c["register_on"] = random_date(
            datetime.datetime(2018, 10, 1, 1, 1, 0, 0),
            datetime.datetime.now()
        )
        customers_map[c["customer_id"]] = {"register_on": c["register_on"]}
        customers_list.append(c)

    return customers_list


def generate_orders(generate_num):
    orders_list = list()
    for o in Orders().generate(generate_num):
        customer = choice(customers_map.keys())
        o["register_at"] = random_date(
            customers_map[customer]["register_on"],
            datetime.datetime.now()
        )

        orders_list.append(o)

    return orders_list


def generate_order_details(generate_num):
    order_details_list = list()
    for od in OrderDetails().generate(15):
        order_details_list.append(od)

    return order_details_list


def generate_products_reviews(generate_num):
    products_reviews_list = list()
    for pr in ProductReviews().generate(15):
        products_reviews_list.append(pr)

    return products_reviews_list


if __name__ == "__main__":

    # generate products
    products_list = generate_products(15)
    save_csv("products.csv", products_list[0].keys(), products_list)

    # generate products_price
    products_price_list = generate_product_price(15)
    save_csv("product_prices.csv", products_price_list[0].keys(), products_price_list)

    # generate customers
    customers_list = generate_customers(15)
    save_csv("customers.csv", customers_list[0].keys(), customers_list)

    # genereate orders
    orders_list = generate_orders(15)
    save_csv("orders.csv", orders_list[0].keys(), orders_list)

    # generate order details
    order_details_list = generate_order_details(15)
    save_csv("order_details.csv", order_details_list[0].keys(), order_details_list)

    # generate product reviews
    product_reviews_list = generate_products_reviews(15)
    save_csv("product_reviews.csv", product_reviews_list[0].keys(), product_reviews_list)

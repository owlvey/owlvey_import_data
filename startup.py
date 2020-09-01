from decimal import Decimal
from dateutil.parser import parse
from OwlveyGateway import OwlveyGateway
import pandas as pd


def find_content_type(node):
    if "headers" in node["request"]:
        for item in node["request"]["headers"]:
            if (item["key"] == "Content-Type" or item["key"] == "Accept") and item["value"]:
                return item["value"].replace("application/", "")
        else:
            return "json"
    else:
        return "json"


def build_url(target: str):
    if "?" in target:
        return target[0: target.index("?")]
    else:
        return target


def build_feature(gateway, product, node, feature_root, sources):
    if "_postman_isSubFolder" not in node:
        feature = gateway.create_feature(product["id"], feature_root.lstrip(" "))
        key_method = node["request"]["method"]
        key_url = build_url(node["request"]["url"]["raw"])
        key_media = find_content_type(node)
        key = "{}:{}:{}".format(key_method, key_url, key_media)
        if key not in sources:
            sources[key] = gateway.create_source(product["id"], key)

        gateway.create_sli(feature["id"], sources[key]["id"])
        return
    else:
        for item in node["item"]:
            build_feature(gateway,product, item, feature_root + " " + node["name"], sources)


if __name__ == "__main__":
    data = []
    with open('./data/backup.csv', 'r') as f:
        lines = f.readlines()
        for line in lines:
            items = line.replace('\n', '').split(';')
            try:
                data.append([
                    items[0],
                    parse(items[1]),  # start
                    parse(items[2]),  # end
                    int(Decimal(items[3])),  # total
                    int(Decimal(items[4])),  # availability
                    int(Decimal(items[5])),  # experience
                    Decimal(items[6]),  # latency
                ])
            except:
                pass
                # print(line)
        df = pd.DataFrame(data, columns=['Source', 'Start', 'End', 'Total', 'Availability',
                                         'Experience', 'Latency'])

        gateway = OwlveyGateway("http://localhost:50001",
                                'http://localhost:47002',
                                "CF4A9ED44148438A99919FF285D8B48D",
                                "0da45603-282a-4fa6-a20b-2d4c3f2a2127")

        customers = gateway.get_customers_lite()
        #  filter customer
        customer = customers[0]['name']
        products = gateway.get_products_lite(customers[0]["id"])
        #  filter product
        product = products[0]['name']

        groups = df.groupby('Source')
        for name, group in groups:
            lines = list()
            for row_index, row in group.iterrows():
                lines.append("{};{};{};{};{};{};{}".format(
                    row['Source'],
                    row['Start'],
                    row['End'],
                    row['Total'],
                    row['Availability'],
                    row['Experience'],
                    row['Latency']
                ))

            gateway.create_source_items_batch(customer, product, lines)


















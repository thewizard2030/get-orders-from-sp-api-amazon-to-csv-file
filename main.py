from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from sp_api.api import Orders
from sp_api.util import throttle_retry, load_all_pages
import const
import pandas as pd
import numpy as np
import time


@throttle_retry()
@load_all_pages()
def load_all_orders(**kwargs):
    """
    a generator function to return all pages, obtained by NextToken
    """
    client_config = dict(
        refresh_token=const.REFRESH_TOKEN,
        lwa_app_id=const.LWA_APP_ID,
        lwa_client_secret=const.CLIENT_SECRET,
        aws_secret_key=const.AWS_SECRET_KEY,
        aws_access_key=const.AWS_ACCESS_KEY,
        role_arn=const.ROLE_ARN,
    )
    return Orders(credentials=client_config, marketplace=Marketplaces.UK).get_orders(**kwargs)


AmazonOrderId_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        AmazonOrderId_list.append(order.get("AmazonOrderId"))

AmazonOrderId = pd.Series(AmazonOrderId_list)


time.sleep(10)
PurchaseDate_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        PurchaseDate_list.append(order.get("PurchaseDate"))


PurchaseDate = pd.Series(PurchaseDate_list)


time.sleep(60)
OrderStatus_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        OrderStatus_list.append(order.get("OrderStatus"))
OrderStatus = pd.Series(OrderStatus_list)
time.sleep(60)
PaymentMethod_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        PaymentMethod_list.append(order.get("PaymentMethod"))
PaymentMethod = pd.Series(PaymentMethod_list)
time.sleep(60)
MarketplaceId_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        MarketplaceId_list.append(order.get("MarketplaceId"))
MarketplaceId = pd.Series(MarketplaceId_list)
time.sleep(60)
ShipmentServiceLevelCategory_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        ShipmentServiceLevelCategory_list.append(order.get("ShipmentServiceLevelCategory"))
ShipmentServiceLevelCategory = pd.Series(ShipmentServiceLevelCategory_list)
time.sleep(60)
OrderType_list = []
for page in load_all_orders(LastUpdatedAfter=(datetime.today() - timedelta(days=1)).isoformat()):
    for order in page.payload.get('Orders'):
        OrderType_list.append(order.get("OrderType"))
OrderType = pd.Series(OrderType_list)
orders = {
    'AmazonOrderId': AmazonOrderId,
    'PurchaseDate': PurchaseDate_list,
    'OrderStatus': OrderStatus_list,
    # 'OrderTotal': OrderTotal_list,
    'PaymentMethod': PaymentMethod_list,
    'MarketplaceId': MarketplaceId_list,
    'ShipmentServiceLevelCategory': ShipmentServiceLevelCategory_list,
    'OrderType': OrderType_list

}

orders_data = dict(
    AmazonOrderId=np.array([AmazonOrderId]),
    PurchaseDate=np.array([PurchaseDate_list]),
    OrderStatus=np.array([OrderStatus_list]),
    PaymentMethod=np.array([PaymentMethod]),
    MarketplaceId=np.array([MarketplaceId]),
    ShipmentServiceLevelCategory=np.array([ShipmentServiceLevelCategory]),
    OrderType=np.array([OrderType_list])

)

new_df = pd.DataFrame(dict([(k, pd.Series(v.flatten())) for k, v in orders_data.items()]))

print(new_df)
new_df.to_csv('orders.csv')

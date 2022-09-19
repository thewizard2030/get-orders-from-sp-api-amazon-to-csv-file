from datetime import datetime, timedelta
from sp_api.base import Marketplaces
from sp_api.api import Orders
from sp_api.util import throttle_retry, load_all_pages
import const
import pandas as pd
import time
import statistics as st


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



def samelen(list1, list2, list3, list4, list5, list6, list7):

    # if length are not equal
    if len(list1) != len(list2) or len(list3) != len(list4) or len(list5) != len(list6) or len(list6) != len(list7):
        # Append mean values to the list with smaller length
        if len(list1) > len(list2):
            mean_width = st.mean(list2)
            list2 += (len(list1) - len(list2)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list1) < len(list2):
            mean_length = st.mean(list1)
            list1 += (len(list2) - len(list1)) * [mean_length]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list3) > len(list4):
            mean_width = st.mean(list4)
            list4 += (len(list3) - len(list4)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list3) < len(list4):
            mean_width = st.mean(list3)
            list3 += (len(list4) - len(list3)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list5) > len(list6):
            mean_width = st.mean(list6)
            list6 += (len(list5) - len(list6)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list5) < len(list6):
            mean_width = st.mean(list5)
            list5 += (len(list6) - len(list5)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list6) > len(list7):
            mean_width = st.mean(list7)
            list7 += (len(list6) - len(list7)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
        elif len(list6) < len(list7):
            mean_width = st.mean(list6)
            list6 += (len(list7) - len(list6)) * [mean_width]
            col = {
                'AmazonOrderId': list1,
                'PurchaseDate': list2,
                'OrderStatus': list3,
                # 'OrderTotal': OrderTotal_list,
                'PaymentMethod': list4,
                'MarketplaceId': list5,
                'ShipmentServiceLevelCategory': list6,
                'OrderType': list7
            }
            return col
    else:
        col = {
            'AmazonOrderId': list1,
            'PurchaseDate': list2,
            'OrderStatus': list3,
            # 'OrderTotal': OrderTotal_list,
            'PaymentMethod': list4,
            'MarketplaceId': list5,
            'ShipmentServiceLevelCategory': list6,
            'OrderType': list7
        }
        return col


new_data = samelen(AmazonOrderId, PurchaseDate_list, OrderStatus_list, PaymentMethod_list, MarketplaceId_list, ShipmentServiceLevelCategory_list, OrderType_list)
df = pd.DataFrame(new_data)

df.to_csv('orders.csv')

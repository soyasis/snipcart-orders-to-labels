#%%
import requests
import base64
import json
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

### -----------------------------------------------------------------
### Variables
url = "https://app.snipcart.com/api/orders"
country_codes_csv = "https://datahub.io/core/country-list/r/data.csv"
current_date = datetime.now().date()

## Secrets
snipcart_secret = "./secrets/snipcart_secret.json"
gs_service_account_path = "./secrets/service_account.json"

with open("./secrets/spreadsheet_id.json") as f:
    spreadsheet = json.load(f)
SPREADSHEET_ID = spreadsheet["id"]

# load into a data frame
df = pd.DataFrame.from_records(data["data"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]


### ------------------------------------------
### Functions ###
def encode_64(string):
    """
    Returns an base64 encoded string
    to be used with Snipcart API
  """
    secret_bytes = string.encode("ascii")
    base64_bytes = base64.b64encode(secret_bytes)
    base64_message = base64_bytes.decode("ascii")
    auth = "Basic" + " " + base64_message
    return auth


def extract_orders(item):
    """
    Return a dataframe with web vitals results
    Skips element if json object not as expected
    Params:
      - column: list of URL='s
  """
    results_df = []
    for i in item:
        data = {}
        f = []
        data["invoiceNumber"] = i["invoiceNumber"]
        data["creationDate"] = str(
            datetime.strptime(i["creationDate"], "%Y-%m-%dT%H:%M:%SZ").date()
        )
        data["shippingAddressName"] = i["shippingAddressName"]
        data["itemsInOrder"] = i["numberOfItemsInOrder"]
        for j in i["items"]:  # extract items for multiple orders
            f.append(j["id"])
            data["items"] = ", ".join(f)
        data["shippingMethod"] = i["shippingMethod"]
        data["notes"] = i["notes"]
        data["shippingAddressName"] = i["shippingAddressName"]
        data["shippingAddressCompanyName"] = i["shippingAddressCompanyName"]
        data["shippingAddressAddress1"] = i["shippingAddressAddress1"]
        data["shippingAddressAddress2"] = i["shippingAddressAddress2"]
        data["shippingAddressCity"] = i["shippingAddressCity"]
        data["shippingAddressProvince"] = i["shippingAddressProvince"]
        data["shippingAddressPostalCode"] = i["shippingAddressPostalCode"]
        data["shippingAddressCountry"] = i["shippingAddressCountry"]
        # Convert to DF and append records
        data_df = pd.DataFrame(data, index=[0])
        results_df.append(data_df)
    results_df = pd.concat(results_df)
    return results_df


def clean_orders(df):
    """
    Adds the country map from the github CSV,
    creates a new printLabel column and filters required columns.
    Takes as input on Snipcart Orders DF
  """
    # Part 1: merge with country column
    country_df = pd.read_csv(country_codes_csv)
    country_df = country_df.rename(
        columns={"Name": "country", "Code": "shippingAddressCountry"}, inplace=False
    )
    df = pd.merge(df, country_df, on="shippingAddressCountry")  # join country code DF
    # Part 2: add print label and filter
    df["printLabel"] = (
        df["shippingAddressName"]
        + " "
        + df["shippingAddressCompanyName"].fillna("")
        + "\n"
        + df["shippingAddressAddress1"].fillna("")
        + " "
        + df["shippingAddressAddress2"].fillna("")
        + "\n"
        + df["shippingAddressCity"]
        + "\n"
        + df["shippingAddressPostalCode"]
        + " "
        + df["shippingAddressProvince"].fillna("")
        + "\n"
        + df["country"]
    )
    return df[
        [
            "invoiceNumber",
            "creationDate",
            "shippingAddressName",
            "itemsInOrder",
            "items",
            "shippingMethod",
            "notes",
            "printLabel",
        ]
    ]


def upload_orders(df):
    """
    creates a new Worksheet with today's date
    and uploads the orders as a DF to the defined spreadsheet.
    If worksheet already exists will return an error
  """
    gc = gspread.service_account(filename=gs_service_account_path)
    sh = gc.open_by_key(SPREADSHEET_ID)  # Open Sheet by key
    worksheet = sh.add_worksheet(
        title=str(current_date), rows=df.shape[0], cols="8"
    )  # new worksheet with today's date
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(str(df.shape[0]) + " new orders successfully uploaded.")


### ------------------------------------------
### Main Part ###

# Collect orders from API and convert to DF
with open(snipcart_secret) as f:
    secret = json.load(f)["API_SECRET"]
    auth = encode_64(secret)
    response = requests.get(
        url,
        params={
            "q": "requests+language:python",
            "offset": 0,
            "limit": 100,
            "status": "Processed",
        },
        headers={"Accept": "application/json", "Authorization": auth},
    )
    json_response = response.json()
    orders_df = extract_orders(json_response["items"])  # Parse aPI and create orders DF

# Clean DF
orders_export_df = clean_orders(orders_df)

# Upload to GoogleSheet
upload_orders(orders_export_df)

# Give warning if orders = 20
if orders_export_df.shape[0] >= 20:
    print(
        "-------------- WARNING: -------------- \n \
  Please be aware that a 20-limit might be active and some orders could be missing. \
  Please double check total orders in Snipcart Dashboard."
    )


# %%

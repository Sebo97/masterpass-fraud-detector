import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import requests
import json
import pymysql
from dateutil import parser 
import pandas as pd 
import traceback
from flask import jsonify
import time
import base64
import io
from PIL import Image

#--------------------------Masterpass Logo
image_filename = 'mp.png' # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())
buffer = io.BytesIO()
imgdata = base64.b64decode(encoded_image)
img = Image.open(io.BytesIO(imgdata))
#new_img = img.resize((120, 120))  # Resize
img.save(buffer, format="PNG")
encoded_image = base64.b64encode(buffer.getvalue())
#--------------------------

colors = {
    'background': '#faf4e6'
}

app = dash.Dash()

app.layout = html.Div([
                    #Logo
                    html.Div([
                                html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()),
                                         style={'height':'25%', 'width':'25%'})
                              ],
                    style={'textAlign': 'center'}),
                    html.H1("Fraud Detector",style={'textAlign': 'center'}),
                    
                    dcc.Input(
                        id='input-msisdn',
                        placeholder='MSISDN',
                        type='number',
                        value='',
                        style={'textAlign': 'center', 'width': '25%'}
                    ),
                    html.Br(),
                    dcc.Input(
                        id='input-date',
                        placeholder='Tx Date (YYYY/MM/DD HH:MM)',
                        type='value',
                        value='',
                        style={'textAlign': 'center', 'width': '25%'}),
                    html.Br(),
                    dcc.Input(
                        id='input-merchantId',
                        placeholder='Merchant Name',
                        type='value',
                        value='',
                        style={'textAlign': 'center', 'width': '25%'}
                    ),
                    html.Br(),
                    dcc.Input(
                        id='input-amount',
                        placeholder='Transaction Amount',
                        type='number',
                        value='',
                        style={'textAlign': 'center', 'width': '25%'}
                    ),
                    html.Br(),
                    html.Br(),
                    html.Button('Get Prediction', id='button'),
                    html.P(
                            children="Response: ", 
                            className="header-title",
                            style={'font-weight': 'bold'}
                            ),
                    
                    html.Div(id = 'user-data'),
                    
                    html.Div(
                                [
                                    html.Div(id='result'),
                                    dcc.Loading(
                                        id="loading-1",
                                        children=[html.Div([html.Div(id="loading-output-2")])],
                                        type="circle",
                                    )
                                ]
                            ),
                    html.Br()
                    
                    
                    ],style={'textAlign': 'center', 'backgroundColor':colors['background']})

@app.callback(Output("loading-output-2", "children"), Input("result", "value"))
def input_triggers_nested(value):
    return value

@app.callback(
   Output('result', 'children'),
    
    Input('button', 'n_clicks'),
    
    [State(component_id='input-msisdn', component_property='value'),
           State(component_id='input-date', component_property='value'),
           State(component_id='input-merchantId', component_property='value'),
           State(component_id='input-amount', component_property='value')]
)
def update_result(n_clicks,msisdn, date, merchant, amount):
    
    if not msisdn:
        return "You need to fill in the MSISDN!"
    elif not date:
        return "You need to fill in the date!"
    elif not merchant:
        return "You need to fill in the merchant name!"
    elif not amount:
        return "You need to fill in the amount!"
    else:
        
        try:
            #Reformat Date
            date = str(parser.parse(date))
        
            print('\n New request:',msisdn, date)
            
            #DB Connection
            conn = pymysql.connect(host="10.0.0.36",
                                user="sebastianor",
                                password="Zn8LMK<@rvCrVxeY",
                                db = "pluto")
            cursor = conn.cursor()
                
            
            #Fetch clientId
            cursor.execute(f"SELECT clientId from pluto.client_msisdn where msisdn = {msisdn}")
            clientId = cursor.fetchall()[0][0]
            
            if not clientId:
                conn.close()
                return "Client not found. Make sure you entered the correct MSISDN."
            
            
            #Fetch merchant IDs.
            merchant_list = pd.read_sql(f"SELECT merchantId from pluto.merchant where merchantName = '{merchant}'", conn)
            
            merchant_list = tuple([merchant_list.values[i][0] for i in range(0,len(merchant_list))])
            
            if not merchant_list:
                conn.close()
                return "Merchant not found. Make sure you entered the correct merchant name."
            
            conn.close()
            
            data = {    
                        "clientId": clientId,
                        "amount": amount,
                        "merchant": str(merchant_list),
                        "transaction_date": date
                    }
            
            result = fetch(data)
            
            input_data = json.dumps(result)
                        
            fraud_url = "http://internal-Internallb-qa-1268800673.af-south-1.elb.amazonaws.com:8080/score"
    
            payload = input_data
            
            headers = {
              'Content-Type': 'application/json'
            }
            
            response = requests.request("POST", fraud_url, headers=headers, data=json.dumps(result))
            
            prediction = response.json()
            
            guess = json.dumps(prediction["guess"])
            
            score = json.dumps(prediction["score"])
            
            return "Prediction: " + guess + ", Score: " + score
        
        except Exception as e:
            return str(e)
            
def fetch(data):
    conn = pymysql.connect(host="10.0.0.36",
                        user="sebastianor",
                        password="Zn8LMK<@rvCrVxeY",
                        db = "pluto",
                        local_infile = 1,read_timeout=60)
    cursor = conn.cursor()
    
    #try:
    df = pd.DataFrame(data, index = [0])
    try:
        print("\nQuery submitted:\n")
        
        df["transaction_date"]= parser.parse(df["transaction_date"][0])
        
        #Assign tx_date and clientId variables.
        tx_date, clientId = df.transaction_date[0], df.clientId.values[0]
        
        #Set datetime characteristics
        df["day_of_week"] = df["transaction_date"].dt.dayofweek
        df["day_of_month"]= df["transaction_date"].dt.day
        df["month"]= df["transaction_date"].dt.month
        df["hour"] = df["transaction_date"].dt.hour
    
        
        #Client registration tenure
        cursor.execute('''select c.dateCreated from client c where c.clientId = ''' + str(clientId))        
        registration_tenure = (tx_date - cursor.fetchall()[0][0]).days
        #Add to client_df
        df['registration_tenure'] = registration_tenure
        
        cursor.execute(('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                                    and t.txState IN ("SUCCESS", "END_BANK_NON_00") \
                                                        AND t.txType != "NETWORK_BALANCE" \
                                                        AND t.dateCreated <= "%s"'''%(clientId, tx_date)))
        #Add to client_df
        df['total_tx'] = cursor.fetchall()[0][0]
    
        #Find successful transactions prior to tx_date
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("SUCCESS") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated <= "%s"'''%(clientId, tx_date))
        #Add to client_df
        df['total_successful_transactions_by_client'] = cursor.fetchall()[0][0]
    
        #Find successful transactions in the last 1 hour
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("SUCCESS") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated >= "%s" - interval 1 HOUR \
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_successful_transactions_by_client_past_1_hour'] = cursor.fetchall()[0][0]
    
        #Find successful transactions in the last 12 hours
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("SUCCESS") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated >= "%s" - interval 12 HOUR \
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_successful_transactions_by_client_past_12_hour'] = cursor.fetchall()[0][0]
    
        
        #Find successful transactions in the last 24 hours
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("SUCCESS") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated >= "%s" - interval 24 HOUR \
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_successful_transactions_by_client_past_24_hour'] = cursor.fetchall()[0][0]
    
    #Find failed transactions prior to tx_date
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("END_BANK_NON_00") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated <= "%s"'''%(clientId, tx_date))
        #Add to client_df
        df['total_failed_transactions_by_client'] = cursor.fetchall()[0][0]
        
        #Find failed transactions in the last 1 hour
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("END_BANK_NON_00") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated >= "%s" - interval 1 HOUR \
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_failed_transactions_by_client_past_1_hour'] = cursor.fetchall()[0][0]
    
        #Find failed transactions in the last 12 hours
        cursor.execute('''SELECT COUNT(*) FROM transaction t where t.clientId = %s \
                                    and t.txState IN ("END_BANK_NON_00") \
                                        AND t.txType != "NETWORK_BALANCE" \
                                        AND t.dateCreated >= "%s" - interval 12 HOUR \
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_failed_transactions_by_client_past_12_hour'] = cursor.fetchall()[0][0]
        #print("Added failed tx last 12")
        #Find failed transactions in the last 24 hours
        cursor.execute('''SELECT count(*) FROM transaction t where t.clientId = %s 
                                    and t.txState IN ("END_BANK_NON_00") 
                                        AND t.txType != "NETWORK_BALANCE" 
                                        AND t.dateCreated >= "%s" - interval 24 HOUR 
                                        AND t.dateCreated <= "%s" '''%(clientId, tx_date, tx_date))
        #Add to client_df
        df['total_failed_transactions_by_client_past_24_hour'] = cursor.fetchall()[0][0]
        #print("Added failed tx last 24")
        
        ###--------------------------Cards loaded             
        #Find the total number of cards loaded by the client
        cursor.execute('''select count(*)
                        from pluto.`client` c
                        LEFT JOIN pluto.client_cards cc on cc.clientId=c.clientId
                        LEFT JOIN pluto.client_msisdn cm on cm.clientId=c.clientId
                        LEFT JOIN pluto.client_nix_tokens cnt on cnt.clientId=c.clientId
                        LEFT JOIN pluto.config_bin_table cbt on cbt.bin=cc.binId
                        WHERE c.clientId = %s
                        and cc.dateAdded <= "%s"''' % (clientId,tx_date))   
        #Add to client_df
        df['total_cards_added_by_client'] = cursor.fetchall()[0][0]
        #print("Added cards")
        #Find the total number of cards loaded by the client in the last 24 hours
        cursor.execute('''select COUNT(*)
                        from pluto.`client` c
                        LEFT JOIN pluto.client_cards cc on cc.clientId=c.clientId
                        LEFT JOIN pluto.client_msisdn cm on cm.clientId=c.clientId
                        LEFT JOIN pluto.client_nix_tokens cnt on cnt.clientId=c.clientId
                        LEFT JOIN pluto.config_bin_table cbt on cbt.bin=cc.binId
                        WHERE c.clientId = %s
                        and cc.dateAdded >= "%s" - interval 24 hour and cc.dateAdded <= "%s"''' % (clientId,tx_date, tx_date))   
        #Add to client_df
        df['total_cards_added_by_client_past_24_hour'] = cursor.fetchall()[0][0]
    
        ##---------------------------------Number of transactions through specific merchant
        #merchant ID of current transaction
        merchant = df.merchant.values[0]
        
        
        
        #Total transactions made under this merchant
        total_tx_under_merchant = cursor.execute(f'''SELECT COUNT(*) from pluto.transaction t
                                                where t.dateCreated <= "%s"
                                                and t.txState IN ("SUCCESS","END_BANK_NON_00")
                                                and t.clientId = %s
                                                and t.merchantId IN {merchant}'''%(tx_date, clientId))
        df['total_transactions_by_client_under_merchant'] = cursor.fetchall()[0][0]
        
        
        
        #Total transactions made under this merchant in the last 24 hours
        total_tx_under_merchant_last24hours = cursor.execute(f'''SELECT COUNT(*) from pluto.transaction t
                                                where t.dateCreated <= "%s"
                                                and t.dateCreated >= "%s"- interval 24 hour
                                                and t.txState IN ("SUCCESS","END_BANK_NON_00")
                                                and t.clientId = %s
                                                and t.merchantId IN {merchant}'''%(tx_date, tx_date, clientId))
        df['total_transactions_by_client_under_merchant_past_1_hour'] = cursor.fetchall()[0][0]
        
        #Total transactions made under this merchant in the last 12 hours
        total_tx_under_merchant_last12hours = cursor.execute(f'''SELECT COUNT(*) from pluto.transaction t
                                                where t.dateCreated <= "%s"
                                                and t.dateCreated >= "%s"- interval 12 hour
                                                and t.txState IN ("SUCCESS","END_BANK_NON_00")
                                                and t.clientId = %s
                                                and t.merchantId IN {merchant}'''%(tx_date, tx_date, clientId))
        df['total_transactions_by_client_under_merchant_past_12_hour'] = cursor.fetchall()[0][0]
        
        #Total transactions made under this merchant in the last 1 hour
        total_tx_under_merchant_last1hour = cursor.execute(f'''SELECT COUNT(*) from pluto.transaction t
                                                where t.dateCreated <= "%s"
                                                and t.dateCreated >= "%s"- interval 1 hour
                                                and t.txState IN ("SUCCESS","END_BANK_NON_00")
                                                and t.clientId = %s
                                                and t.merchantId IN {merchant}'''%(tx_date, tx_date, clientId))
        df['total_transactions_by_client_under_merchant_past_24_hour'] = cursor.fetchall()[0][0]
        #Client registration date
        cursor.execute('''SELECT MIN(dateCreated) FROM client c 
                        WHERE c.clientId = "%s"''' % clientId)
                       
        df['client_registration_date'] = str(cursor.fetchall()[0][0])
        
        df["transaction_date"] = str(df["transaction_date"][0])
        
        df = df.drop(["merchant"], axis = 1)
    
        result = df.to_json(orient="records")
        
        parsed = json.loads(result)
        
        client_data = json.loads(json.dumps(parsed))
        
        conn.close()
        
        return client_data[0]
    
    except:
        return jsonify({'trace': traceback.format_exc()})    


if __name__ == '__main__':
        app.run_server(host='0.0.0.0', debug=False, port = 10000)
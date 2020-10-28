import requests
import datetime
import pandas as pd
import dateutil
import os

def get_Bhav_file(HOLIDAY_FILE_PATH,BASE_DIR):
    hol_df = pd.read_csv(HOLIDAY_FILE_PATH)
    holiday_list = [dateutil.parser.parse(dat).date() for dat in hol_df['Holiday'].values]

    "Weekday Return day of the week, where Monday == 0 ... Sunday == 6."
    date = datetime.date.today()
    while date in holiday_list or date.weekday() == 6 or date.weekday() == 5:
        date = date - datetime.timedelta(days=1)

    if date.day<10:
        str_date=''.join(['0',str(date.day), str(date.month), str(date.year)])
    else:
        str_date=''.join([str(date.day), str(date.month), str(date.year)])


    BHAV_FILE_NAME = 'sec_bhavdata_full_{}.csv'.format(str_date)
    if not os.path.exists(BASE_DIR+'/data/'+BHAV_FILE_NAME):
        # downloading bhav file
        response=requests.get("https://archives.nseindia.com/products/content/{}".format(BHAV_FILE_NAME))
        open(BASE_DIR+'/data/'+BHAV_FILE_NAME, 'wb').write(response.content)
    return BASE_DIR+'/data/'+BHAV_FILE_NAME,str_date


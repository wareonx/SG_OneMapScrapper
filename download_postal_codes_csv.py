##### POWERED BY DIGITAL #####
####### VICKNESH.MANO #######

import requests
import time
import datetime
from multiprocessing import Pool
import pandas as pd

start = datetime.datetime.now()

output_path = "./zipcode_master.csv"

min_count = 0
max_count = 1000000
#min_count = 659900
#max_count = 659990

columns = ['SEARCHVAL', 'BLK_NO', 'ROAD_NAME', 'BUILDING', 'ADDRESS', 'POSTAL', 'X', 'Y', 'LATITUDE', 'LONGITUDE', 'LONGTITUDE']

def pcode_to_data(pcode):
    if int(pcode) % 1000 == 0:
        print(pcode)

    page = 1
    results = []

    while True:
        try:
            response = requests.get('http://developers.onemap.sg/commonapi/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum={1}'
                                    .format(pcode, page)).json()
        except requests.exceptions.ConnectionError as e:
            print('Fetching {} failed. Retrying in 2 sec'.format(pcode))
            time.sleep(2)
            continue

        results += response['results']
    
        if response['totalNumPages'] > page:
            page += 1
        else:
            break

    return results


def convert_results(json):
    rows = []
    for record in json:
        rows.append([v for k, v in record.items()])
    return rows


pool = Pool(processes=6)
postal_codes = ['{0:06d}'.format(p) for p in range(min_count, max_count)]
all_buildings = [convert_results(x) for x in pool.map(pcode_to_data, postal_codes) if x]
flat_list = [item for sublist in all_buildings for item in sublist]

business_df = pd.DataFrame(flat_list, columns=columns)
print(business_df.shape)

business_df.to_csv(output_path, encoding="utf-8", index=False)

duration = datetime.datetime.now() - start
print(duration)

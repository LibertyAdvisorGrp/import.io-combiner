import json
import sys
import pymysql
import time
import re
from joblib import Parallel, delayed

# try:
results = []
i = 0
t = 1


with open('FILENAME.json') as inputfile:
    for line in inputfile:
        results.append(line)
try:

    cnx = pymysql.connect(user='USERNAME', password='PASSWORD',
                                          host='HOST',
                                          database='DATABASE')

    cursor = cnx.cursor()

    startTime = time.time()
    for r in results:
            json_data = json.loads(r)
            if 'data' not in json_data['result']['extractorData']:
                print('Data not present')
                continue
            elif len(json_data['result']['extractorData']['data']) == 0:
                print('Data not present')
                continue
            else:
                for t in range(1,len(json_data['result']['extractorData']['data'][0]['group'][0]['Detail Attributes'])+1):
                    if (len(json_data['result']['extractorData']['data'][0]['group'][0]['Detail Attributes'])+1) != (len(json_data['result']['extractorData']['data'][0]['group'][0]['Detail Values'])+1):
                        continue
                    SKU = str(json_data['result']['extractorData']['data'][0]['group'][0]['SKU'][0]['text'])
                    Attribute_Value = re.sub(r'[^\x00-\x7F]+',' ', str(json_data['result']['extractorData']['data'][0]['group'][0]['Detail Attributes'][t-1]['text']) + ' ' + str(json_data['result']['extractorData']['data'][0]['group'][0]['Detail Values'][t-1]['text'].replace("'","")))

                    insert_query=("INSERT INTO `fastenal`.`SKU Attribute Map` (SKU,Attribute_Value) VALUES ('%s', '%s')" % (SKU,Attribute_Value))
                    print(insert_query)
                    print(str(i) + '-' + str(t-1) + ' : ' + SKU + ' '+ Attribute_Value)
                    cursor.execute(insert_query)
                    cnx.commit()

                    t += 1
                    # time.sleep(1)
            i += 1
            t = 1

        # Handle Exceptions
except pymysql.Error as e:
    print("Something went wrong: {}".format(e))

# Close out the cursor and connection
finally:
    cursor.close()
    cnx.close()
    endTime = time.time()
    print(str(endTime - startTime))

import json
import MySQLdb
import sys
import os
import csv

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

try:
  connection = MySQLdb.connect(host=credentials.mysql_host, user=credentials.mysql_user, passwd=credentials.mysql_passwd, db=credentials.mysql_db, unix_socket=credentials.mysql_socket)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()

cursor = connection.cursor()


# Load the output of step #2
with open('parsed_lexicon_ids.json', 'r') as input:
  lexicon = json.loads(input.read())

missing = []

for concept in lexicon:
    for usage in concept['usage']:
        sql = 'SELECT usgs_strat_name_id FROM lookup_usgs_strat_names WHERE '
        where = []
        params = []
        for name in usage['parsed_name']:
            if name['rank'] == '':
                missing.append(name['name'])
            else :
                where.append(name['rank'].lower() + '_name = %s')
                params.append(name['name'])

        if len(where):
            sql += ' AND '.join(where)

            cursor.execute(sql, params)

            if not cursor.rowcount:
                out = []
                for name in usage['parsed_name']:
                    out.append(name['name'] + ' ' + name['rank'])

                print ' of '.join(out)
                missing.append(' of '.join(out))

missing = [x.encode('utf-8') for x in missing]
with open("missing.csv", "wb") as output:
    for item in missing:
        output.write("%s\n" % item)

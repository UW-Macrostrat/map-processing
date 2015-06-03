import json
import MySQLdb
import sys
import psycopg2

try:
  connection = MySQLdb.connect(host="localhost", user="jczaplewski", passwd="pbdb", db="playground")
  pg_conn = psycopg2.connect(dbname="geomacro", user="john", host="localhost", port=5432)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()
 


pg_cur = pg_conn.cursor()

cursor = connection.cursor()

pg_cur.execute(""" 
DROP TABLE IF EXISTS macrostrat.usgs_lexicon_meta;
CREATE TABLE macrostrat.usgs_lexicon_meta (
  usgs_id integer,
  name text,
  geologic_age text,
  usage_notes text,
  other text,
  province text
);

DROP TABLE IF EXISTS macrostrat.usgs_strat_names;
CREATE TABLE macrostrat.usgs_strat_names (
  id serial,
  usgs_id integer,
  strat_name text,
  rank text,
  ref_id integer,
  places text[]
);

DROP TABLE IF EXISTS macrostrat.usgs_strat_tree;
CREATE TABLE macrostrat.usgs_strat_tree (
  id serial,
  this_name integer,
  rel text,
  that_name integer,
  ref_id integer,
  check_me integer
);

""")
pg_conn.commit()


with open('parsed_lexicon_ids.json', 'r') as input:
  lexicon = json.loads(input.read())

# Record our canonnical names
for lex_idx, name in enumerate(lexicon):
  pg_cur.execute(""" 
    INSERT INTO macrostrat.usgs_lexicon_meta (usgs_id, name, geologic_age, usage_notes, other, province) VALUES (%s, %s, %s, %s, %s, %s)
  """, [name['id'], name['name'], ', '.join(name['geologic_age']), name['usage_notes'], name['other'], name['province']])

  #print lex_idx
  for usage_idx, usage in enumerate(name['usage']):
    places = [place['state'] for place in usage['places']]

    if len(usage['parsed_name']) > 0 and usage['parsed_name'][0]['name'].split(' ')[0] == name['name'].split(' ')[0]:
      print name['id'], usage['parsed_name'][0]['name']
      pg_cur.execute("""
        INSERT INTO macrostrat.usgs_strat_names (usgs_id, strat_name, rank, places) VALUES (%s, %s, %s, %s) RETURNING id
      """, [name['id'], usage['parsed_name'][0]['name'], usage['parsed_name'][0]['rank'], places ])

      lexicon[lex_idx]['usage'][usage_idx]['new_id'] = pg_cur.fetchone()[0]
  

pg_conn.commit()
print "--- Done inserting strat names ---"


for lex_idx, name in enumerate(lexicon):
  for usage_idx, usage in enumerate(name['usage']):
    for parsed_idx, parsee in enumerate(usage['parsed_name']):

      places = [place['state'] for place in usage['places']]
      pg_cur.execute("""
        SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name = %s AND rank = %s AND places && %s
      """, [parsee['name'], parsee['rank'], places])

      try :
        new_id = pg_cur.fetchone()[0]
        lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id
      except:
        if len(places) > 0:
          # Relax space a bit
          pg_cur.execute("SELECT id, places FROM macrostrat.usgs_strat_names WHERE strat_name = %s AND rank = %s", [parsee['name'], parsee['rank']])
          possibilities = pg_cur.fetchall()

          for pos in possibilities:
            pg_cur.execute(""" 
              with given AS (SELECT st_union(st_envelope(geom)) as geom from us_states where postal = ANY(%s)),
                   expected AS (SELECT st_buffer(st_envelope(geom), 0.12) AS geom FROM us_states WHERE postal IN (SELECT array_to_string(places, ',') FROM macrostrat.usgs_strat_names WHERE id = %s))

            SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name = %s AND rank = %s AND ST_Intersects((SELECT geom FROM given), (SELECT geom FROM expected))
            """, [[place['state'] for place in usage['places']], pos[0], parsee['name'], parsee['rank'] ])

            try:
              new_id = pg_cur.fetchone()[0]
              lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id
            except:
              continue
        else :
          print "       No places for ", usage['name'], usage['places']
        

        


print "--- Done assiging new strat_name_ids ---"
#print json.dumps(lexicon, indent=2)

# Hierarcy/synonynmy
for name in lexicon:
  # Start with synonyms
  for each in xrange(len(name['usage']) - 1) :
    if 'new_id' in name['usage'][each] and 'new_id' in name['usage'][each + 1]:
      pg_cur.execute(""" 
        INSERT INTO macrostrat.usgs_strat_tree (this_name, rel, that_name) VALUES (%s, %s, %s)
      """, [name['usage'][each]['new_id'], 'synonym', name['usage'][each + 1]['new_id']])

  pg_conn.commit()


print "--- Done adding synonyms ---"


for name in lexicon:
  for usage in name['usage'] :
    for name_idx in xrange(len(usage['parsed_name']) - 1, 0, -1):
      if 'new_id' in usage['parsed_name'][name_idx] and 'new_id' in usage['parsed_name'][name_idx - 1]:
        pg_cur.execute(""" 
          INSERT INTO macrostrat.usgs_strat_tree (this_name, rel, that_name) VALUES (%s, %s, %s)
        """, [usage['parsed_name'][name_idx]['new_id'], 'parent', usage['parsed_name'][name_idx - 1]['new_id'] ])
      else :
        print 'no new id --- ', usage['parsed_name'], [place['state'] for place in usage['places']]

pg_conn.commit()
print "--- Done adding parents ---"


'''
names_to_insert = []
new_names = []
matched = 0
for name in lexicon :
  for usage in name['usage']:
    for each in usage['parsed_name']:
      term = each['name'] + ' ' + each['rank']

      if "strat_name_id" in each:
        matched += 1

      if 'strat_name_id' not in each and term not in new_names:
        new_names.append(term)
        names_to_insert.append(each)


print json.dumps(names_to_insert, indent=2), len(names_to_insert)
'''



'''
for name in lexicon:
  cursor.execute(""" 
    INSERT INTO usgs_lexicon_meta (usgs_id, name, geologic_age, usage_notes, other, province) VALUES (%s, %s, %s, %s, %s, %s)
  """, [name['id'], name['name'], ', '.join(name['geologic_age'], name['usage_notes'], name['other'], name['province'])])
  
  for idx, usage in enumerate(name['usage']) :
    insert_data = {
      'bed_name': None,
      'bed_id': None,
      'mbr_name': None,
      'mbr_id': None,
      'fm_name': None,
      'fm_id': None,
      'gp_name': None,
      'gp_id': None,
      'sgp_name': None,
      'sgp_id': None
    }

    for each in usage['parsed_usage'] :
      if each['rank'] == 'Bed':
        insert_data['bed_name'] = each['name']

        if 'strat_name_id' in each:
          insert_data['bed_id'] = each['strat_name_id']

      elif each['rank'] == 'Mbr':
        insert_data['mbr_name'] = each['name']

        if 'strat_name_id' in each:
          insert_data['mbr_id'] = each['strat_name_id']

      elif each['rank'] == 'Fm':
        insert_data['fm_name'] = each['name']

        if 'strat_name_id' in each:
          insert_data['fm_id'] = each['strat_name_id']

      elif each['rank'] == 'Gp':
        insert_data['gp_name'] = each['name']

        if 'strat_name_id' in each:
          insert_data['gp_id'] = each['strat_name_id']

      elif each['rank'] == 'Sgp':
        insert_data['sgp_name'] = each['name']

        if 'strat_name_id' in each:
          insert_data['sgp_id'] = each['strat_name_id']

      else:
        print 'WTF RANK? ', each['rank']

    cursor.execute(""" 
      INSERT INTO usgs_lexicon_names (lex_id, usgs_id, name, conforms, bed_name, bed_id, mbr_name, mbr_id, fm_name, fm_id, gp_name, gp_id, sgp_name, sgp_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, [idx, name['id'], usage['name'], usage['conforms'], insert_data['bed_name'], insert_data['bed_id'], insert_data['mbr_name'], insert_data['mbr_id'], insert_data['fm_name'], insert_data['fm_id'], insert_data['gp_name'], insert_data['gp_id'], insert_data['sgp_name'], insert_data['sgp_id']])


  for place in usage['places']:
    cursor.execute(""" 
      INSERT INTO usgs_lexicon_name_meta (lex_id, state, in_use) VALUES (%s, %s, %s)
    """, [name['id'], place['state'], place['in_use']])


'''


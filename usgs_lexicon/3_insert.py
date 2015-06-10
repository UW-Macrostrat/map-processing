import json
import MySQLdb
import sys
import psycopg2
import urllib2

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

lith_data = json.loads(urllib2.urlopen('http://localhost:5000/api/v1/defs/lithologies?all').read())

lithologies = ['formation', 'member', 'group', 'supergroup', 'beds', 'bed', 'submember', 'metaquartzite', 'bentonite', 'informal']
upperLiths = []

for lith in lith_data['success']['data']:
  lithologies.append(lith['lith'])
  upperLiths.append(lith['lith'].title())

try:
  connection = MySQLdb.connect(host=credentials.mysql_host, user=credentials.mysql_user, passwd=credentials.mysql_passwd, db=credentials.mysql_db, unix_socket=credentials.mysql_socket, cursorclass=MySQLdb.cursors.DictCursor)
  pg_conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()
 


def removeLithology(name):
  words = name.split(" ")
  for idx, word in enumerate(words):
    if word in lithologies or word in upperLiths:
      words.remove(word)

  return " ".join(words)



def findIDs():
  global orphans

  # Assign new IDs
  for lex_idx, name in enumerate(lexicon):
    for usage_idx, usage in enumerate(name['usage']):
      for parsed_idx, parsee in enumerate(usage['parsed_name']):

        if "new_id" in lexicon[lex_idx]["usage"][usage_idx]["parsed_name"][parsed_idx]:
          continue

        places = [place['state'] for place in usage['places']]
        new_id = findIDSimple(usage, parsee)

        if new_id:
          lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

        else :
          if len(places) > 0:
            new_id = findIDComplex(usage, parsee)

            if new_id :
               lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

            else :
              # remove lexicon...

              new_id = findIDSimpleNoLith(usage, parsee)

              if new_id :
                lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id
              else :
                new_id = findIDComplexNoLith(usage, parsee)

                if new_id:
                  lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id
                else:
                  if len(orphans) < 1 :
                    orphans.append({"name": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"], "rank": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"], "places": places})
                  
                  found = False
                  for idx, orphan in enumerate(orphans):
                    if orphan["name"] == lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"] and orphan["rank"] == lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"]:
                      found = True
                      for place in places:
                        if place not in orphan["places"]:
                          orphans[idx]["places"].append(place)
                  
                  if not found and "," not in lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"]:
                    print "adding orphan"
                    orphans.append({"name": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"], "rank": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"], "places": places})

          else :
             print "       No places for ", usage['name'], usage['places']


def findIDSimple(usage, parsee) :
  places = [place['state'] for place in usage['places']]

  pg_cur.execute("""
    SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name ilike %s AND rank = %s AND places && %s
  """, [parsee['name'], parsee['rank'], places])

  try :
    new_id = pg_cur.fetchone()[0]
    return new_id
  except:
    return False


def findIDSimpleNoLith(usage, parsee) :
  places = [place['state'] for place in usage['places']]

  pg_cur.execute("""
    SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND rank = %s AND places && %s
  """, [removeLithology(parsee["name"]), parsee['rank'], places])

  try :
    new_id = pg_cur.fetchone()[0]
    return new_id
  except:
    return False




def findIDComplex(usage, parsee) :
  pg_cur.execute("SELECT id, places FROM macrostrat.usgs_strat_names WHERE strat_name = %s AND rank = %s", [parsee['name'], parsee['rank']])
  possibilities = pg_cur.fetchall()

  for pos in possibilities:
    pg_cur.execute(""" 
      with given AS (SELECT st_union(st_envelope(geom)) as geom from us_states where postal = ANY(%s)),
           expected AS (SELECT st_union(st_buffer(st_envelope(geom), 0.12)) AS geom FROM us_states WHERE postal IN (SELECT unnest(places) FROM macrostrat.usgs_strat_names WHERE id = %s))

    SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name ilike %s AND rank = %s AND ST_Intersects((SELECT geom FROM given), (SELECT geom FROM expected))
    """, [[place['state'] for place in usage['places']], pos[0], removeLithology(parsee["name"]), parsee['rank'] ])

    try:
      new_id = pg_cur.fetchone()[0]
      return new_id
    except:
      return False



def findIDComplexNoLith(usage, parsee) :
  pg_cur.execute("SELECT id, places FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND rank = %s", [parsee['name'], parsee['rank']])
  possibilities = pg_cur.fetchall()

  for pos in possibilities:
    pg_cur.execute(""" 
      with given AS (SELECT st_union(st_envelope(geom)) as geom from us_states where postal = ANY(%s)),
           expected AS (SELECT st_union(st_buffer(st_envelope(geom), 0.12)) AS geom FROM us_states WHERE postal IN (SELECT unnest(places) FROM macrostrat.usgs_strat_names WHERE id = %s))

    SELECT id FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND rank = %s AND ST_Intersects((SELECT geom FROM given), (SELECT geom FROM expected))
    """, [[place['state'] for place in usage['places']], pos[0], parsee['name'], parsee['rank'] ])

    try:
      new_id = pg_cur.fetchone()[0]
      return new_id
    except:
      return False


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
  strat_name_sans_lith text,
  rank text,
  ref_id integer,
  places text[]
);

DROP TABLE IF EXISTS macrostrat.usgs_strat_names_bad;
CREATE TABLE macrostrat.usgs_strat_names_bad (
  usgs_id integer,
  strat_name text,
  rank text,
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

errors = []

# Record our canonnical names
for lex_idx, name in enumerate(lexicon):
  pg_cur.execute(""" 
    INSERT INTO macrostrat.usgs_lexicon_meta (usgs_id, name, geologic_age, usage_notes, other, province) VALUES (%s, %s, %s, %s, %s, %s)
  """, [name['id'], name['name'], ', '.join(name['geologic_age']), name['usage_notes'], name['other'], name['province']])

  #print lex_idx
  for usage_idx, usage in enumerate(name['usage']):
    places = sorted([place['state'] for place in usage['places']])

    if len(usage['parsed_name']) > 0 and usage['parsed_name'][0]['name'].split(' ')[0] == name['name'].split(' ')[0]:
      #print name['id'], usage['parsed_name'][0]['name']

      name_no_lith = removeLithology(usage['parsed_name'][0]['name'])

      pg_cur.execute("""
        INSERT INTO macrostrat.usgs_strat_names (usgs_id, strat_name, strat_name_sans_lith, rank, places) VALUES (%s, %s, %s, %s, %s) RETURNING id
      """, [name['id'], usage['parsed_name'][0]['name'], name_no_lith, usage['parsed_name'][0]['rank'], places ])

      new_id = pg_cur.fetchone()[0]
      lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][0]['new_id'] = new_id
      lexicon[lex_idx]['usage'][usage_idx]['new_id'] = new_id

    else:
      if len(usage['parsed_name']) > 0:
        pg_cur.execute(""" 
          INSERT INTO macrostrat.usgs_strat_names_bad (usgs_id, strat_name, rank, places) VALUES(%s, %s, %s, %s)
        """, [name["id"], usage['parsed_name'][0]['name'], usage['parsed_name'][0]['rank'], places])

      errors.append(usage)
  

pg_conn.commit()

with open('not_inserted_errors.json', 'with') as outerrors:
  json.dump(errors, outerrors, indent=2)

print "--- Done inserting strat names with ", len(errors), " errors ---"

orphans = []

# Assign IDs
findIDs()

for lex_idx, name in enumerate(lexicon):
  for usage_idx, usage in enumerate(name['usage']):
    for baddy in usage['bad_name']:
      pg_cur.execute(""" 
        INSERT INTO macrostrat.usgs_strat_names_bad (usgs_id, strat_name, rank, places) VALUES(%s, %s, %s, %s)
      """, [name["id"], baddy["name"], baddy["rank"], sorted([place['state'] for place in usage['places']])])

new_usgs_id = 50000
for idx, orphan in enumerate(orphans):
  pg_cur.execute(""" 
    SELECT DISTINCT usgs_id FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND places && %s
  """, [removeLithology(orphan["name"]), orphan["places"]])
  
  try:
    orphans[idx]["usgs_id"] = pg_cur.fetchone()[0]
  except :
    orphans[idx]["usgs_id"] = new_usgs_id
    new_usgs_id += 1

  if len(removeLithology(orphans[idx]["name"])) > 1:
    pg_cur.execute("""
      INSERT INTO macrostrat.usgs_strat_names (usgs_id, strat_name, strat_name_sans_lith, rank, places) VALUES (%s, %s, %s, %s, %s) RETURNING id
    """, [orphans[idx]["usgs_id"], orphans[idx]["name"], removeLithology(orphans[idx]["name"]), orphans[idx]["rank"], orphans[idx]["places"] ])
    pg_conn.commit()


# Try to assign IDs again with new names added
findIDs()

with open('orphans.json', 'w') as output:
  json.dump(orphans, output, indent=2)
'''
orphans = []

for idx, orphan in enumerate(orphans):
  pg_cur.execute(""" 
    SELECT DISTINCT usgs_id FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND places && %s
  """, [removeLithology(orphan["name"]), orphan["places"]])
  
  try:
    orphans[idx]["usgs_id"] = pg_cur.fetchone()[0]
  except :
    orphans[idx]["usgs_id"] = new_usgs_id
    new_usgs_id += 1


with open('orphans.json', 'w') as output:
  json.dump(orphans, output, indent=2)
'''

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

no_id = 0
# Insert parents
for name in lexicon:
  for usage in name['usage'] :
    for name_idx in xrange(len(usage['parsed_name']) - 1, 0, -1):
      if 'new_id' in usage['parsed_name'][name_idx] and 'new_id' in usage['parsed_name'][name_idx - 1]:
        pg_cur.execute(""" 
          INSERT INTO macrostrat.usgs_strat_tree (this_name, rel, that_name) VALUES (%s, %s, %s)
        """, [usage['parsed_name'][name_idx]['new_id'], 'parent', usage['parsed_name'][name_idx - 1]['new_id'] ])
      else :
        no_id += 1
        print 'no new id --- ', usage['parsed_name'], [place['state'] for place in usage['places']]

pg_conn.commit()
print "--- Done adding parents ---"

print "no IDs", no_id

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


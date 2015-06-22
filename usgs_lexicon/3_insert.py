import json
import MySQLdb
import sys
import os
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
  connection = MySQLdb.connect(host=credentials.mysql_host, user=credentials.mysql_user, passwd=credentials.mysql_passwd, db=credentials.mysql_db, unix_socket=credentials.mysql_socket)
  pg_conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()
 

# Remove lithological terms from a name
def removeLithology(name):
  words = name.split(" ")
  for idx, word in enumerate(words):
    if word in lithologies or word in upperLiths:
      words.remove(word)

  return " ".join(words)


# Assign new IDs to every name in every usage of every term in the lexicon
def findIDs():
  global orphans

  # Assign new IDs
  # For each name in the lexicon
  for lex_idx, name in enumerate(lexicon):

    # for each usage of each name...
    for usage_idx, usage in enumerate(name['usage']):

      # For each name parsed out of each usage...
      for parsed_idx, parsee in enumerate(usage['parsed_name']):

        # If we have already accounted for this and assigned it an ID, carry on our wayward song
        if "new_id" in lexicon[lex_idx]["usage"][usage_idx]["parsed_name"][parsed_idx]:
          continue

        # Record places for ease of use
        places = [place['state'] for place in usage['places']]

        # Try and get a new ID
        new_id = findIDSimple(usage, parsee)

        # If it worked, sweet action
        if new_id:
          lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

        # If it didn't...
        else :
          if len(places) > 0:
            # Try relaxing space a bit and trying again
            new_id = findIDComplex(usage, parsee)

            # If it worked, sweet action
            if new_id :
               lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

            # If not...
            else :
              # Try removing an lithological terms
              new_id = findIDSimpleNoLith(usage, parsee)

              # If that worked, cool!
              if new_id :
                lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

              # If it didn't...
              else :
                # Keep the lithological terms removed AND relax space
                new_id = findIDComplexNoLith(usage, parsee)

                # If it worked, cool!
                if new_id:
                  lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["new_id"] = new_id

                # If not, we have an orphan
                else:
                  if len(orphans) < 1 :
                    orphans.append({"name": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"], "rank": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"], "places": places})
                  
                  found = False
                  # Check if this orphan already exists
                  for idx, orphan in enumerate(orphans):
                    if orphan["name"] == lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"] and orphan["rank"] == lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"]:
                      found = True
                      # If it does, add any unique places to the existing occurrence
                      for place in places:
                        if place not in orphan["places"]:
                          orphans[idx]["places"].append(place)

                  # If it doesn't exist, add a new orphan
                  if not found and "," not in lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"]:
                    print "adding orphan"
                    orphans.append({"name": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["name"], "rank": lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][parsed_idx]["rank"], "places": places})

          # If no places, welp...
          else :
             print "       No places for ", usage['name'], usage['places']


# Check if the name-rank-place tuple exists
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


# Check if name-rank-place tuple exists (with lithological terms removed)
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


# Check if name-rank-place tuple exists with loose spatial constraints
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


# Check if name-rank-place-place tuple exists with lithological terms removed and loose spatial constraints
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


# Cursors
pg_cur = pg_conn.cursor()
cursor = connection.cursor()

# Clean up/prep. Like a good chef.
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

orphans = []

# Load the output of step #2
with open('parsed_lexicon_ids.json', 'r') as input:
  lexicon = json.loads(input.read())

# Keep track of how many errors are encountered
errors = []

# Record our canonnical names
for lex_idx, name in enumerate(lexicon):
  pg_cur.execute(""" 
    INSERT INTO macrostrat.usgs_lexicon_meta (usgs_id, name, geologic_age, usage_notes, other, province) VALUES (%s, %s, %s, %s, %s, %s)
  """, [name['id'], name['name'], ', '.join(name['geologic_age']), name['usage_notes'], name['other'], name['province']])

  #print lex_idx
  for usage_idx, usage in enumerate(name['usage']):
    places = sorted([place['state'] for place in usage['places']])

    inserted = False
    for parsed_idx, parsee in enumerate(usage['parsed_name']):
      if not inserted:
        if len(parsee) > 0 and parsee['name'].split(' ')[0] == name['name'].split(' ')[0]:
          name_no_lith = removeLithology(usage['parsed_name'][0]['name'])

          pg_cur.execute("""
            INSERT INTO macrostrat.usgs_strat_names (usgs_id, strat_name, strat_name_sans_lith, rank, places) VALUES (%s, %s, %s, %s, %s) RETURNING id
          """, [name['id'], usage['parsed_name'][0]['name'], name_no_lith, usage['parsed_name'][0]['rank'], places ])

          new_id = pg_cur.fetchone()[0]
          lexicon[lex_idx]['usage'][usage_idx]['parsed_name'][0]['new_id'] = new_id
          lexicon[lex_idx]['usage'][usage_idx]['new_id'] = new_id
          inserted = True

    if not inserted:
      if len(usage["bad_name"]) > 0:
        for baddy in usage["bad_name"] :
          pg_cur.execute(""" 
            INSERT INTO macrostrat.usgs_strat_names_bad (usgs_id, strat_name, rank, places) VALUES(%s, %s, %s, %s)
          """, [name["id"], baddy['name'], baddy['rank'], places])

          errors.append(parsee)


pg_conn.commit()

# Write out the errors
with open('not_inserted_errors.json', 'with') as outerrors:
  json.dump(errors, outerrors, indent=2)

print "--- Done inserting strat names with ", len(errors), " errors ---"


# Assign IDs
findIDs()

# Give the orphans a foster home
for idx, orphan in enumerate(orphans):
  pg_cur.execute(""" 
    SELECT DISTINCT usgs_id FROM macrostrat.usgs_strat_names WHERE strat_name_sans_lith ilike %s AND places && %s
  """, [removeLithology(orphan["name"]), orphan["places"]])
  
  try:
    orphans[idx]["usgs_id"] = pg_cur.fetchone()[0]
  except :
    pg_cur.execute(""" 
      SELECT DISTINCT usgs_id FROM macrostrat.usgs_lexicon_meta WHERE name ilike %s
    """, [orphan["name"]])

    try:
      orphans[idx]["usgs_id"] = pg_cur.fetchone()[0]
    except:
      orphans[idx]["usgs_id"] = 0

  if len(removeLithology(orphans[idx]["name"])) > 1:
    pg_cur.execute("""
      INSERT INTO macrostrat.usgs_strat_names (usgs_id, strat_name, strat_name_sans_lith, rank, places) VALUES (%s, %s, %s, %s, %s) RETURNING id
    """, [orphans[idx]["usgs_id"], orphans[idx]["name"], removeLithology(orphans[idx]["name"]), orphans[idx]["rank"], orphans[idx]["places"] ])
    pg_conn.commit()


# Try to assign IDs again with new names added
findIDs()

with open('orphans.json', 'w') as output:
  json.dump(orphans, output, indent=2)


print "--- Done assiging new strat_name_ids ---"


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


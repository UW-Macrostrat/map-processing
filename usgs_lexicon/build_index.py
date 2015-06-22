import json
import re
import urllib2

lith_data = json.loads(urllib2.urlopen('http://localhost:5000/api/v1/defs/lithologies?all').read())

lithologies = ['formation', 'member', 'group', 'supergroup', 'bed', 'beds', 'submember', 'metaquartzite', 'bentonite', 'informal']
upperLiths = []
ranks = ['formation', 'member', 'group', 'supergroup', 'bed', 'beds', 'submember']

for lith in lith_data['success']['data']:
  lithologies.append(lith['lith'])
  upperLiths.append(lith['lith'].title())


strat_data = json.loads(urllib2.urlopen('http://localhost:5000/api/v1/defs/strat_names?all').read())
strat_names = {}
for name in strat_data['success']['data']:
  strat_names[name['name']] = {'name': name['name'], 'rank': name['rank'], 'id': name['id']}



def normalizeName(name) :
  name = name.split(' ')
  removals = []

  name = [re.sub(r'([^\s\w]|_)+', '', word).lower() for word in name]

  for word in name:
    if word in lithologies :
      removals.append(word)

  for each in removals :
    name.remove(each)

  return ' '.join(name).encode('UTF-8')


total = 0
found = 0
lastRank = ''

def getUSGSID(name) :
  global total
  global found

  total += 1
  if name.lower() in lexicon_idx :
    found += 1
    return lexicon_idx[name.lower()]
  else :
    return -99



def flattenHierarchy(name):
  global lastRank
  rank = ''
  origName = name

  if 'Supergroup' in name or 'supergroup' in name:
    rank = 'SGp'
    name = name.replace('Supergroup', '').replace('supergroup', '')

  if 'Group' in name or 'group' in name:
    rank = 'Gp'
    name = name.replace('Group', '').replace('group', '')

  if 'Formation' in name or 'formation' in name:
    rank = 'Fm'
    name = name.replace('Formation', '').replace('formation', '')

  if 'Member' in name or 'member' in name:
    rank = 'Mbr'
    name = name.replace('Member', '').replace('member', '')

  if 'Bed' in name or 'bed' in name:
    rank = 'Bed'
    name = name.replace('Bed', '').replace('bed', '')

  if rank == '':
    words = name.split(' ')
    for word in words:
      if word in upperLiths:
        if lastRank == 'Bed':
          rank = 'Mbr'
        elif lastRank == 'Mbr':
          rank = 'Fm'
        elif lastRank == 'Fm':
          rank = 'Gp'
        elif lastRank == 'Gp':
          rank = 'SGp'
        else:
          rank = 'Fm'


  lastRank = rank
  return {'name': name.strip(), 'rank': rank}




with open('parsed_lexicon.json', 'r') as input:
  lexicon = json.loads(input.read())

lexicon_idx = {}
new_lexicon = {}

for name in lexicon:
  #lexicon_idx[normalizeName(name['name'])] = name['id']

  # For each name in the usage field...
  for syn in name['usage'] :
    lastRank = ''
    good_units = []
    # Clean up, and split on "of"
    units = syn['name'].replace(' in ', ' of ').split(' of ')
    for unit in units:
      if 'informal' in unit:
        clean = unit.replace('(informal)', '').replace('[informal]', '')
        # Split the name on the space
        words = clean.split(' ')
        bad = False
        hasRank = False
        # See if the name contains a known rank
        for word in words:
          if word in ranks:
            hasRank = True
          if word.islower() and word not in ranks:
            bad = True

        if not bad and hasRank:
          good_units.append(clean)

      else :
        words = unit.split(' ')
        bad = False
        for word in words:
          if word.islower() and word not in ranks:
            bad = True

        if not bad:
          good_units.append(unit)


    # Remove hierarchy, but record it
    for i in xrange(len(good_units)):
      good_units[i] = flattenHierarchy(good_units[i])

    # These have units with unknown ranks
    for unit in good_units:
      if unit['name'] in strat_names and unit['rank'] == strat_names[unit['name']]['rank']:
        unit['strat_name_id'] = strat_names[unit['name']]['id']

      else :
        if unit['rank'] != '':
          print unit['name'], unit['rank']
      #if unit['rank'] == '':
      #  print 'Original name --- ', syn['name']
      #  print good_units, name['id'], '\n'
      #  continue
    
    if name['id'] not in new_lexicon:
      new_lexicon[name['id']] = {'names': []}
    
    new_lexicon[name['id']]['names'].append(good_units)

      
    # Check if each already exists in Macrostrat

    # If so, get the strat_name_id

    # If not, create a new record in strat_names
      #INSERT INTO strat_names (strat_name, rank, ref_id, orig_id) VALUES ('something', 'Fm', 14, usgs_id

    # If hierarchy available (and doesn't already exist), add to strat_tree

    lexicon_idx[normalizeName(syn['name'])] = name['id']


#print lexicon_idx
print json.dumps(new_lexicon, indent=2)

#for name in xrange(len(lexicon)):
#  for subtype in lexicon[name]['subunits']:
#    for each in xrange(len(lexicon[name]['subunits'][subtype])):
#      term = normalizeName(lexicon[name]['subunits'][subtype][each]['name'])
      #print term, ' -- ', lexicon[name]['id']
      #if getUSGSID(term) < 0 :
        #print term, ' -- ', lexicon[name]['id']
#      lexicon[name]['subunits'][subtype][each]['usgs_id'] = getUSGSID(term)





print total, found
#print lexicon
#with open('parsed_lexicon_ids.json', 'w') as output:
#  json.dump(lexicon, output, indent=2)

import json
import re
import urllib2

lith_data = json.loads(urllib2.urlopen('http://localhost:5000/api/v1/defs/lithologies?all').read())

lithologies = ['au', 'aux', 'de', 'du', 'la', 'formation', 'member', 'group', 'supergroup', 'beds', 'bed', 'submember', 'metaquartzite', 'bentonite']
upperLiths = []
ranks = ['formation', 'member', 'group', 'supergroup', 'bed', 'beds', 'submember']

for lith in lith_data['success']['data']:
  lithologies.append(lith['lith'])
  upperLiths.append(lith['lith'].title())


strat_data = json.loads(urllib2.urlopen('http://localhost:5000/api/v1/defs/strat_names?all').read())
strat_names = {}
for name in strat_data['success']['data']:
  strat_names[name['name']] = {'name': name['name'], 'rank': name['rank'], 'id': name['id']}





lastRank = ''
badWords = []

def flattenHierarchy(name):
  global lastRank
  rank = ''
  origName = name

  if 'Supergroup' in name or 'supergroup' in name:
    rank = 'SGp'
    name = name.replace('Supergroup', '').replace('supergroup', '')

  if ' Group' in name or ' group' in name:
    rank = 'Gp'
    name = name.replace('Group', '').replace('group', '')

  if 'Formation' in name or 'formation' in name:
    rank = 'Fm'
    name = name.replace('Formatione', '').replace('Formation', '').replace('formation', '')

  if ' Member' in name or ' member' in name:
    rank = 'Mbr'
    name = name.replace('Member', '').replace('member', '')

  if ' Bed' in name or ' bed' in name:
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


def isValidName(name) :
  # Split the name on the space
  words = name.split(' ')
  bad = False
  hasRank = False

  for word in words:
    if word in ranks:
      hasRank = True
    if word.islower() and word not in lithologies:
      bad = True

  if not bad:
    return True
  else:
    return False



with open('parsed_lexicon.json', 'r') as input:
  lexicon = json.loads(input.read())


lexicon_idx = {}
new_lexicon = {}

for i in xrange(len(lexicon)):
  lexicon[i]["name"] = lexicon[i]["name"].replace(".", "").replace("-", "")
  lexicon[i]["name"] = re.sub('\[.+\]', '', lexicon[i]["name"]).strip()
  lexicon[i]["name"] = re.sub('\(.+\)', '', lexicon[i]["name"]).strip()

  if len(lexicon[i]['usage']) > 0:
    for j in xrange(len(lexicon[i]['usage'])):

      if lexicon[i]['usage'][j]['name'].startswith("basalt of") :
        lexicon[i]['usage'][j]['name'] = lexicon[i]['usage'][j]['name'].replace("basalt of", "").strip()


      lastRank = ''
      good_units = []
      bad_units = []

      units = re.sub('\[.+\]', '', lexicon[i]['usage'][j]['name'])
      units = re.sub('\(.+\)', '', units)
      units = units.replace('(informal)', '').replace('[informal]', '').replace('[?]', '').replace('(?)', '').replace('[informal, unranked]', '')
      units = units.replace(' in ', ' of ').split(' of ')

      for unit in units:
        clean = unit.replace('(informal)', '').replace('[informal]', '').replace('[?]', '').replace('(?)', '').replace('[informal, unranked]', '').replace(".", "").replace("-", "").replace("]", "").replace("[", "").replace("?", "").replace('"', '').strip()
        clean = re.sub('\[.+\]', '', clean).strip()
        clean = re.sub('\(.+\)', '', clean).strip()

        if isValidName(clean) :
          good_units.append(clean)
        else:
          bad_units.append(clean)

      # Remove hierarchy, but record it
      for x in xrange(len(good_units)):
        good_units[x] = flattenHierarchy(good_units[x])

      for x in xrange(len(bad_units)):
        bad_units[x] = flattenHierarchy(bad_units[x])

      # If name is known, assign a strat_name_id
      for unit in good_units:
        if unit['name'] in strat_names and unit['rank'] == strat_names[unit['name']]['rank']:
          unit['strat_name_id'] = strat_names[unit['name']]['id']

        # These have no known rank
       # else :
       #   if unit['rank'] != '':
        #    print unit['name'], unit['rank']

      lexicon[i]['usage'][j]['bad_name'] = bad_units
      lexicon[i]['usage'][j]['parsed_name'] = good_units


#print json.dumps(lexicon, indent=2)


with open('parsed_lexicon_ids.json', 'w') as output:
  json.dump(lexicon, output, indent=2)

from bs4 import BeautifulSoup
import re
import json
import sys

## * = USGS recognized/actively used
## (no asterisk) - Only used/recognized by states


def UnitTemplate():
  return {
    'id': -99,
    'name': '',
    'usage': [],
    'usage_notes': '',
    'subunits': {
      'groups': [],
      'formations': [],
      'members': [],
      'unknown': []
    },
    'geologic_age': [],
    'other': '',
    'province': ''
  }


def parseSubunit(target) :
  subunit = {'places': []}
  try :
    states = re.search('\((.+)\)?', target).group(1).replace(')', '').split(',')
  except:
    states = ['Unknown']

  for state in states:
    if '*' in state:
      subunit['places'].append({'state': state.replace('*', '').replace('.', ''), 'in_use': True})
    else :
      subunit['places'].append({'state': state.replace('.', ''), 'in_use': False})

  subunit['name'] = target.split('(')[0].replace('.', '').replace('*', '').replace(':', '').strip()

  if '/' in subunit['name'] :
    subunit['conforms'] = False
    subunit['name'] = subunit['name'].replace('/', '')
  else :
    subunit['conforms'] = True

  return subunit



def parseSubunitState(target, state, in_use) :
  subunit = {'places': [{'state': state, 'in_use': in_use}]}

  subunit['name'] = target.replace('(' + state + ')', '').replace('.', '').replace('*', '').replace(':', '').strip()
  if '*' in subunit['name'] :
    subunit['in_use'] = True

  if '/' in subunit['name'] :
    subunit['conforms'] = False
    subunit['name'] = subunit['name'].replace('/', '')
  else :
    subunit['conforms'] = True

  return subunit



def normalizeName(name):
  # Remove parenthesis and anything between them
  name = re.sub('\(subsurface of .*\)', '', name)
  return name.replace('*', '').replace('/', '').replace(':', '').replace('.', '').replace('[?]', '').strip()



lexicon = []

for i in xrange(1, 16800):
  print i

  try:
    doc = open('usgslex/' + str(i) + '.html', 'r')
  except:
    continue

  # Have to have this jankiness because BeautifulSoup chokes on the cross symbol USGS uses
  newdoc = ''
  for line in doc:
    newdoc += line.replace('&#134', '')
  soup = BeautifulSoup(newdoc)

  # Create a new unit instance
  unit = UnitTemplate()
  unit['id'] = i
  unit['name'] = soup.find('span', class_='majorheading').text.replace('Geologic Unit:', '').strip()

  attributes = soup.find_all('span', class_='strongheading')
  
  for each in attributes :
    attr_title = each.text.replace(":", "").strip().lower()
    attr_contents = each.findNext('p').contents

    if attr_title == 'usage' :
      for name in attr_contents :
        if isinstance(name, basestring) :
          usageInstance = {'name': '', 'places': []}
          if 'No current usage' in name:
            unit['usage_notes'] = ' '.join([i for i in attr_contents if isinstance(i, basestring)]).strip()
            break

          if name.strip()[:1] == '(' or '(' not in name:
            unit['usage_notes'] = ' '.join([i for i in attr_contents if isinstance(i, basestring)]).strip()
            break


          parts = re.split('(\([A-Z]{2}.*)', name)
          usageInstance['name'] = normalizeName(parts[0])


          if len(usageInstance['name']) < 1 or len(re.sub(r'\W+', '', usageInstance['name'])) < 1:
            break
          

          if (parts[0][:1] == '/') :
            usageInstance['conforms'] = False
          else :
            usageInstance['conforms'] = True

          states = re.findall('[A-Z]{2}\*?', name)
          if len(states) > 0:
            for state in states:
              if '*' in state:
                usageInstance['places'].append({'state': normalizeName(state), 'in_use': True})
              else :
                usageInstance['places'].append({'state': normalizeName(state), 'in_use': False})

          unit['usage'].append(usageInstance)


    
    if attr_title == 'subunits':
      # Check for group or formation status first
      if 'GROUP STATUS' in attr_contents[0] and 'FORMATION STATUS' in attr_contents[0]:
        formations = re.split(': |\.|FORMATION', attr_contents[0])[1].split(',')
        members = re.split(': ')[3].split(',')

        rank = 'formations'
        for formation in formations:
          subunit = parseSubunit(formation)
          unit['subunits'][rank].append(subunit)

        rank = 'members'
        for member in members:
          subunit = parseSubunit(member)
          unit['subunits'][rank].append(subunit)


      elif 'FORMATION STATUS' in attr_contents[0] and 'MEMBER STATUS' in attr_contents[0]:

        members = re.split(': |\.|MEMBER', attr_contents[0])[1].split(', ')
        others = re.split(': ')[3].split(', ')

        rank = 'members'
        for member in members:
          subunit = parseSubunit(member)
          unit['subunits'][rank].append(subunit)

        rank = 'unknown'
        for other in others:
          subunit = parseSubunit(other)
          unit['subunits'][rank].append(subunit)


    
      elif '--all in ' in attr_contents[0]:

        ## Need to split this on ST(*) + 
        try :
          state = re.split(r'\s(?=[A-Z]{2}\*?:?)', attr_contents[0])[1]
          state = state.split(':')[0]
          #state = re.search('[A-Z]{2}\*?', attr_contents[0].split(':')[0]).group(0)
        except :
          print attr_contents[0]

        if '*' in state:
          in_use = True
        else:
          in_use = False

        subunits = attr_contents[0].split(':')[1]
        subunits = re.split(', |; ', subunits)

        for each in subunits :
          subunit = parseSubunitState(each, state, in_use)

          if subunit['name'].count('Member') > 0 :
            unit['subunits']['members'].append(subunit)
          elif subunit['name'].count('Formation') > 0 :
            unit['subunits']['formations'].append(subunit)
          else :
            unit['subunits']['unknown'].append(subunit)
        

      elif 'GROUP STATUS' in attr_contents[0]:
        rank = 'formations'
        formations = attr_contents[0].replace('GROUP STATUS (alphabetical):', '').replace('GROUP STATUS:', '').split(', ')
        for formation in formations:
          subunit = parseSubunit(formation)
          unit['subunits'][rank].append(subunit)



      elif 'FORMATION STATUS' in attr_contents[0]:
        rank = 'members'
        members = attr_contents[0].replace('FORMATION STATUS (alphabetical):', '').replace('FORMATION STATUS:', '').split(', ')
        for member in members:
          subunit = parseSubunit(member)
          unit['subunits'][rank].append(subunit)


      elif 'MEMBER STATUS' in attr_contents[0]:
        rank = 'unknown'
        members = attr_contents[0].split(':')[1].split(', ')
        for member in members:
          subunit = parseSubunit(member)
          unit['subunits'][rank].append(subunit)


      elif '(alphabetical)' in attr_contents[0]:
        try :
          # comma split needs to be on ),
          subunits = attr_contents[0].split(':')[1].split(', ')
        except:
          try:
            # stupid 6424
            subunits = attr_contents[0].split(';')[1].split(',')
          except:
            #stupid 8566
            subunits = attr_contents[0].split('habetical) ')[1].split(',')

        for each in subunits:
          subunit = parseSubunit(each)

          if subunit['name'].count('Member') > 0 :
            unit['subunits']['members'].append(subunit)
          elif subunit['name'].count('Formation') > 0 :
            unit['subunits']['formations'].append(subunit)
          else :
            unit['subunits']['unknown'].append(subunit)

      else:
        subunits = re.sub('\((.*)\)', '', attr_contents[0])
        subunits = re.split('\),|, ', subunits)
        for each in subunits:
          if ';' in each:
            subsubunits = each.split(';')
            for subsubunit in subsubunits:
              subunit = parseSubunit(subsubunit)
              if subunit['name'].count('Member') > 0 :
                unit['subunits']['members'].append(subunit)
              elif subunit['name'].count('Formation') > 0 :
                unit['subunits']['formations'].append(subunit)
              else :
                unit['subunits']['unknown'].append(subunit)

          else:
            subunit = parseSubunit(each)

          if subunit['name'].count('Member') > 0 :
            unit['subunits']['members'].append(subunit)
          elif subunit['name'].count('Formation') > 0 :
            unit['subunits']['formations'].append(subunit)
          else :
            unit['subunits']['unknown'].append(subunit)

        #print '-----Subunits exist but not sure about the format -----', attr_contents



    if attr_title == 'geologic age' :
      for age in attr_contents:
        if isinstance(age, basestring) :
          age = age.replace('*', '').strip()
          if ',' in age:
            age = age.split(',')
            age = ' '.join(reversed(age)).strip()

          unit['geologic_age'].append(age)

    if 'origin of name' in attr_title:
      unit['other'] = attr_contents[0].strip()

    if 'geologic province' in attr_title:
      province = attr_contents[0].replace('*', '').strip()
      unit['province'] = province

    #print attr_title, '----', attr_contents

  lexicon.append(unit)


#print json.dumps(lexicon, indent=2)

with open('parsed_lexicon.json', 'w') as output:
  json.dump(lexicon, output, indent=2)


'''
lexicon = [
  {
    'id': 14861,
    'name': 'Western Cascade',
    'usage': [
      {'name': 'Western Cascade Volcanics', 'places': [
          {'state': 'CA', 'in_use': True}, 
          {'state': 'OR', 'in_use': True}
        ]
      },
      {'name': 'Western Cascade Group', 'places': [
          {'state': 'CA', 'in_use': True}, 
          {'state': 'OR', 'in_use': True},
          {'state': 'WA', 'in_use': True}
        ]
      }
    ],
    'subunits': {
      'groups': [
        {'name': 'beds of Bull Creek (informal)', 'places': [{'state': 'OR', 'in_use': True}]},
        {'name': 'Colestin Formation', 'places': [{'state': 'CA', 'in_use': True}]},
        {'name': 'Roxy Formation', 'places': [{'state': 'CA', 'in_use': True}]},
        {'name': 'Wasson Formation', 'places': [{'state': 'CA', 'in_use': True}]}
      ],
      'formations' : []
    },
    'geologic_age': ['Miocene', 'Oligocene', 'Eocene'],
    'other': 'No synopysized to date. [See US geologic names lexicon, USGS Bull. 1200]',
    'province': ''
  }
]
'''
import urllib2
from bs4 import BeautifulSoup

for x in xrange(1, 16800):
  print x
  req = urllib2.urlopen("http://ngmdb.usgs.gov/Geolex/NewUnits/unit_" + str(x) + ".html")

  # Soup it
  soup = BeautifulSoup(req.read())

  # 404's return a 200 response code, so check the title instead
  if soup.title.text != 'Page not found | NGMDB':
    with open('usgslex/' + str(x) + '.html', 'w') as out:
      # Save only what we need
      out.write(BeautifulSoup(unicode.join(u'\n',map(unicode,soup.find('div', class_="glx_primary")))).prettify().encode('UTF-8'))


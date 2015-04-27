import psycopg2
import json
from collections import OrderedDict
import sys, os
sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials


# Connect to the database
try:
  conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
except:
  print "Could not connect to database: ", sys.exc_info()[1]
  sys.exit()

cur = conn.cursor()

cur.execute("select distinct macro_color FROM gmus.lookup_units WHERE macro_color IS NOT NULL AND macro_color != ''")
colors = cur.fetchall()

css = ""
macro_css = """
Map {
  background-color: transparent;
}

#gmus {
  polygon-opacity:1;
  polygon-fill: #000;
  line-color: #aaa;
}
#gmus[zoom<=9] {
  line-width: 0.0;
}
#gmus[zoom>9] {
  line-width: 0.2;
}
#gmus[macro_color="null"] {
   polygon-fill: #777777;
}
#gmus[macro_color=null] {
   polygon-fill: #777777;
}
#gmus[macro_color=""] {
   polygon-fill: #777777;
}

"""

xml = """<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE Map>
<Map srs="+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0.0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs +over" background-color="rgba(0, 0, 0, 0)">

<Layer name="gmus" status="on" srs="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs">
  <StyleName>gmus</StyleName>
  <Datasource>
     <Parameter name="type"><![CDATA[postgis]]></Parameter>
     <Parameter name="table"><![CDATA[(SELECT gid, macro_color, geom from gmus.lookup_units) AS data]]></Parameter>
     <Parameter name="key_field"><![CDATA[gid]]></Parameter>
     <Parameter name="geometry_field"><![CDATA[geom]]></Parameter>
     <Parameter name="extent_cache"><![CDATA[auto]]></Parameter>
     <Parameter name="extent"><![CDATA[-126.488422097817,24.2764737404176,-66.7433374647366,49.3845234431582]]></Parameter>
     <Parameter name="host"><![CDATA[localhost]]></Parameter>
     <Parameter name="port"><![CDATA[5432]]></Parameter>
     <Parameter name="user"><![CDATA[john]]></Parameter>
     <Parameter name="dbname"><![CDATA[geomacro]]></Parameter>
  </Datasource>
</Layer>

<Style name="gmus" filter-mode="first">
"""

xml_template = """ 
<Rule>
  <MaxScaleDenominator>750000</MaxScaleDenominator>
  <Filter>([macro_color] = '%s')</Filter>
  <PolygonSymbolizer fill="%s" fill-opacity="1" />
  <LineSymbolizer stroke-width="0.2" stroke="#aaaaaa" />
</Rule>
"""


for color in colors :
 # css += '#gmus[interval_color="' + color[0] + '"] {\n   polygon-fill: ' + color[0] + ';\n}\n'
  macro_css += '#gmus[macro_color="' + color[0] + '"] {\n   polygon-fill: ' + color[0] + ';\n}\n'
  xml += xml_template % (color[0], color[0])

#with open("gmus_styles.txt", "w") as output:
#    output.write(css)

xml += """
</Style>

</Map>
"""
#with open("/Users/" + credentials.system_user + "/Documents/MapBox/project/gmus_new/style.mss", "w") as output:
#    output.write(macro_css)

with open("/Users/" + credentials.system_user + "/gmus.xml", "w") as output:
    output.write(xml)


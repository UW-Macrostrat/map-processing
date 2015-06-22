import os
import psycopg2
from psycopg2.extensions import AsIs
import sys
import subprocess

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

# Connect to Postgres
pg_conn = psycopg2.connect(dbname=credentials.pg_db, user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_port)
pg_cur = pg_conn.cursor()


# 1. Low hanging fruit. Fill best_* if * or new_* are the same
pg_cur.execute("UPDATE gmus.units SET best_unit_name = unit_name WHERE unit_name = new_unit_name");
pg_cur.execute("UPDATE gmus.units SET best_unitdesc = unitdesc WHERE unitdesc = new_unitdesc");
pg_cur.execute("UPDATE gmus.units SET best_unit_com = unit_com WHERE unit_com = new_unit_com");
pg_cur.execute("UPDATE gmus.units SET best_strat_unit = strat_unit WHERE strat_unit = new_strat_unit");
pg_conn.commit()

# nulls after step1 (6283 rows in gmus.units):
# best_unit_name - 36
# best_unitdesc - 574
# best_unit_com - 2685 (500 not empty)
# best_strat_unit - 5263 (13 not empty)

# 2. If len(thing) > 1 and len(other thing) < 1, set best_thing = thing; Do inverse as well

pg_cur.execute("UPDATE gmus.units SET best_unit_name = unit_name WHERE char_length(unit_name) > 1 and new_unit_name is null");
pg_cur.execute("UPDATE gmus.units SET best_unitdesc = unitdesc WHERE char_length(unitdesc) > 1 and new_unitdesc is null");
pg_cur.execute("UPDATE gmus.units SET best_unit_com = unit_com WHERE char_length(unit_com) > 1 and new_unit_com is null");
pg_cur.execute("UPDATE gmus.units SET best_strat_unit = strat_unit WHERE char_length(strat_unit) > 1 and new_strat_unit is null");

# nulls after step2 (6283 rows in gmus.units):
# best_unit_name - 18
# best_unitdesc - 557 (299 not empty)
# best_unit_com - 2668 (483 not empty)
# best_strat_unit - 5261 (11 not empty)

# 3. char_length(unitdesc) < 255 and char_length(new_unitdesc) > 255, use new_unitdesc
pg_cur.execute("UPDATE gmus.units SET best_unitdesc = new_unitdesc WHERE char_length(unitdesc) < 255 AND char_length(new_unitdesc) > 255")
pg_cur.execute("UPDATE gmus.units SET best_unit_com = new_unit_com WHERE char_length(unit_com) < 255 AND char_length(new_unit_com) > 255")

# nulls after step2 (6283 rows in gmus.units):
# best_unit_name - 18
# best_unitdesc - 288 (30 not empty)
# best_unit_com - 2304 (119 not empty)
# best_strat_unit - 5261 (11 not empty)

pg_cur.execute("update gmus.units set best_strat_unit = new_strat_unit where char_length(new_strat_unit) > char_length(strat_unit) and best_strat_unit is null");
pg_cur.execute("update gmus.units set best_unit_com = new_unit_com where unit_com is not null AND best_unit_com is null");


pg_cur.execute("update gmus.units set best_unit_name = new_unit_name where id in (3276, 6106, 4538, 1877, 4533, 6254, 6253)");

# A + B
pg_cur.execute("update gmus.units set best_unit_name = concat(unit_name, ' | ', new_unit_name) where id in (6238, 4968, 6135, 5120)");

# A
pg_cur.execute("update gmus.units set best_unit_name = unit_name where id in (2733, 2732, 2736, 2735, 2737, 2734, 5115)");

pg_cur.execute("update gmus.units set best_unitdesc = concat(unitdesc, ' | ', new_unitdesc) WHERE id in (6238, 4968, 6135, 5120) and best_unitdesc is null");
pg_cur.execute("update gmus.units set best_unitdesc = new_unitdesc WHERE best_unitdesc is null");

# Leftovers
pg_cur.execute("update gmus.units set best_unit_name = new_unit_name where unit_link = 'SDJms;0'");

pg_conn.commit()





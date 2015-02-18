import os
import psycopg2
import sys
import csv

sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials

conn = psycopg2.connect(dbname='geomacro', user=credentials.pg_user, host=credentials.pg_host, port=credentials.pg_host)
cur = conn.cursor()

cwd = '/Users/john/Downloads/gmus/csvs'
files = [ f for f in os.listdir(cwd) if os.path.isfile(os.path.join(cwd,f)) and not f.startswith('.') ]

def fix_row(total_rows, row) :
  newrow = []
  for field in row :
    # So. Much. BS. To. Clean. UP.
    field = field.replace('\"', '')
    field = field.strip()
    field = " ".join(field.split())
    # Oye...use a real encoding!
    try :
      field = field.decode('latin1').encode('utf-8')
    except UnicodeDecodeError as E:
      print field, E
      sys.exit()

    # Be explicit and consistent with null values
    if field == '' :
      field = "\N"

    newrow.append(field)

  # Make sure we're not missing any fields, otherwise psql bonks
  while len(newrow) < total_rows :
    newrow.append("\N")

  return newrow


def fix_csv(file) :
  if not 'redo' in file :
    # Create a new file name that's the same as the old but with _redo
    new_file = cwd + "/" + file.replace('.csv', '') + '_redo.csv'

    # Keep track of total rows to make sure each row is the same length regardless of null values
    total_rows = 0

    with open((cwd + "/" + file), 'rb') as inputcsv :
      with open((new_file), 'wb') as outputcsv :
        reader = csv.reader(inputcsv, delimiter=',', quotechar='"', dialect=csv.excel)
        writer = csv.writer(outputcsv, delimiter=',', quotechar='"', dialect=csv.excel)

        for i, row in enumerate(reader) :
          if i == 0 :
            total_rows = len(row)

          # Clean it up
          newrow = fix_row(total_rows, row)

          # Write our fixed row to our new file
          writer.writerow(newrow)

      return new_file

  # If the file detected has already been fixed (has _redo in name), PUNT
  else :
    return False


for dataset in files :
  new_file = fix_csv(dataset)

  # Make sure we're not fixing and importing an already fixed CSV
  if new_file :
    table = ''
    fields = ''

    # Figure out which dataset we're dealing with
    if 'age' in dataset :
      table = 'ages'
      fields = '(unit_link, min_eon, min_era, min_period, min_epoch, min_age, full_min, cmin_age, max_eon, max_era, max_period, max_epoch, max_age, full_max, cmax_age, min_ma, max_ma, age_type, age_com)'

    elif 'lith' in dataset :
      table = 'liths'
      fields = '(unit_link, lith_rank, lith1, lith2, lith3, lith4, lith5, total_lith, low_lith, lith_form, lith_com)'

    elif 'ref-link' in dataset :
      table = 'reflinks'
      fields = '(unit_link, ref_id)'

    elif 'ref' in dataset and 'ref-link' not in dataset :
      table = 'refs'
      fields = '(ref_id, reference)'

    elif 'units' in dataset :
      table = 'units'
      fields = '(state, orig_label, map_sym1, map_sym2, unit_link, prov_no, province, unit_name, unit_age, unitdesc, strat_unit, unit_com, map_ref, rocktype1, rocktype2, rocktype3, unit_ref)'

    # Make sure we found a match
    if len(table) > 1 :
      cur.execute("COPY gmus." + table + " " + fields + " FROM '" + new_file + "' NULL '\N' QUOTE '\"' CSV HEADER")
      conn.commit()
      os.remove(new_file)
      print table, new_file   


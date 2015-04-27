import sys, os
import argparse
import subprocess
sys.path = [os.path.join(os.path.dirname(__file__), os.pardir)] + sys.path
import credentials
import subprocess


# Add argument parser - accept gmus, gmna, gmus-faults, and gmna-faults

parser = argparse.ArgumentParser(
  description="Reroll tiles for a given map dataset",
  epilog="Example usage goes here")

parser.add_argument("-m", "--map", dest="map_type",
  default="na", type=str, required=True,
  help="Map type to roll tiles for. Can be 'gmus', 'gmna', 'gmus-faults', or 'gmna-faults'")

arguments = parser.parse_args()

if (arguments.map_type not in ["gmus", "gmna", "gmus-faults", "gmna-faults"]) :
  print "Invalid map type given"
  sys.exit()


# Rebuild the Postgres database to make sure everything is up to date
execfile(os.path.dirname(os.path.realpath(sys.argv[0])) + "/rebuild.py")

# styles.py
execfile(os.path.dirname(os.path.realpath(sys.argv[0])) + "/styles.py")

# Roll dem tiles
config = {
  "gmus": "-b 24 -128 50 -66 -c " + os.path.dirname(os.path.realpath(sys.argv[0])) + "/TileStache/tilestache.cfg -l gmus 5 6 7 8 9 10 11 12",
  "gmna": "-b 5.5 -180 90 180 -c " + os.path.dirname(os.path.realpath(sys.argv[0])) + "/TileStache/tilestache.cfg -l gmna 1 2 3 4 5 6 7"
}

os.system(os.path.dirname(os.path.realpath(sys.argv[0])) + "/TileStache/scripts/tilestache-seed.py " + config[arguments.map_type])

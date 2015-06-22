DROP TABLE IF EXISTS gmus.ages;
DROP TABLE IF EXISTS gmus.liths;
DROP TABLE IF EXISTS gmus.reflinks;
DROP TABLE IF EXISTS gmus.refs;
DROP TABLE IF EXISTS gmus.units;

CREATE TABLE gmus.ages (
  id serial PRIMARY KEY NOT NULL,
  unit_link character varying(28),
  min_eon text,
  min_era text,
  min_period text,
  min_epoch text,
  min_age text,
  full_min text,
  cmin_age text,
  max_eon text,
  max_era text,
  max_period text,
  max_epoch text,
  max_age text,
  full_max text,
  cmax_age text,
  min_ma numeric,
  max_ma numeric,
  age_type text,
  age_com text,
  macro_containing_interval_id integer,
  macro_min_interval_id integer,
  macro_max_interval_id integer
);


CREATE TABLE gmus.liths (
  id serial PRIMARY KEY NOT NULL,
  unit_link character varying(28),
  lith_rank text,
  lith1 text,
  lith2 text,
  lith3 text,
  lith4 text,
  lith5 text,
  total_lith text,
  low_lith text,
  lith_form text,
  lith_com text
);


CREATE TABLE gmus.reflinks (
  id serial PRIMARY KEY NOT NULL,
  unit_link character varying(28),
  ref_id character varying(12)
);


CREATE TABLE gmus.refs (
  id serial PRIMARY KEY NOT NULL,
  ref_id character varying(12),
  reference text
);


CREATE TABLE gmus.units (
  id serial PRIMARY KEY NOT NULL,
  state character varying(2),
  orig_label text,
  map_sym1 text,
  map_sym2 text,
  unit_link character varying(28),
  prov_no smallint,
  province text,
  unit_name text,
  unit_age text,
  unitdesc text,
  strat_unit text,
  unit_com text,
  map_ref text,
  rocktype1 text,
  rocktype2 text,
  rocktype3 text,
  unit_ref text,

  new_unit_name text,
  new_unitdesc text,
  new_unit_com text,
  new_strat_unit text,

  best_unit_name text DEFAULT NULL,
  best_unitdesc text DEFAULT NULL,
  best_unit_com text DEFAULT NULL,
  best_strat_unit text DEFAULT NULL
);


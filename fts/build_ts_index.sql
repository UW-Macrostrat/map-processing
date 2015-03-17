
alter table fts_test add column text_search tsvector;

--update fts_test set text_search = setweight(to_tsvector('macro', coalesce(unit_name, '')), 'A')   ||
--setweight(to_tsvector('macro', coalesce(strat_unit, '')), 'B')  ||
--setweight(to_tsvector('macro', coalesce(unitdesc, '')), 'C')   ||
--setweight(to_tsvector('macro', coalesce(unit_com, '')), 'D');

update fts_test set text_search = to_tsvector('macro', concat(unit_name, ' ', strat_unit, ' ', unitdesc, ' ', unit_com));


CREATE INDEX test_ts_idx ON fts_test USING GIN (text_search);

--select * from fts_test WHERE text_search @@ to_tsquery('macro', '''bangor ls''')
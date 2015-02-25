psql -h localhost -p 5439 -c "DROP DATABASE geomacro;"
pg_dump -C geomacro | psql -h localhost -p 5439

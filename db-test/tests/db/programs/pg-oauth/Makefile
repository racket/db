all: oauth_dupe.so

oauth_dupe.so: oauth_dupe.o
	gcc -o oauth_dupe.so -shared oauth_dupe.o

oauth_dupe.o: oauth_dupe.c
	gcc -c -I `pg_config --includedir-server` -fPIC oauth_dupe.c

install: oauth_dupe.so
	cp oauth_dupe.so `pg_config --pkglibdir`

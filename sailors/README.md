# Sailors 

## Setup - create tables using psql
1. Start postgres container
```bash
    docker exec -it sailors_db psql -U postgres
``` 
2. Create database `sailors_db` in postgres container
```psql
    CREATE DATABASE sailors_db;
```
Verify it exists
```
    \l
```

3. Get out of psql container
```
    \q
```

4. Populate tables with `sailors-mysql.sql`
```bash
docker exec -i sailors_db psql -U postgres -d sailors_db < sailors-mysql.sql
```

5. Connect to database and see if tables exist
```bash
docker exec -it sailors_db psql -U postgres -d sailors_db
```
in psql shell
```
    \dt
```

6. Run SQL queries to your heart's desire 


## Part 2 SQLAlchemy
1. Change `DATABASE_URL` as needed. Make sure `DATABASE` has already been created. 
```DATABASE_URL = "postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>"```

2. 
```bash
python create_sailors.py
```

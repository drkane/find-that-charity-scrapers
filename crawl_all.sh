alembic upgrade head
for i in $(scrapy list); 
    do scrapy crawl "$i" -s DB_URI="$DB_URI"; 
done; 

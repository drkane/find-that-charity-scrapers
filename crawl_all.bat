alembic upgrade head
for /F %i in ('scrapy list') do scrapy crawl "%i" -s DB_URI="%DB_URI%"
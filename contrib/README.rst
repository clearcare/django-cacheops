To use this dump do something like this

# stop time sync
sudo /etc/init.d/ntpdate stop

# stop redis
sudo /etc/init.d/redis-server stop

# set your datetime to that of the dump (or all the keys will expire)
sudo date 04200500  # That's PDT

# Copy the dump so redis can load it
sudo cp cacheops-redis-20150421-1300.rdb /var/lib/redis/dump.rdb

# start redis
sudo /etc/init.d/redis-server start

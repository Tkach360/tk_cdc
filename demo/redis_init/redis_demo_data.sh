#!/bin/sh

until redis-cli -h redis -p 6379 ping | grep -q PONG; do
  sleep 1
done

echo "Redis is ready, loading data..."

redis-cli -h redis SET 'user:1' '{"name":"Alice","email":"alice@test.com"}'
redis-cli -h redis SET 'user:2' '{"name":"Joe","email":"cooljoe@test.com"}'
redis-cli -h redis SET 'user:3' '{"name":"Bob","email":"marley@test.com"}'
redis-cli -h redis SET 'user:4' '{"name":"Skinny Pete","email":"skinnypete@test.com"}'
redis-cli -h redis SET 'user:5' '{"name":"Badger","email":"badger@test.com"}'
redis-cli -h redis SET 'user:6' '{"name":"Hank Schrader","email":"schrader@test.com"}'

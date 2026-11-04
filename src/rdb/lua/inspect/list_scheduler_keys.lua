local now = tonumber(ARGV[1])
local keys = redis.call("ZRANGEBYSCORE", KEYS[1], now, "+inf")
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now-1)
return keys

-- KEYS[1] -> zset which holds task ids (e.g. asynq:{<qname>}:scheduled)
-- KEYS[2] -> asynq:{<qname>}:pending
-- --
-- ARGV[1] -> task key prefix
-- Output:
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
return table.getn(ids)

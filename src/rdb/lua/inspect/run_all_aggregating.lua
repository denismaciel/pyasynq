-- KEYS[1] -> asynq:{<qname>}:g:<gname>
-- KEYS[2] -> asynq:{<qname>}:pending
-- KEYS[3] -> asynq:{<qname>}:groups
-- -------
-- ARGV[1] -> task key prefix
-- ARGV[2] -> group name
-- Output:
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("LPUSH", KEYS[2], id)
	redis.call("HSET", ARGV[1] .. id, "state", "pending")
end
redis.call("DEL", KEYS[1])
redis.call("SREM", KEYS[3], ARGV[2])
return table.getn(ids)

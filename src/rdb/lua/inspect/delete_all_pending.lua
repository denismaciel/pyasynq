-- KEYS[1] -> asynq:{<qname>}:pending
-- --
-- ARGV[1] -> task key prefix
-- Output:
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)

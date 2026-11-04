-- KEYS[1] -> asynq:{<qname>}:g:<gname>
-- KEYS[2] -> asynq:{<qname>}:groups
-- -------
-- ARGV[1] -> task key prefix
-- ARGV[2] -> group name
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("SREM", KEYS[2], ARGV[2])
redis.call("DEL", KEYS[1])
return table.getn(ids)

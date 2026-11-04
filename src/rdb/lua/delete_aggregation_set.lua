-- KEYS[1] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
-- KEYS[2] -> asynq:{<qname>}:aggregation_sets
-- -------
-- ARGV[1] -> task key prefix
-- Output:
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids)  do
	redis.call("DEL", ARGV[1] .. id)
end
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], KEYS[1])
return redis.status_reply("OK")

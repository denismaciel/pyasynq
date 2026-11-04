-- KEYS[1] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
-- ------
-- ARGV[1] -> task key prefix
-- Output:
local msgs = {}
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local key = ARGV[1] .. id
	table.insert(msgs, redis.call("HGET", key, "msg"))
end
return msgs

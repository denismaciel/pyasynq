-- KEYS[1] -> zset holding the task ids.
-- --
-- ARGV[1] -> task key prefix
-- Output:
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
for _, id in ipairs(ids) do
	local task_key = ARGV[1] .. id
	local unique_key = redis.call("HGET", task_key, "unique_key")
	if unique_key and unique_key ~= "" and redis.call("GET", unique_key) == id then
		redis.call("DEL", unique_key)
	end
	redis.call("DEL", task_key)
end
redis.call("DEL", KEYS[1])
return table.getn(ids)

-- KEYS[1] -> asynq:{<qname>}:groups
-- -------
-- ARGV[1] -> group key prefix
-- Output:
local res = {}
local group_names = redis.call("SMEMBERS", KEYS[1])
for _, gname in ipairs(group_names) do
	local size = redis.call("ZCARD", ARGV[1] .. gname)
	table.insert(res, gname)
	table.insert(res, size)
end
return res

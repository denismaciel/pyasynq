-- KEYS[1] -> key for id list (e.g. asynq:{<qname>}:pending)
-- ARGV[1] -> start offset
-- ARGV[2] -> stop offset
-- ARGV[3] -> task key prefix
local ids = redis.call("LRange", KEYS[1], ARGV[1], ARGV[2])
local data = {}
for _, id in ipairs(ids) do
	local key = ARGV[3] .. id
	local msg, result = unpack(redis.call("HMGET", key, "msg","result"))
	table.insert(data, msg)
	table.insert(data, result)
end
return data

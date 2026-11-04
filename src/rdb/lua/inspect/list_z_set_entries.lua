-- KEYS[1] -> key for ids set (e.g. asynq:{<qname>}:scheduled)
-- ARGV[1] -> min
-- ARGV[2] -> max
-- ARGV[3] -> task key prefix
-- Returns an array populated with
local data = {}
local id_score_pairs = redis.call("ZRANGE", KEYS[1], ARGV[1], ARGV[2], "WITHSCORES")
for i = 1, table.getn(id_score_pairs), 2 do
	local id = id_score_pairs[i]
	local score = id_score_pairs[i+1]
	local key = ARGV[3] .. id
	local msg, res = unpack(redis.call("HMGET", key, "msg", "result"))
	table.insert(data, msg)
	table.insert(data, score)
	table.insert(data, res)
end
return data

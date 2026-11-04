-- KEYS[1] -> task key (asynq:{<qname>}:t:<taskid>)
-- ARGV[1] -> task id
-- ARGV[2] -> current time in Unix time (seconds)
-- ARGV[3] -> queue key prefix (asynq:{<qname>}:)
-- Output:
	if redis.call("EXISTS", KEYS[1]) == 0 then
		return redis.error_reply("NOT FOUND")
	end
	local msg, state, result = unpack(redis.call("HMGET", KEYS[1], "msg", "state", "result"))
	if state == "scheduled" or state == "retry" then
		return {msg, state, redis.call("ZSCORE", ARGV[3] .. state, ARGV[1]), result}
	end
	if state == "pending" then
		return {msg, state, ARGV[2], result}
	end
	return {msg, state, 0, result}

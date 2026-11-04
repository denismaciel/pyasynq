-- KEYS[1] -> asynq:{<qname>}:pending
-- KEYS[2] -> asynq:{<qname>}:paused
-- KEYS[3] -> asynq:{<qname>}:active
-- KEYS[4] -> asynq:{<qname>}:lease
-- ARGV[1] -> initial lease expiration unix time
-- ARGV[2] -> task key prefix
--
-- Returns the encoded TaskMessage, or nil if the queue is paused or empty.
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[2] .. id
		redis.call("HSET", key, "state", "active")
		redis.call("HDEL", key, "pending_since")
		redis.call("ZADD", KEYS[4], ARGV[1], id)
		return redis.call("HGET", key, "msg")
	end
end
return nil

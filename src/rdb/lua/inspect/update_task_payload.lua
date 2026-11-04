-- KEYS[1] -> asynq:{<qname>}:t:<task_id>
-- --
-- ARGV[1] -> task message data
-- Output:
-- Returns 1 if task is successfully updated.
-- Returns 0 if task is not found.
-- Returns -1 if task is not in scheduled state.
-- Check if given taks exists
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, pending_since, group, unique_key = unpack(redis.call("HMGET", KEYS[1], "state", "pending_since", "group", "unique_key"))
if state ~= "scheduled" then
    return -1
end
local redis_call_args = {"state", state}

if pending_since then
    table.insert(redis_call_args, "pending_since")
    table.insert(redis_call_args, pending_since)
end
if group then
    table.insert(redis_call_args, "group")
    table.insert(redis_call_args, group)
end
if unique_key then
    table.insert(redis_call_args, "unique_key")
    table.insert(redis_call_args, unique_key)
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], unpack(redis_call_args))
return 1

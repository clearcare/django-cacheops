local conj_key = ARGV[1]
local bytes = 0 
local processed = 0
local deleted = 0

for i, key in ipairs(KEYS) do
    local exists = redis.call('exists', key)
    processed = processed + 1
    if exists == 0 then
        bytes = bytes + string.len(key)
        deleted = deleted + 1
        local response = redis.call('srem', conj_key, key)
        if response == 0 then
            redis.log(redis.LOG_NOTICE, 'srem ' .. conj_key .. ' ' .. key .. ' ' .. response .. ' ' .. exists)
        end
    end
end

if redis.call('scard', conj_key) == 0 then
    redis.call('del', conj_key)
    bytes = bytes + string.len(conj_key)
end

return {processed, deleted, bytes}

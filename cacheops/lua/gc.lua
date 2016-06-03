local conj_key = ARGV[1]
local bytes = 0 
local processed = 0
local deleted_items = 0
local deleted_sets = 0
local errors = 0

for i, key in ipairs(KEYS) do
    local exists = redis.call('exists', key)
    processed = processed + 1
    if exists == 0 then
        bytes = bytes + string.len(key)
        deleted_items = deleted_items + 1
        local response = redis.call('srem', conj_key, key)
        if response == 0 then
            errors = errors + 1
            redis.log(redis.LOG_NOTICE, 'srem ' .. conj_key .. ' ' .. key .. ' ' .. response .. ' ' .. exists)
        end
    end
end

if redis.call('scard', conj_key) == 0 then
    redis.call('del', conj_key)
    bytes = bytes + string.len(conj_key)
    deleted_sets = deleted_sets + 1
end

return {processed, deleted_items, deleted_sets, errors, bytes}

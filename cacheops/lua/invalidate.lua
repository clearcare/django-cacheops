local db_table = ARGV[1]
-- redis.log(redis.LOG_NOTICE, 'inval1 db_table: ' .. db_table)


local obj = cjson.decode(ARGV[2])
-- redis.log(redis.LOG_NOTICE, 'inval2 obj: ' .. ARGV[2])
local hash_tag = ARGV[3]
-- redis.log(redis.LOG_NOTICE, 'inval3 hash_tag: ' .. hash_tag)
if hash_tag == 'None' then
    hash_tag = nil
end

-- Utility functions
local conj_cache_key = function (db_table, scheme, obj)
    local parts = {}
    for field in string.gmatch(scheme, "[^,]+") do
        table.insert(parts, field .. '=' .. tostring(obj[field]))
    end

    local prefix = 'conj:'
    if hash_tag ~= nil then
        prefix = hash_tag .. prefix
    end

    return prefix .. db_table .. ':' .. table.concat(parts, '&')
end

local call_in_chunks = function (command, args)
    local step = 1000
    for i = 1, #args, step do
        redis.call(command, unpack(args, i, math.min(i + step - 1, #args)))
    end
end


-- Calculate conj keys
local conj_keys = {}

local prefix = 'schemes:'
if hash_tag ~= nil then
    prefix = hash_tag .. prefix
end

-- redis.log(redis.LOG_NOTICE, 'inval4 prefix/db_table: ' .. prefix .. '/' .. db_table)

local schemes = redis.call('smembers',  prefix .. db_table)
for _, scheme in ipairs(schemes) do
    -- redis.log(redis.LOG_NOTICE, 'inval6 :' .. _ .. scheme)

    table.insert(conj_keys, conj_cache_key(db_table, scheme, obj))
end


-- Delete cache keys and refering conj keys
if next(conj_keys) ~= nil then
    local cache_keys = redis.call('sunion', unpack(conj_keys))
    -- redis.log(redis.LOG_NOTICE, 'inval7 conj_keys:' .. tostring(conj_keys))
    -- redis.log(redis.LOG_NOTICE, 'inval8 cache_keys' .. tostring(cache_keys))


    -- we delete cache keys since they are invalid
    -- and conj keys as they will refer only deleted keys
    redis.call('del', unpack(conj_keys))
    if next(cache_keys) ~= nil then
        -- NOTE: can't just do redis.call('del', unpack(...)) cause there is limit on number
        --       of return values in lua.
        call_in_chunks('del', cache_keys)
    end
    return table.getn(cache_keys) + table.getn(conj_keys)
end

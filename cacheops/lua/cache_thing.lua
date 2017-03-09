local key = KEYS[1]
local data = ARGV[1]
local dnfs = cjson.decode(ARGV[2])
local timeout = tonumber(ARGV[3])
local hash_tag = ARGV[4]
if hash_tag == 'None' then
    hash_tag = nil
end
-- redis.log(-- redis.LOG_NOTICE, 'cache1 - key: ' .. key)
-- redis.log(-- redis.LOG_NOTICE, 'cache2 - data: ' .. data)
-- redis.log(-- redis.LOG_NOTICE, 'cache3 - timeout: ' .. timeout)
-- redis.log(-- redis.LOG_NOTICE, 'cache4 - hash_tag: ' .. hash_tag)


-- Write data to cache
redis.call('setex', key, timeout, data)
-- redis.log(-- redis.LOG_NOTICE, 'cache5 ' .. 'setex: ' .. key .. timeout .. data)

-- A pair of funcs
local conj_schema = function (conj)
    -- redis.log(-- redis.LOG_NOTICE, conj)
    local parts = {}
    for _, eq in ipairs(conj) do
        -- redis.log(-- redis.LOG_NOTICE, 'xx', _, eq)
        table.insert(parts, eq[1])
    end

    return table.concat(parts, ',')
end

local conj_cache_key = function (db_table, conj)
    local parts = {}
    for _, eq in ipairs(conj) do
        table.insert(parts, eq[1] .. '=' .. tostring(eq[2]))
    end

    local prefix = 'conj:'
    if hash_tag ~= nil then
        prefix = hash_tag .. prefix
    end

    return prefix .. db_table .. ':' .. table.concat(parts, '&')
end


-- Update schemes and invalidators
for _, disj_pair in ipairs(dnfs) do
    local db_table = disj_pair[1]
    local disj = disj_pair[2]
    for _, conj in ipairs(disj) do
        -- Ensure scheme is known
        --
        -- redis.log(-- redis.LOG_NOTICE, 'cache6 _/conj: ' .. _ .. '/' .. tostring(conj))

        local prefix = 'schemes:'
        if hash_tag ~= nil then
            prefix = hash_tag .. prefix
        end
        -- redis.log(redis.LOG_NOTICE, conj_schema(conj))
        redis.call('sadd', prefix .. db_table, conj_schema(conj))

        -- redis.log(-- redis.LOG_NOTICE, 'cache7 sadd: ' .. prefix .. db_table)

        -- Add new cache_key to list of dependencies
        local conj_key = conj_cache_key(db_table, conj)
        -- redis.log(-- redis.LOG_NOTICE, 'cache8 conj_key: ' .. conj_key)
        redis.call('sadd', conj_key, key)
        -- redis.log(-- redis.LOG_NOTICE, 'cache9 :' .. conj_key .. key)

        -- NOTE: an invalidator should live longer than any key it references.
        --       So we update its ttl on every key if needed.
        -- NOTE: if CACHEOPS_LRU is True when invalidators should be left persistent,
        --       so we strip next section from this script.
        -- TOSTRIP
        local conj_ttl = redis.call('ttl', conj_key)
        if conj_ttl < timeout then
            -- We set conj_key life with a margin over key life to call expire rarer
            -- And add few extra seconds to be extra safe
            redis.call('expire', conj_key, timeout * 2 + 10)
        end
        -- /TOSTRIP
    end
end

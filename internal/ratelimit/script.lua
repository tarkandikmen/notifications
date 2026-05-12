-- rate_limit.lua — atomic per-channel token bucket.
--
-- KEYS[1] = bucket key, e.g. "rate:sms".
-- ARGV[1] = refill rate in tokens/second (e.g. 100).
-- ARGV[2] = burst capacity in tokens (e.g. 100).
-- ARGV[3] = caller-supplied wall clock in milliseconds (worker's
--           time.Now().UnixMilli(); the script does not call TIME so
--           tests can drive a deterministic clock).
--
-- Returns {ok, wait_ms}:
--   {1, 0}        — a token was deducted; caller proceeds.
--   {0, wait_ms}  — no token was available; caller sleeps wait_ms (plus
--                   small jitter, clamped) before retrying.
--
-- Atomic in Redis. Multiple worker instances share one key safely.
-- See docs/ARCHITECTURE.md §6.6.

local key      = KEYS[1]
local rate     = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])

local data    = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens  = tonumber(data[1]) or capacity
local last    = tonumber(data[2]) or now_ms

local elapsed_ms = math.max(0, now_ms - last)
tokens = math.min(capacity, tokens + elapsed_ms * rate / 1000)

if tokens >= 1 then
  tokens = tokens - 1
  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now_ms)
  redis.call('EXPIRE', key, 60)
  return {1, 0}
else
  local wait_ms = math.ceil((1 - tokens) * 1000 / rate)
  redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now_ms)
  redis.call('EXPIRE', key, 60)
  return {0, wait_ms}
end

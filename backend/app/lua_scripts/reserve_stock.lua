--[[
  Atomic Inventory Reservation + Enqueue Lua Script for Redis
  Ensures zero overselling under arbitrary concurrency, and that a granted token
  can never be lost: the stock decrement, the claim record, the ticket status and
  the stream entry all commit together or not at all. Redis runs the whole script
  to completion before any other command, so nothing can interleave inside it.

  KEYS[1]: stock_key       (e.g., "product:1:stock")
  KEYS[2]: claims_key      (hash user_id -> ticket_id, e.g., "product:1:claims")
  KEYS[3]: stream_key      (e.g., "flash_sale_stream")
  KEYS[4]: ticket_key      (e.g., "ticket:tok_abc123")
  KEYS[5]: window_key      (hash with field "start" = sale start, epoch ms; absent = live)
  ARGV[1]: user_id
  ARGV[2]: requested_quantity (usually 1)
  ARGV[3]: ticket_id
  ARGV[4]: product_id
  ARGV[5]: ticket_ttl_seconds

  Returns {status, ticket_id}:
    { 1, new_ticket }       -> Success: stock reserved and order queued
    { 0, "" }               -> Failure: Out of stock (Sold out)
    {-1, existing_ticket }  -> This user already claimed; returns THEIR ticket so a retry
                               after a lost response is idempotent
    {-2, "" }               -> The sale has not started yet (server clock decides)
]]

local stock_key = KEYS[1]
local claims_key = KEYS[2]
local stream_key = KEYS[3]
local ticket_key = KEYS[4]
local window_key = KEYS[5]
local user_id = ARGV[1]
local quantity = tonumber(ARGV[2]) or 1
local ticket_id = ARGV[3]
local product_id = ARGV[4]
local ticket_ttl = tonumber(ARGV[5]) or 7200

-- 0. Sale window: the Redis server clock is the single source of truth for "is it live"
local starts_at = redis.call("HGET", window_key, "start")
if starts_at then
    local t = redis.call("TIME")
    local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
    if now_ms < tonumber(starts_at) then
        return {-2, ""}
    end
end

-- 1. Idempotency: has this user already claimed an item? Hand back their ticket.
local existing = redis.call("HGET", claims_key, user_id)
if existing then
    return {-1, existing}
end

-- 2. Check current stock level
local current_stock = tonumber(redis.call("GET", stock_key))

if not current_stock or current_stock < quantity then
    return {0, ""}
end

-- 3. Atomically decrement stock, record the claim and enqueue the order
redis.call("DECRBY", stock_key, quantity)
redis.call("HSET", claims_key, user_id, ticket_id)
redis.call("XADD", stream_key, "*",
    "ticket_id", ticket_id,
    "product_id", product_id,
    "user_id", user_id)
redis.call("HSET", ticket_key,
    "status", "QUEUED",
    "product_id", product_id,
    "user_id", user_id,
    "message", "Token acquired. Order processing in queue.")
redis.call("EXPIRE", ticket_key, ticket_ttl)

return {1, ticket_id}

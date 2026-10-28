--[[
  Atomic Inventory Reservation + Enqueue Lua Script for Redis
  Ensures zero overselling under arbitrary concurrency, and that a granted token
  can never be lost: the stock decrement, the one-per-user record, the ticket
  status and the stream entry all commit together or not at all.

  KEYS[1]: stock_key       (e.g., "product:1:stock")
  KEYS[2]: users_key       (e.g., "product:1:users")
  KEYS[3]: stream_key      (e.g., "flash_sale_stream")
  KEYS[4]: ticket_key      (e.g., "ticket:tok_abc123")
  ARGV[1]: user_id
  ARGV[2]: requested_quantity (usually 1)
  ARGV[3]: ticket_id
  ARGV[4]: product_id
  ARGV[5]: ticket_ttl_seconds

  Returns:
    1  -> Success: stock reserved and order queued
    0  -> Failure: Out of stock (Sold out)
   -1  -> Failure: User already placed an order (One per customer policy)
]]

local stock_key = KEYS[1]
local users_key = KEYS[2]
local stream_key = KEYS[3]
local ticket_key = KEYS[4]
local user_id = ARGV[1]
local quantity = tonumber(ARGV[2]) or 1
local ticket_id = ARGV[3]
local product_id = ARGV[4]
local ticket_ttl = tonumber(ARGV[5]) or 7200

-- 1. Check idempotency: Has this user already claimed an item?
if redis.call("SISMEMBER", users_key, user_id) == 1 then
    return -1
end

-- 2. Check current stock level
local current_stock = tonumber(redis.call("GET", stock_key))

if not current_stock or current_stock < quantity then
    return 0
end

-- 3. Atomically decrement stock, record the reservation and enqueue the order
redis.call("DECRBY", stock_key, quantity)
redis.call("SADD", users_key, user_id)
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

return 1

-- Copyright (C) Dejiang Zhu(doujiang24)


local buffer = require "resty.kafka.buffer"
local producer = require "resty.kafka.producer"
local client = require "resty.kafka.client"


local setmetatable = setmetatable
local timer_at = ngx.timer.at
local ngx_log = ngx.log
local DEBUG = ngx.DEBUG
local ERR = ngx.ERR
--local debug = ngx.config.debug
local debug = true
local is_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 4)
_M._VERSION = '0.01'


local cluster_inited = {}


local mt = { __index = _M }


local function _flush_lock(self)
    if not self.flushing then
        if debug then
            ngx_log(DEBUG, "flush lock accquired")
        end
        self.flushing = true
        return true
    end
    return false
end


local function _flush_unlock(self)
    if debug then
        ngx_log(DEBUG, "flush lock released")
    end
    self.flushing = false
end


local function _flush(premature, self, force)
    if not _flush_lock(self) then
        if debug then
            ngx_log(DEBUG, "previous flush not finished")
        end

        if not force then
            return true
        end

        repeat
            if debug then
                ngx_log(DEBUG, "last flush required lock")
            end
            ngx_sleep(0.1)
        until _flush_lock(self)
    end

    local send_num = 0
    for topic, buffers in pairs(self.buffers) do
        for partition_id, buffer in pairs(buffers) do
            if force or buffer:need_flush() then
                -- get queue
                local queue, index = buffer:flush()

                -- batch send queue
                if index > 0 then
                    local offset, err = self.producer:batch_send(topic, partition_id, queue, index)
                    if offset then
                        send_num = send_num + (index / 2)
                    else
                        if self.error_handle then
                            self.error_handle(topic, partition_id, queue, index, err)
                        else
                            ngx_log(ERR, "buffered messages send to kafka err: ", err,
                                            "; message num: ", index / 2)
                        end
                    end
                end
            end
        end
    end

    _flush_unlock(self)
    return send_num
end


local function _flush_buffer(self, force)
    local ok, err = timer_at(0, _flush, self, force)
    if not ok then
        ngx_log(ERR, "failed to create timer at _flush_buffer, err: ", err)
    end
end


local _timer_flush
_timer_flush = function (premature, self, time)
    _flush(nil, self, true)

    if is_exiting() then
        return _flush(nil, self, true)
    end

    local ok, err = timer_at(time, _timer_flush, self, time)
    if not ok then
        ngx_log(ERR, "failed to create timer at _timer_flush, err: ", err)
    end
end


function _M.new(self, cluster_name, broker_list, client_config,
                producer_config, buffer_config)

    local cluster_name = cluster_name or "default"
    local bp = cluster_inited[cluster_name]
    if bp then
        return bp
    end

    local p = producer:new(broker_list, client_config, producer_config)

    local opts = buffer_config or {}
    local buffer_opts = {
        flush_size = opts.flush_size or 1024,  -- 1KB
        max_size = opts.max_size or 1048576,    -- 1MB
        flush_time = opts.flush_time or 1000,   -- 1s
    }

    local bp = setmetatable({
                producer = p,
                buffer_opts = buffer_opts,
                buffers = {},
                error_handle = opts.error_handle,
            }, mt)

    cluster_inited[cluster_name] = bp
    _timer_flush(nil, bp, buffer_opts.flush_time / 1000)
    return bp
end


function _M.send(self, topic, key, message)
    local partition_id, err = self.producer:choose_partition(topic, key)
    if not partition_id then
        return nil, err
    end

    local buffers = self.buffers
    if not buffers[topic] then
        buffers[topic] = {}
    end
    if not buffers[topic][partition_id] then
        buffers[topic][partition_id] = buffer:new(self.buffer_opts)
    end

    local buffer = buffers[topic][partition_id]
    local size, err = buffer:add(key, message)
    if not size then
        return nil, err
    end

    local force = is_exiting()
    if force or buffer:need_flush() then
        _flush_buffer(self, force)
    end

    return size
end


function _M.flush(self)
    return _flush(nil, self, true)
end


return _M

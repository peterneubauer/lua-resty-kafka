
-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"


local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 3)
_M._VERSION = '0.01'


local mt = { __index = _M }


function _M.new(self, host, port, socket_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
    }, mt)
end


function _M.send_receive(self, request)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, err
    end

    sock:settimeout(self.config.socket_timeout)

    local bytes, err = sock:send(request:package())
    if not bytes then
        return nil, err
    end

    local data, err = sock:receive(4)
    if not data then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    local len = to_int32(data)

    local data, err = sock:receive(len)
    if not data then
        if err == "timeout" then
            sock:close()
        end
        return nil, err
    end

    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)

    return response:new(data)
end


return _M

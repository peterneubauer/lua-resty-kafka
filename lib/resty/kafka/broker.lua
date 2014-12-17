
-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"
local resolver = require "resty.dns.resolver"
local r, err = resolver:new{
    nameservers = {"8.8.8.8", {"8.8.4.4", 53} },
    retrans = 5,  -- 5 retransmissions on receive timeout
    timeout = 2000,  -- 2 sec
}
if not r then
    ngx.say("failed to instantiate the resolver: ", err)
    return
end


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
    local answers, err = r:query(self.host)
    if not answers then
        ngx.say("failed to query the DNS server: ", err)
        return
    end
--    ngx.log(ngx.ERR, answers)
    for i, ans in ipairs(answers) do
        ngx.say(ans.name, " ", ans.address or ans.cname,
            " type:", ans.type, " class:", ans.class,
            " ttl:", ans.ttl)
    end
    ngx.log(ngx.ERR, 'talking to host ' .. answers[0].address)
    local ok, err = sock:connect(answers[0].address, self.port)
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

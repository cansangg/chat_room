#pragma once
#include <hiredis/hiredis.h>
#include <string>
#include <stdexcept>
#include <iostream>

class RedisClient {
private:
    redisContext* context = nullptr;

public:
    RedisClient() {}
    
    ~RedisClient() {
        if (context) redisFree(context);
    }

    // 1. 连接 Redis
    void connect(const std::string& host = "127.0.0.1", int port = 6379) {
        context = redisConnect(host.c_str(), port);
        if (context == nullptr || context->err) {
            std::string err_msg = context ? context->errstr : "";
            if (context) { redisFree(context); context = nullptr; }
            throw std::runtime_error("Redis 连接失败: " + err_msg);
        }
    }

    // 2. 存入数据 (带过期时间，默认 60 秒)
    void set(const std::string& key, const std::string& value, int expire_seconds = 60) {
        redisReply* reply = (redisReply*)redisCommand(context, "SETEX %s %d %s", 
                                                      key.c_str(), expire_seconds, value.c_str());
        if (reply == nullptr) throw std::runtime_error("Redis SET 指令执行失败");
        freeReplyObject(reply);
    }

    // 3. 读取数据
    std::string get(const std::string& key) {
        redisReply* reply = (redisReply*)redisCommand(context, "GET %s", key.c_str());
        if (reply == nullptr) throw std::runtime_error("Redis GET 指令执行失败");
        
        std::string result = "";
        // REDIS_REPLY_STRING 表示找到了数据，REDIS_REPLY_NIL 表示数据不存在或已过期
        if (reply->type == REDIS_REPLY_STRING) {
            result = reply->str;
        }
        freeReplyObject(reply);
        return result;
    }

    // 4. 删除数据 (当有新消息时，用来清除旧缓存)
    void del(const std::string& key) {
        redisReply* reply = (redisReply*)redisCommand(context, "DEL %s", key.c_str());
        if (reply) freeReplyObject(reply);
    }
};
#include <fstream>
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <sstream>

#include "EPoller.hpp"
#include "Mysql.hpp"
#include "Redis.hpp"
#include "Json.hpp"

using json = nlohmann::json;

// ==============================================================================
// 🌟 工业级事务重试引擎：精准拦截断网与死锁
// ==============================================================================
void process(const std::function<void(MysqlClient&)>& f, MysqlClient& db) {
    while (true) {
        try {
            db.begin();
            f(db);
            db.commit();
            return; 
        } catch (const std::exception& e) {
            std::string err = e.what();
            
            if (err.find("connection_broken") != std::string::npos) {
                db.reconnect(); //我永在
                continue; 
            } 
            else if (err.find("Deadlock") != std::string::npos) {
                db.rollback(); //连不断
                continue; 
            } else {
                db.rollback(); //主动回滚
                throw std::runtime_error(err); 
            }
        }
    }
}
// ==============================================================================


int main() {
    Sock server;
    server.bindAndListen(8080);
    EPoller epoller;
    epoller.setListener(std::move(server));

    // 1. 初始化 MySQL 全局环境
    if (mysql_library_init(0, nullptr, nullptr) != 0) {
        std::cerr << "致命错误：MySQL 全局库初始化失败！" << std::endl;
        return -1;
    }

    std::string http_response;
    std::ifstream file("index.html");
    if (file.is_open()) {
        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();
        
        http_response = "HTTP/1.1 200 OK\r\n"
                        "Content-Type: text/html; charset=utf-8\r\n"
                        "Connection: close\r\n"
                        "Content-Length: " + std::to_string(content.size()) + "\r\n\r\n" + 
                        content;
    } else {
        std::string not_found = "<h1>404 Not Found</h1>";
        http_response = "HTTP/1.1 404 Not Found\r\n"
                        "Content-Type: text/html; charset=utf-8\r\n"
                        "Connection: close\r\n"
                        "Content-Length: " + std::to_string(not_found.size()) + "\r\n\r\n" + 
                        not_found;
    }

    for (int i = 1; i <= 16; i++) std::thread([&]() -> void {
        try {
            MysqlClient db;
            db.connect("127.0.0.1", "root", "Ltm66666@", "chat_db");

            RedisClient redis;
            try { 
                redis.connect("127.0.0.1", 6379); 
            } catch (const std::exception& e) {
                std::cerr << "警告: Redis连接失败，系统将降级无缓存运行! (" << e.what() << ")" << std::endl;
            }
            
            // 自动初始化建表
            try {
                process([&](MysqlClient& db_ref) {
                    std::string create_users_table = R"(CREATE TABLE IF NOT EXISTS `users` (`user_id` INT AUTO_INCREMENT PRIMARY KEY, `user_name` VARCHAR(50) NOT NULL UNIQUE, `user_password` VARCHAR(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;)";
                    db_ref.execute(create_users_table);

                    std::string create_rooms_table = R"(CREATE TABLE IF NOT EXISTS `rooms` (`room_id` INT AUTO_INCREMENT PRIMARY KEY, `room_name` VARCHAR(100) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;)";
                    db_ref.execute(create_rooms_table);

                    std::string create_room_members_table = R"(CREATE TABLE IF NOT EXISTS `room_members` (`room_id` INT NOT NULL, `user_id` INT NOT NULL, PRIMARY KEY (`room_id`, `user_id`), KEY `idx_user_id` (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;)";
                    db_ref.execute(create_room_members_table);

                    std::string create_message_table = R"(CREATE TABLE IF NOT EXISTS `messages` (`message_id` BIGINT AUTO_INCREMENT PRIMARY KEY, `message_roomid` INT NOT NULL, `message_userid` INT NOT NULL, `content` TEXT NOT NULL, `time_stamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, INDEX `idx_room` (`message_roomid`, `time_stamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;)";
                    db_ref.execute(create_message_table);
                }, db);
            } catch (const std::exception& e) {
                throw std::runtime_error(std::string("数据库建表失败: ") + e.what());
            }

            // 🌟 Worker 线程主循环
            while (true) {
                // 🌟 核心修改：三行合一。内部自带阻塞挂起与唤醒机制，完美替代原先的 tasks_num.acquire() 和 front/pop
                Task task = task_queue.get_pop();

                Result res;
                res.fd = task.fd;
                res.user = task.user;
                res.gen = task.gen;
                res.type = 0; 
            
                try { 
                    if (task.req.find("GET") == 0 && task.req.find("Upgrade: websocket") != std::string::npos) {
                        std::string accept_key = getWsAcceptKey(task.req);
                        if (accept_key.empty()) throw std::runtime_error("Invalid WebSocket Key");

                        res.res = "HTTP/1.1 101 Switching Protocols\r\n"
                                  "Upgrade: websocket\r\n"
                                  "Connection: Upgrade\r\n"
                                  "Sec-WebSocket-Accept: " + accept_key + "\r\n\r\n";
                        res.type = 3; 
                    } 
                    else if (task.req.find("GET") == 0) {
                        res.res = http_response;
                        res.type = 0; 
                    }
                    else {
                        json j_req = json::parse(task.req);
                        std::string action = j_req.value("action", "");
                        std::string req_id = j_req.value("req_id", "");
                        
                        json j_res;
                        j_res["req_id"] = req_id;
                        j_res["action"] = action;
                        j_res["status"] = "success";

                        if (action == "ping") {
                            j_res["message"] = "pong";
                        } 
                        else if (action == "login/register/logout") {
                            std::string name = j_req["data"]["name"];
                            std::string pwd = j_req["data"]["password"];
                            
                            process([&](MysqlClient& db_ref) {
                                auto rows = db_ref.query("SELECT user_id, user_password FROM users WHERE user_name = ?", {name});
                                if (rows.empty()) {
                                    db_ref.execute("INSERT INTO users (user_name, user_password) VALUES (?, ?)", {name, pwd});
                                    auto new_user = db_ref.query("SELECT LAST_INSERT_ID() AS user_id");
                                    res.user = std::stoi(new_user[0].at("user_id"));
                                    res.type = 1; 
                                    j_res["message"] = "注册并登录成功";
                                } else {
                                    if (rows[0].at("user_password") == pwd) {
                                        res.user = std::stoi(rows[0].at("user_id"));
                                        res.type = 1; 
                                        j_res["message"] = "登录成功";
                                    } else {
                                        j_res["status"] = "fail";
                                        j_res["message"] = "密码错误";
                                    }
                                }
                            }, db);
                        } 
                        else if (action == "get_rooms") {
                            if (task.user == -1) throw std::runtime_error("未登录");
                            
                            process([&](MysqlClient& db_ref) {
                                std::string sql = "SELECT r.room_id AS id, r.room_name AS name FROM rooms r "
                                                  "JOIN room_members rm ON r.room_id = rm.room_id "
                                                  "WHERE rm.user_id = ?";
                                auto rows = db_ref.query(sql, {std::to_string(task.user)});
                                j_res["data"]["rooms"] = rows;
                            }, db);
                        } 
                        else if (action == "pull_room_history") {
                            if (task.user == -1) throw std::runtime_error("未登录");
                            
                            std::string room_id_str = j_req["data"]["room_id"].is_string() ? 
                                                      j_req["data"]["room_id"].get<std::string>() : 
                                                      std::to_string(j_req["data"]["room_id"].get<int>());
                            
                            std::string cache_key = "room_history_" + room_id_str;
                            std::string cached_data;
                            
                            try { cached_data = redis.get(cache_key); } catch(...) {} 

                            if (!cached_data.empty()) {
                                j_res["data"]["messages"] = json::parse(cached_data);
                            } else {
                                process([&](MysqlClient& db_ref) {
                                    std::string sql = "SELECT u.user_name AS sender_name, m.content FROM messages m JOIN users u ON m.message_userid = u.user_id WHERE m.message_roomid = ? ORDER BY m.message_id DESC LIMIT 50";
                                    auto rows = db_ref.query(sql, {room_id_str});
                                    j_res["data"]["messages"] = rows;
                                }, db);

                                try { redis.set(cache_key, j_res["data"]["messages"].dump(), 60); } catch(...) {}
                            }
                        } 
                        else if (action == "send_msg") {
                            if (task.user == -1) throw std::runtime_error("未登录");
                            
                            int room_id = j_req["data"]["room_id"].is_string() ? 
                                          std::stoi(j_req["data"]["room_id"].get<std::string>()) : 
                                          j_req["data"]["room_id"].get<int>();
                            std::string content = j_req["data"]["content"];
                            
                            std::vector<int> target_users;
                            std::string sender_name;

                            process([&](MysqlClient& db_ref) {
                                target_users.clear(); 
                                db_ref.execute("INSERT INTO messages (message_roomid, message_userid, content) VALUES (?, ?, ?)", 
                                                {std::to_string(room_id), std::to_string(task.user), content});
                                j_res["message"] = "发送成功";

                                auto sender_rows = db_ref.query("SELECT user_name FROM users WHERE user_id = ?", {std::to_string(task.user)});
                                sender_name = sender_rows[0].at("user_name");

                                auto members = db_ref.query("SELECT user_id FROM room_members WHERE room_id = ?", {std::to_string(room_id)});
                                for (const auto& m : members) {
                                    int uid = std::stoi(m.at("user_id"));
                                    if (uid != task.user) target_users.push_back(uid);
                                }
                            }, db);

                            try { redis.del("room_history_" + std::to_string(room_id)); } catch(...) {}

                            if (!target_users.empty()) {
                                Result broadcast_res;
                                broadcast_res.fd = -1;       
                                broadcast_res.user = -1;
                                broadcast_res.gen = -1;
                                broadcast_res.type = 4;      
                                broadcast_res.to = std::move(target_users);
                                
                                json j_broadcast;
                                j_broadcast["action"] = "receive_msg";
                                j_broadcast["data"] = {
                                    {"room_id", room_id},
                                    {"sender_name", sender_name},
                                    {"content", content}
                                };
                                broadcast_res.res = j_broadcast.dump();
                                result_queue.push(broadcast_res);
                            }
                        } 
                        else if (action == "create_room") {
                            if (task.user == -1) throw std::runtime_error("未登录");
                            std::string room_name = j_req["data"]["room_name"];
                            auto members = j_req["data"]["members"].get<std::vector<std::string>>();
                            
                            std::vector<int> target_users;
                            int room_id_int = 0;

                            process([&](MysqlClient& db_ref) {
                                target_users.clear(); 
                                db_ref.execute("INSERT INTO rooms (room_name) VALUES (?)", {room_name});
                                auto rows = db_ref.query("SELECT LAST_INSERT_ID() AS id");
                                std::string room_id_str = rows[0].at("id");
                                room_id_int = std::stoi(room_id_str);
                                
                                for (const auto& m_name : members) {
                                    auto u_rows = db_ref.query("SELECT user_id FROM users WHERE user_name = ?", {m_name});
                                    if (!u_rows.empty()) {
                                        int uid = std::stoi(u_rows[0].at("user_id"));
                                        db_ref.execute("INSERT INTO room_members (room_id, user_id) VALUES (?, ?)", 
                                                        {room_id_str, std::to_string(uid)});
                                        if (uid != task.user) target_users.push_back(uid);
                                    } else {
                                        throw std::runtime_error("建群失败：找不到用户 [" + m_name + "]");
                                    }
                                }
                                j_res["data"]["room_id"] = room_id_int;
                            }, db);

                            if (!target_users.empty()) {
                                Result broadcast_res;
                                broadcast_res.fd = -1;       
                                broadcast_res.user = -1;
                                broadcast_res.gen = -1;
                                broadcast_res.type = 4;      
                                broadcast_res.to = std::move(target_users);
                                
                                json j_broadcast;
                                j_broadcast["action"] = "receive_room";
                                j_broadcast["data"] = {
                                    {"room_id", room_id_int},
                                    {"room_name", room_name}
                                };
                                broadcast_res.res = j_broadcast.dump();
                                result_queue.push(broadcast_res);
                            }
                        }
                        else {
                            throw std::runtime_error("未知操作指令: " + action);
                        }
                        
                        res.res = j_res.dump();
                    }
                } catch (const std::exception& e) {
                    json err;
                    err["status"] = "error";
                    err["message"] = e.what();
                    res.res = err.dump();
                    res.type = 0;
                }

                result_queue.push(res);
                Notice(notice_fd);
            }
        } catch (const std::exception& e) {
            std::cerr << "Worker 线程启动失败或崩溃: " << e.what() << std::endl;
        }
    }).detach();

    // 心跳线程
    std::thread([&]() -> void {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5 * 1000));
            result_queue.push(Result{-1, -1, -1, "", 5, {}}); 
            Notice(notice_fd);
        }
    }).detach();

    std::cout << "Server is running..." << '\n';
    epoller.poll(-1);
    mysql_library_end(); 
    return 0;
}

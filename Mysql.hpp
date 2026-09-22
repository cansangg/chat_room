#pragma once
#include <mysql/mysql.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <stdexcept>
#include <iostream>

class MysqlClient {
private:
    MYSQL* conn; // MySQL 原生连接句柄
    
    // 🌟 新增：记住连接凭证，断网重连时必须靠它们满血复活
    std::string host_, user_, passwd_, db_;
    unsigned int port_;

    std::string formatSafeSql(std::string sql, const std::vector<std::string>& params) {
        size_t pos = 0;
        for (const auto& param : params) {
            pos = sql.find("?", pos);
            if (pos == std::string::npos) {
                throw std::runtime_error("SQL 拼接失败: 问号数量比参数少！\nSQL: " + sql);
            }
            
            std::vector<char> buffer(param.length() * 2 + 1);
            mysql_real_escape_string(conn, buffer.data(), param.c_str(), param.length());
            std::string safe_param(buffer.data()); 
            
            std::string replacement = "'" + safe_param + "'";
            sql.replace(pos, 1, replacement);
            pos += replacement.length(); 
        }
        
        if (sql.find("?", pos) != std::string::npos) {
            throw std::runtime_error("SQL 拼接失败: 问号数量比参数多！\nSQL: " + sql);
        }
        
        return sql;
    }

public:
    // 1. RAII 构造与析构
    MysqlClient() {
        conn = mysql_init(nullptr);
        if (conn == nullptr) {
            throw std::runtime_error("MySQL 初始化失败: 内存不足");
        }
    }

    ~MysqlClient() {
        if (conn != nullptr) {
            mysql_close(conn);
        }
    }

    MysqlClient(const MysqlClient&) = delete;
    MysqlClient& operator=(const MysqlClient&) = delete;

    // 2. 智能连接数据库
    void connect(const std::string& host, const std::string& user, 
                 const std::string& passwd, const std::string& db, unsigned int port = 3306) {
        
        // 🌟 新增：把参数存到类成员里，留着重连用
        host_ = host;
        user_ = user;
        passwd_ = passwd;
        db_ = db;
        port_ = port;

        if (mysql_real_connect(conn, host.c_str(), user.c_str(), passwd.c_str(), 
                                nullptr, port, nullptr, 0) == nullptr) {
            throw std::runtime_error("MySQL 服务器连接失败: " + std::string(mysql_error(conn)));
        }
        
        mysql_set_character_set(conn, "utf8mb4");

        if (!db.empty()) {
            std::string create_db_sql = "CREATE DATABASE IF NOT EXISTS `" + db + "` DEFAULT CHARACTER SET utf8mb4";
            if (mysql_query(conn, create_db_sql.c_str()) != 0) {
                throw std::runtime_error("自动创建数据库失败: " + std::string(mysql_error(conn)));
            }
            if (mysql_select_db(conn, db.c_str()) != 0) {
                throw std::runtime_error("切换到指定数据库失败: " + std::string(mysql_error(conn)));
            }
        }
    }

    // 🌟 新增：最纯粹暴力的断网重连技能
    void reconnect() {
        // 1. 无情销毁旧的废弃句柄（释放底层的僵尸 socket）
        if (conn != nullptr) {
            mysql_close(conn);
        }

        // 2. 重新向内核申请一个崭新的原生句柄
        conn = mysql_init(nullptr);
        if (conn == nullptr) {
            throw std::runtime_error("重新初始化失败: 内存不足");
        }

        bool reconnect_flag = true;
        mysql_options(conn, MYSQL_OPT_RECONNECT, &reconnect_flag);

        // 3. 用当初保存下来的凭证重新登录。
        // 注意：这里我们直接把 db_ 传进去了，因为第一次 connect 时库肯定已经建好了
        if (mysql_real_connect(conn, host_.c_str(), user_.c_str(), passwd_.c_str(), 
                               db_.empty() ? nullptr : db_.c_str(), port_, nullptr, 0) == nullptr) {
            throw std::runtime_error("重新连接失败: " + std::string(mysql_error(conn)));
        }
        
        mysql_set_character_set(conn, "utf8mb4");
        
        std::cout << "[MysqlClient] 网络连接已成功恢复！" << std::endl;
    }

    void begin() {
        if (mysql_autocommit(conn, 0) != 0) {
            throw std::runtime_error("connection_broken");
        }
    }

    void commit() {
        if (mysql_commit(conn) != 0) {
            throw std::runtime_error("connection_broken");
        }
        mysql_autocommit(conn, 1); 
    }

    void rollback() {
        if (mysql_rollback(conn) != 0) {
            throw std::runtime_error("connection_broken");
        }
        mysql_autocommit(conn, 1); 
    }

    int execute(std::string sql, const std::vector<std::string>& params = {}) {
        size_t pos = 0;
        for (const auto& param : params) {
            pos = sql.find("?", pos);
            if (pos == std::string::npos) {
                throw std::runtime_error("SQL 执行失败: 问号数量比参数少！\nSQL: " + sql);
            }
            
            std::vector<char> buffer(param.length() * 2 + 1);
            mysql_real_escape_string(conn, buffer.data(), param.c_str(), param.length());
            std::string safe_param(buffer.data());
            
            std::string replacement = "'" + safe_param + "'";
            sql.replace(pos, 1, replacement);
            pos += replacement.length(); 
        }
        
        if (sql.find("?", pos) != std::string::npos) {
            throw std::runtime_error("SQL 执行失败: 问号数量比参数多！\nSQL: " + sql);
        }

        if (mysql_query(conn, sql.c_str()) != 0) {
            unsigned int err_no = mysql_errno(conn);
            if (err_no == 2006 || err_no == 2013) {
                throw std::runtime_error("connection_broken");
            }
            throw std::runtime_error("执行 SQL 失败: " + std::string(mysql_error(conn)) + "\nSQL: " + sql);
        }
        return mysql_affected_rows(conn);
    }

    std::vector<std::unordered_map<std::string, std::string>> query(std::string sql, const std::vector<std::string>& params = {}) {
        size_t pos = 0;
        for (const auto& param : params) {
            pos = sql.find("?", pos);
            if (pos == std::string::npos) {
                throw std::runtime_error("SQL 查询失败: 问号数量比参数少！\nSQL: " + sql);
            }
            
            std::vector<char> buffer(param.length() * 2 + 1);
            mysql_real_escape_string(conn, buffer.data(), param.c_str(), param.length());
            std::string safe_param(buffer.data());
            
            std::string replacement = "'" + safe_param + "'";
            sql.replace(pos, 1, replacement);
            pos += replacement.length(); 
        }
        
        if (sql.find("?", pos) != std::string::npos) {
            throw std::runtime_error("SQL 查询失败: 问号数量比参数多！\nSQL: " + sql);
        }

        if (mysql_query(conn, sql.c_str()) != 0) {
            unsigned int err_no = mysql_errno(conn);
            if (err_no == 2006 || err_no == 2013) {
                throw std::runtime_error("connection_broken");
            }
            throw std::runtime_error("查询 SQL 失败: " + std::string(mysql_error(conn)) + "\nSQL: " + sql);
        }

        MYSQL_RES* result = mysql_store_result(conn);
        if (result == nullptr) {
            if (mysql_field_count(conn) == 0) return {}; 
            
            unsigned int err_no = mysql_errno(conn);
            if (err_no == 2006 || err_no == 2013) {
                throw std::runtime_error("connection_broken");
            }
            throw std::runtime_error("获取结果集失败: " + std::string(mysql_error(conn)));
        }

        int num_fields = mysql_num_fields(result);
        MYSQL_FIELD* fields = mysql_fetch_fields(result);
        std::vector<std::string> field_names(num_fields);
        for (int i = 0; i < num_fields; ++i) {
            field_names[i] = fields[i].name;
        }

        std::vector<std::unordered_map<std::string, std::string>> rows;
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result)) != nullptr) {
            std::unordered_map<std::string, std::string> row_map;
            for (int i = 0; i < num_fields; ++i) {
                row_map[field_names[i]] = row[i] ? row[i] : "NULL";
            }
            rows.push_back(std::move(row_map));
        }

        mysql_free_result(result);
        return rows;
    }
};
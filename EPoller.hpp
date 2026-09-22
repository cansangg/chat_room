#pragma once
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <tuple>
#include <string>
#include <chrono>
#include <algorithm>
#include "Sock.hpp"
#include "Queue.hpp"
#include "Utility.hpp"

struct Task {
    int fd, user, gen;
    std::string req;
};

struct Result {
    int fd, user, gen;
    std::string res;
    int type; // 0: 普通请求 1：登录 2：登出 3：协议升级 4：广播 5：check live
    std::vector<int> to;
};

// 🌟 全局队列初始化（指定容量 65536，废弃旧的 tasks_num 独立信号量）
inline Queue<Task> task_queue(65536);
inline Queue<Result> result_queue(65536); 
inline int notice_fd;

class EPoller {
private:
    int epfd;
public:
    Sock listener;
    std::unordered_map<int, Sock> connected_sockets; 
    int notice_fd;

    std::unordered_map<int, std::chrono::time_point<std::chrono::system_clock>> last_active_time;
    std::unordered_map<int, bool> is_websocket, read_wait, write_wait;
    std::unordered_map<int, int> fd_to_user, user_to_fd, fd_gen; 

    std::vector<int> to_remove;

public: 
    EPoller() : epfd(epoll_create1(0)), listener(-1), notice_fd(eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) { 
        SetMonitoring(notice_fd, EPOLLIN);
        ::notice_fd = notice_fd;
    }
    EPoller(const EPoller&) = delete;
    EPoller& operator=(const EPoller&) = delete;
    EPoller(EPoller&&) = delete;
    EPoller& operator=(EPoller&&) = delete;
    ~EPoller() { 
        close(epfd);
        close(notice_fd); 
    }

    void setListener(Sock&& _listener) {
        listener = std::move(_listener); 
        SetMonitoring(listener.fd, EPOLLIN);
    }

    void addSocket(Sock&& client) {
        int fd = client.fd;
        connected_sockets.emplace(fd, std::move(client));
        last_active_time[fd] = std::chrono::system_clock::now();
        is_websocket[fd] = false;
        SetMonitoring(fd, EPOLLIN);
        fd_to_user[fd] = -1;
        fd_gen[fd]++;
    }

    void SetMonitoring(int targetfd, uint32_t state) {  
        struct epoll_event ev;
        ev.events = state; 
        ev.data.fd = targetfd;
        if (epoll_ctl(epfd, EPOLL_CTL_MOD, targetfd, &ev) == -1) {
            epoll_ctl(epfd, EPOLL_CTL_ADD, targetfd, &ev);
        }
        read_wait[targetfd] = state & EPOLLIN;
        write_wait[targetfd] = state & EPOLLOUT;
    }

    void ToggleMonitoring(int targetfd, uint32_t option) {
        uint32_t current_state = 0;
        if (read_wait[targetfd]) current_state |= EPOLLIN;
        if (write_wait[targetfd]) current_state |= EPOLLOUT;
        current_state ^= option;
        SetMonitoring(targetfd, current_state);
    }

    void poll(int timeout_ms) {
        while (true) {
            const int MAX_EVENTS = 1024;
            struct epoll_event events[MAX_EVENTS];
            int n = epoll_wait(epfd, events, MAX_EVENTS, timeout_ms);

            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                uint32_t ev = events[i].events;

                if (fd == notice_fd) { 
                    Reset(notice_fd);
                    
                    Result tmp_res;
                    // 🌟 核心修改：使用非阻塞的 try_pop 循环清空队列，防止 Reactor 线程被卡死
                    while (result_queue.try_pop(tmp_res)) {
                        auto [_fd, user, gen, res, type, to] = tmp_res; 

                        if (type == 4) {
                            for (int _user : to) {
                                if (!user_to_fd.count(_user)) continue;
                                int _fd_ = user_to_fd[_user];
                                ssize_t rem = is_websocket[_fd_] ? connected_sockets[_fd_].appendAndSend_Ws(res) : connected_sockets[_fd_].appendAndSend(res);
                                if (rem > 0 && !write_wait[_fd_]) ToggleMonitoring(_fd_, EPOLLOUT);
                            }
                            continue;
                        }

                        if (type == 5) {
                            auto now = std::chrono::system_clock::now();
                            for (auto [_fd_, tm] : last_active_time) {
                                if (now - tm >= std::chrono::milliseconds(15000)) {
                                    if (std::find(to_remove.begin(), to_remove.end(), _fd_) == to_remove.end()) {
                                        to_remove.push_back(_fd_);
                                    }
                                }
                            }
                            continue;
                        }

                        if (gen != fd_gen[_fd]) continue;

                        ToggleMonitoring(_fd, EPOLLIN); 
                        
                        ssize_t rem = is_websocket[_fd] ? connected_sockets[_fd].appendAndSend_Ws(res) : connected_sockets[_fd].appendAndSend(res);
                        if (rem > 0 && !write_wait[_fd]) ToggleMonitoring(_fd, EPOLLOUT);

                        switch (type) {
                            case 0:
                                break;
                            case 1: 
                                fd_to_user[_fd] = user;
                                user_to_fd[user] = _fd;
                                fd_gen[_fd]++;
                                break;
                            case 2:
                                fd_to_user[_fd] = -1;
                                user_to_fd.erase(user);
                                fd_gen[_fd]++;
                                break;
                            case 3:
                                is_websocket[_fd] = true;
                                break;
                        }
                    }
                } else if (fd == listener.fd) { 
                    addSocket(listener.acceptClient());
                } else { 
                    if (ev & EPOLLIN) {
                        last_active_time[fd] = std::chrono::system_clock::now();

                        int sta;
                        std::string req;
                        if (is_websocket[fd]) {
                            auto [x, y] = connected_sockets[fd].readRequest_Ws();
                            sta = x, req = y;
                        } else {
                            auto [x, y, z] = connected_sockets[fd].readHttpRequest();
                            sta = x, req = y + z;
                        }
                        
                        if (sta == 0) break; 
                        if (sta == -1) {
                            to_remove.push_back(fd);
                            break; 
                        }
                        
                        ToggleMonitoring(fd, EPOLLIN);
                        
                        // 🌟 核心修改：直接 push，底层会自动通过信号量唤醒 Worker 线程
                        task_queue.push(Task{fd, fd_to_user[fd], fd_gen[fd], req});
                    }
                    if (ev & EPOLLOUT) {
                        ssize_t rem = connected_sockets[fd].send_to_slot();
                        if (rem == 0) ToggleMonitoring(fd, EPOLLOUT);
                    }
                }
            }

            for (int fd : to_remove) {
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL); 
                last_active_time.erase(fd);
                read_wait.erase(fd), write_wait.erase(fd);
                is_websocket.erase(fd);
                if (fd_to_user.count(fd)) user_to_fd.erase(fd_to_user[fd]);
                fd_to_user.erase(fd);
                fd_gen[fd]++;
                connected_sockets.erase(fd); 
            }
            to_remove.clear();
        }
    }
};
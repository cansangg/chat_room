#include <iostream>
#include <string>
#include <cstring>
#include <stdexcept>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <cerrno>
#include <optional>
#include <functional>
#include <netinet/tcp.h>
#include <tuple>

/*
    bindAndListen()
    connectTo() -> acceptClient()
    
    readHttpRequest() / readExactly() / readUntil()
    appendAndSend()
    
    recv_from_slot()
    send_to_slot()
*/



class TcpSocket { 
public:
    std::string in_buffer;
    std::string out_buffer; 
    int fd;

    TcpSocket(int _fd = 0) {
        if (_fd != 0) fd = _fd;
        else fd = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    }

    virtual ~TcpSocket() { if (fd != -1) ::close(fd); }

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;
    
    TcpSocket(TcpSocket&& other) noexcept 
        : fd(other.fd), 
        in_buffer(std::move(other.in_buffer)),
        out_buffer(std::move(other.out_buffer))
        { other.fd = -1; }

    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this == &other) return *this;
        ::close(fd);
        fd = other.fd;
        other.fd = -1;
        in_buffer = std::move(other.in_buffer);
        out_buffer = std::move(other.out_buffer);
        return *this;
    }

    void bindAndListen(int port, int backlog = 128) {
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        if (::bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) throw std::runtime_error("Bind 失败!");
        if (::listen(fd, backlog) < 0) throw std::runtime_error("Listen 失败!");
    }

    void connectTo(const std::string& host, int port) {
        hostent* he = gethostbyname(host.c_str());
        if (!he) throw std::runtime_error("域名解析失败: " + host);
        sockaddr_in addr = {};
        addr.sin_family = AF_INET, addr.sin_port = htons(port), addr.sin_addr = *(in_addr*)he->h_addr_list[0];
        int ret = ::connect(fd, (sockaddr*)&addr, sizeof(addr));
        if (ret == 0) return;
        int flags = fcntl(fd, F_GETFL, 0);
        bool is_nonblock = (flags != -1 && (flags & O_NONBLOCK));
        if (is_nonblock) return;
        throw std::runtime_error("connect: " + std::string(strerror(errno)));
    }

    TcpSocket acceptClient() {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(fd, (struct sockaddr*)&client_addr, &len);
        return TcpSocket(client_fd);
    }

    void setBlocking(bool blocking) {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags == -1) throw std::runtime_error("fcntl F_GETFL 失败");
        if (!blocking) flags |= O_NONBLOCK;  
        else flags &= ~O_NONBLOCK; 
        if (fcntl(fd, F_SETFL, flags) == -1) throw std::runtime_error("fcntl F_SETFL 失败");
    }

    //private:
    ssize_t recv_from_slot() {
        char tmp_buf[4096];
        ssize_t bytes_read = ::recv(fd, tmp_buf, sizeof(tmp_buf), 0);
        if (bytes_read > 0) {
            in_buffer.append(tmp_buf, bytes_read);
            return bytes_read;
        } else if (bytes_read == 0) {
            return -1;
        } else if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
            return -1;
        } else return 666;
    }

    std::tuple<int, std::string, std::string> readHttpRequest() {
        size_t header_end = std::string::npos;
        while ((header_end = in_buffer.find("\r\n\r\n")) == std::string::npos) {
            ssize_t res = recv_from_slot();
            if (res == 0) return {0, std::string(), std::string()};
            if (res == -1) return {-1, std::string(), std::string()};  
        }
        size_t header_len = header_end + 4;      
        std::string header_view = in_buffer.substr(0, header_len);
        size_t body_len = [](const std::string& header) -> size_t {
            size_t pos = header.find("Content-Length:");
            if (pos == std::string::npos) pos = header.find("content-length:");
            if (pos != std::string::npos) {
                pos += 15; 
                size_t end_pos = header.find("\r\n", pos);
                if (end_pos != std::string::npos)
                    return std::stoull(header.substr(pos, end_pos - pos));
            }
            return 0; 
        } (header_view);
        size_t total_len = header_len + body_len;
        while (in_buffer.length() < total_len) {
            ssize_t res = recv_from_slot();
            if (res == 0) return {0, std::string(), std::string()};
            if (res == -1) return {-1, std::string(), std::string()};
        }
        std::string full_request = in_buffer.substr(0, total_len);
        in_buffer.erase(0, total_len);
        std::string header = full_request.substr(0, header_len);
        std::string body = full_request.substr(header_len);
        return {header.size() + body.size(), header, body};
    }
    
    std::tuple<int, std::string> readExactly(size_t length) {
        while (in_buffer.length() < length) {
            ssize_t res = recv_from_slot();
            if (res == 0) return {0, std::string()};
            if (res == -1) return {-1, std::string()};
        }
        std::string result = in_buffer.substr(0, length);
        in_buffer.erase(0, length);
        return {int(result.size()), result};
    }

    std::tuple<int, std::string> readUntil(const std::string& delimiter) {
        while (true) {
            size_t pos = in_buffer.find(delimiter);
            if (pos != std::string::npos) {
                size_t extract_len = pos + delimiter.length();
                std::string result = in_buffer.substr(0, extract_len);
                in_buffer.erase(0, extract_len); 
                return {int(result.size()), result};
            }
            ssize_t res = recv_from_slot();
            if (res == 0) return {0, std::string()};
            if (res == -1) return {-1, std::string()};
        }
    }

    //private:
    ssize_t send_to_slot() {
        ssize_t sent = ::send(fd, out_buffer.c_str(), out_buffer.length(), MSG_NOSIGNAL);
        if (sent >= 0) { 
            out_buffer.erase(0, sent);
            ssize_t rem = out_buffer.size();
            return rem;
        } else if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return out_buffer.size();
            return -1;
        }  else return 666;
    }

    ssize_t appendAndSend(const std::string& msg = "") { 
        out_buffer += msg;
        return send_to_slot();
    }

    
    //----------------------------------------------------------------------


    std::tuple<int, std::string> readRequest_Ws() {
        while (true) {
            if (in_buffer.size() < 2) {
                ssize_t res = recv_from_slot();
                if (res <= 0) return {int(res), ""};
                continue;
            }
            uint8_t b1 = in_buffer[0], b2 = in_buffer[1], op = b1 & 0x0F;
            bool has_mask = b2 & 0x80;
            uint64_t plen = b2 & 0x7F;
            size_t hlen = 2 + (plen == 126 ? 2 : (plen == 127 ? 8 : 0)) + (has_mask ? 4 : 0);
            
            if (in_buffer.size() < hlen) {
                ssize_t res = recv_from_slot();
                if (res <= 0) return {int(res), ""};
                continue;
            }
            size_t off = 2;
            if (plen == 126) {
                plen = ((uint8_t)in_buffer[2] << 8) | (uint8_t)in_buffer[3];
                off += 2;
            } else if (plen == 127) {
                plen = 0;
                for (int i = 0; i < 8; ++i) plen = (plen << 8) | (uint8_t)in_buffer[off++];
            }
            char mask[4] = {0};
            if (has_mask) for (int i = 0; i < 4; ++i) mask[i] = in_buffer[off++];
            
            if (in_buffer.size() < off + plen) {
                ssize_t res = recv_from_slot();
                if (res <= 0) return {int(res), ""};
                continue;
            }
            std::string payload = in_buffer.substr(off, plen);
            in_buffer.erase(0, off + plen);
            if (has_mask) for (size_t i = 0; i < plen; ++i) payload[i] ^= mask[i % 4];
            if (op == 0x8) return {-1, ""};
            return {1, payload};
        }
    }

    ssize_t appendAndSend_Ws(const std::string& msg) {
        std::string f(1, (char)0x81);
        size_t l = msg.size();
        if (l < 126) f += (char)l;
        else if (l <= 0xFFFF) { f += (char)126; f += (char)(l >> 8); f += (char)(l & 0xFF); }
        else { f += (char)127; for (int i = 7; i >= 0; --i) f += (char)((l >> (i * 8)) & 0xFF); }
        out_buffer += f + msg;
        return send_to_slot();
    }
};



using Sock = TcpSocket;
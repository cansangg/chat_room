#include <string>
#include <vector>
#include <cstdint>
#include <unistd.h>
#include <sys/epoll.h>

void Notice(int notice_fd) {
    uint64_t val = 1;
    ::write(notice_fd, &val, sizeof(val)); 
}

void Reset(int notice_fd) {
    uint64_t val; 
    ::read(notice_fd, &val, sizeof(val));
}

std::string getWsAcceptKey(const std::string& req) {
    size_t start = req.find("Sec-WebSocket-Key: ");
    if (start == std::string::npos) return "";
    start += 19;
    
    size_t end = req.find("\r\n", start);
    if (end == std::string::npos) return "";

    std::string input = req.substr(start, end - start) + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    
    uint32_t state[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};
    uint64_t bit_len = input.length() * 8;
    std::vector<uint8_t> data(input.begin(), input.end());
    
    data.push_back(0x80);
    while ((data.size() % 64) != 56) data.push_back(0x00);
    for (int i = 7; i >= 0; i--) data.push_back((bit_len >> (i * 8)) & 0xFF);

    auto rol = [](uint32_t val, uint32_t bits) { return (val << bits) | (val >> (32 - bits)); };

    for (size_t i = 0; i < data.size(); i += 64) {
        uint32_t a = state[0], b = state[1], c = state[2], d = state[3], e = state[4], w[80];
        for (int j = 0; j < 16; j++)
            w[j] = (data[i + j * 4] << 24) | (data[i + j * 4 + 1] << 16) | (data[i + j * 4 + 2] << 8) | data[i + j * 4 + 3];
        for (int j = 16; j < 80; j++)
            w[j] = rol(w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16], 1);
        
        for (int j = 0; j < 80; j++) {
            uint32_t f, k;
            if (j < 20) { f = (b & c) | ((~b) & d); k = 0x5A827999; }
            else if (j < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1; }
            else if (j < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
            else { f = b ^ c ^ d; k = 0xCA62C1D6; }
            uint32_t temp = rol(a, 5) + f + e + k + w[j];
            e = d; d = c; c = rol(b, 30); b = a; a = temp;
        }
        state[0] += a; state[1] += b; state[2] += c; state[3] += d; state[4] += e;
    }

    std::vector<uint8_t> hash(20);
    for (int i = 0; i < 5; i++) {
        hash[i * 4] = (state[i] >> 24) & 0xFF;
        hash[i * 4 + 1] = (state[i] >> 16) & 0xFF;
        hash[i * 4 + 2] = (state[i] >> 8) & 0xFF;
        hash[i * 4 + 3] = state[i] & 0xFF;
    }

    const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    int val = 0, valb = -6;
    for (uint8_t c : hash) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(b64[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(b64[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');

    return out;
}

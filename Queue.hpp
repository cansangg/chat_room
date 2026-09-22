#pragma once
#include <atomic>
#include <vector>
#include <semaphore>
#include <stdexcept>
#include <new>

// 硬件缓存行大小，用于防止伪共享 (False Sharing)
constexpr size_t CACHE_LINE_SIZE = 64;

template<typename T>
class Queue {
private:
    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    alignas(CACHE_LINE_SIZE) std::vector<Cell> buffer;
    alignas(CACHE_LINE_SIZE) const size_t buffer_mask;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> enqueue_pos;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> dequeue_pos;

    // 🌟 修复：直接使用整型常量 2147483647 作为 LeastMaxValue，完美避开函数调用在模板参数中的解析限制
    std::counting_semaphore<2147483647> free_slots;
    std::counting_semaphore<2147483647> data_items;

    bool lf_push(T data) {
        Cell* cell;
        size_t pos = enqueue_pos.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer[pos & buffer_mask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)pos;
            
            if (dif == 0) {
                if (enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; 
            } else {
                pos = enqueue_pos.load(std::memory_order_relaxed);
            }
        }
        cell->data = std::move(data);
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool lf_pop(T& data) {
        Cell* cell;
        size_t pos = dequeue_pos.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer[pos & buffer_mask];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
            
            if (dif == 0) {
                if (dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; 
            } else {
                pos = dequeue_pos.load(std::memory_order_relaxed);
            }
        }
        data = std::move(cell->data);
        cell->sequence.store(pos + buffer_mask + 1, std::memory_order_release);
        return true;
    }

public:
    explicit Queue(size_t capacity = 65536) 
        : buffer(capacity), 
          buffer_mask(capacity - 1),
          free_slots(capacity),
          data_items(0) 
    {
        if ((capacity == 0) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("Queue capacity 必须是 2 的整数次幂");
        }
        for (size_t i = 0; i < capacity; ++i) {
            buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos.store(0, std::memory_order_relaxed);
        dequeue_pos.store(0, std::memory_order_relaxed);
    }

    // 塞入数据（满了会自动挂起）
    void push(T elem) {
        free_slots.acquire();
        lf_push(std::move(elem));
        data_items.release();
    }

    // 取出数据（空了会自动挂起，供 Worker 线程使用）
    T get_pop() {
        data_items.acquire();
        T data;
        lf_pop(data);
        free_slots.release();
        return data;
    }

    // 非阻塞取出数据（专供 Epoll 主线程使用）
    bool try_pop(T& data) {
        if (data_items.try_acquire()) {
            lf_pop(data);
            free_slots.release();
            return true;
        }
        return false;
    }
};
#include "position_distributor/topic_system.h"
#include "position_distributor/logger.h"
#include "position_distributor/position_encoding.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cstring>
#include <stdexcept>
#include <filesystem>
#include <functional>
#include <cstdlib>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <atomic>

namespace position_distributor
{

    // Lock-free SharedMemoryRingBuffer implementation
    SharedMemoryRingBuffer::SharedMemoryRingBuffer(const std::string &topic, uint32_t term_length)
        : topic_(topic), term_length_(term_length), term_count_(DEFAULT_TERM_COUNT), shm_fd_(-1), mmap_ptr_(nullptr), header_(nullptr), terms_(nullptr), is_creator_(false)
    {
        // Ensure term length is power of 2
        if ((term_length & (term_length - 1)) != 0)
        {
            throw std::invalid_argument("Term length must be power of 2");
        }

        shm_path_ = getSharedMemoryPath();
        total_size_ = sizeof(SharedMemoryHeader) + (term_count_ * (sizeof(TermBuffer) + term_length_));
        total_capacity_ = term_length_ * (term_count_ - 1); // Reserve one term for safety

        // Allocate term pointer array
        terms_ = new TermBuffer *[term_count_];
        std::memset(terms_, 0, term_count_ * sizeof(TermBuffer *));
    }

    SharedMemoryRingBuffer::~SharedMemoryRingBuffer()
    {
        cleanup();
        delete[] terms_;
    }

    std::string SharedMemoryRingBuffer::getSharedMemoryPath() const
    {
        // Use POSIX shared memory naming (without path separators)
        // Keep names short for macOS compatibility (max ~30 chars)
        std::string name = "pd_" + topic_; // pd = position_distributor
        // Replace dots with underscores for valid shm names
        std::replace(name.begin(), name.end(), '.', '_');

        // Truncate if too long (POSIX SHM names are limited on macOS)
        if (name.length() > 30)
        {
            name = name.substr(0, 30);
        }

        LOG_DEBUG("Generated shared memory path: " + name + " for topic: " + topic_);
        return name;
    }

    bool SharedMemoryRingBuffer::initialize()
    {
        try
        {
            // Try to open existing POSIX shared memory first
            shm_fd_ = shm_open(shm_path_.c_str(), O_RDWR, 0666);

            if (shm_fd_ == -1 && errno == ENOENT)
            {
                // Doesn't exist, create it
                shm_fd_ = shm_open(shm_path_.c_str(), O_CREAT | O_RDWR, 0666);
                is_creator_ = true;
            }

            if (shm_fd_ == -1)
            {
                LOG_ERROR("Failed to open/create POSIX shared memory: " + shm_path_ +
                          " (errno: " + std::to_string(errno) + " - " + strerror(errno) + ")");
                return false;
            }

            // Only set size if we created it
            if (is_creator_)
            {
                if (ftruncate(shm_fd_, total_size_) == -1)
                {
                    LOG_ERROR("Failed to set shared memory file size");
                    close(shm_fd_);
                    shm_unlink(shm_path_.c_str());
                    shm_fd_ = -1;
                    return false;
                }
            }

            // Memory map the file
            mmap_ptr_ = mmap(nullptr, total_size_, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
            if (mmap_ptr_ == MAP_FAILED)
            {
                LOG_ERROR("Failed to memory map shared memory file");
                close(shm_fd_);
                shm_fd_ = -1;
                mmap_ptr_ = nullptr;
                return false;
            }

            // Initialize header
            header_ = static_cast<SharedMemoryHeader *>(mmap_ptr_);
            if (is_creator_ || !header_->isValid())
            {
                // Initialize new header (only if we created it or it's invalid)
                new (header_) SharedMemoryHeader();
                header_->term_length = term_length_;
                header_->term_count = term_count_;
            }

            // Initialize term pointers
            uint8_t *term_base = static_cast<uint8_t *>(mmap_ptr_) + sizeof(SharedMemoryHeader);
            for (uint32_t i = 0; i < term_count_; ++i)
            {
                terms_[i] = reinterpret_cast<TermBuffer *>(
                    term_base + i * (sizeof(TermBuffer) + term_length_));
                terms_[i]->term_id = i;
            }

            LOG_INFO("Initialized shared memory ring buffer for topic: " + topic_);
            return true;
        }
        catch (const std::exception &e)
        {
            LOG_ERROR("Exception during shared memory initialization: " + std::string(e.what()));
            cleanup();
            return false;
        }
    }

    void SharedMemoryRingBuffer::cleanup()
    {
        if (mmap_ptr_ != nullptr)
        {
            munmap(mmap_ptr_, total_size_);
            mmap_ptr_ = nullptr;
        }

        if (shm_fd_ != -1)
        {
            close(shm_fd_);
            // Only unlink if we created the shared memory object
            if (is_creator_)
            {
                shm_unlink(shm_path_.c_str());
            }
            shm_fd_ = -1;
        }

        header_ = nullptr;
        std::memset(terms_, 0, term_count_ * sizeof(TermBuffer *));
    }

    bool SharedMemoryRingBuffer::write(const uint8_t *data, uint32_t length, uint32_t session_id)
    {
        if (!header_ || !data || length == 0)
        {
            return false;
        }

        // 1. Add CACHELINE - 1 (to make sure we round up), 2. then filter out last CACHELINE - 1 bits -> 3. Effectively rounding up to multiple of CACHELINE
        const uint32_t aligned_size = (sizeof(MessageFrame) + length + (CACHELINE - 1)) & ~(CACHELINE - 1);

        // For now only allow up to term_length size
        if (aligned_size > term_length_)
        {
            LOG_ERROR("Message size (" + std::to_string(aligned_size) +
                      ") exceeds term buffer size (" + std::to_string(term_length_) + ")");
            return false;
        }

        // Check backpressure before attempting reservation
        if (!producerCanWrite(aligned_size))
        {
            LOG_WARN("Ring buffer full, dropping message for topic: " + topic_);
            return false;
        }

        // Atomically reserve space
        uint64_t claimed = header_->producer_pos.fetch_add(aligned_size, std::memory_order_acq_rel);

        uint32_t term_index = getTermIndex(claimed);
        uint32_t term_offset = getTermOffset(claimed);
        TermBuffer *term = terms_[term_index];

        // Handle wrap-around / padding
        if (term_offset + aligned_size > term_length_)
        {
            // Write a padding frame
            uint32_t padding = term_length_ - term_offset;
            MessageFrame *pad = reinterpret_cast<MessageFrame *>(term->data + term_offset);
            pad->frame_type = MessageFrame::FRAME_TYPE_PADDING;
            pad->reserved = 0;
            pad->session_id = session_id;
            pad->term_id = term->term_id;
            pad->term_offset = term_offset;
            pad->frame_length.store(padding, std::memory_order_release);

            // Move to next term
            header_->active_term_id.fetch_add(1, std::memory_order_relaxed);
            claimed = header_->producer_pos.fetch_add(aligned_size, std::memory_order_acq_rel);
            term_index = getTermIndex(claimed);
            term_offset = getTermOffset(claimed);
            term = terms_[term_index];
        }

        // Write frame with frame_length = 0 initially (publish barrier)
        MessageFrame *frame = reinterpret_cast<MessageFrame *>(term->data + term_offset);
        frame->frame_type = MessageFrame::FRAME_TYPE_DATA;
        frame->reserved = 0;
        frame->session_id = session_id;
        frame->term_id = term->term_id;
        frame->term_offset = term_offset;

        // Copy payload
        std::memcpy(frame->getPayload(), data, length);

        // Publish: set length last with release semantics (CRITICAL for lock-free operation)
        frame->frame_length.store(aligned_size, std::memory_order_release);

        return true;
    }

    std::optional<SubscriberHandle> SharedMemoryRingBuffer::registerSubscriber(const char *name)
    {
        if (!header_)
        {
            return std::nullopt;
        }

        const uint32_t pid = static_cast<uint32_t>(::getpid());
        const uint64_t now = nowNanos();

        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            uint32_t expected = 0;
            if (header_->subs[i].active.compare_exchange_strong(expected, 1, std::memory_order_acq_rel))
            {
                // Claimed slot
                header_->subs[i].pid = pid;
                header_->subs[i].generation++;
                header_->subs[i].cursor.store(header_->producer_pos.load(std::memory_order_acquire), std::memory_order_release);
                header_->subs[i].last_heartbeat_ns.store(now, std::memory_order_release);

                if (name)
                {
                    std::snprintf(header_->subs[i].name, sizeof(header_->subs[i].name), "%.31s", name);
                }
                else
                {
                    std::snprintf(header_->subs[i].name, sizeof(header_->subs[i].name), "sub_%u_%u", pid, i);
                }

                // Update cached min consumer position
                header_->min_consumer_pos.store(minOfAllSubscribers(), std::memory_order_release);

                LOG_INFO("Registered subscriber: " + std::string(header_->subs[i].name) + " (slot " + std::to_string(i) + ")");
                return SubscriberHandle{i, header_->subs[i].generation};
            }
        }

        LOG_ERROR("No free subscriber slots available (max: " + std::to_string(MAX_SUBSCRIBERS) + ")");
        return std::nullopt; // No free slot
    }

    void SharedMemoryRingBuffer::unregisterSubscriber(const SubscriberHandle &handle)
    {
        if (!header_ || !handle.isValid())
        {
            return;
        }

        auto &slot = header_->subs[handle.index];
        // Only clear if still the same generation to avoid stomping a new owner
        if (slot.generation == handle.generation)
        {
            LOG_INFO("Unregistering subscriber: " + std::string(slot.name) + " (slot " + std::to_string(handle.index) + ")");
            slot.active.store(0, std::memory_order_release);
            header_->min_consumer_pos.store(minOfAllSubscribers(), std::memory_order_release);
        }
    }

    bool SharedMemoryRingBuffer::poll(const SubscriberHandle &handle, std::function<void(const uint8_t *, uint32_t, uint32_t)> handler)
    {
        if (!header_ || !handle.isValid() || !handler)
        {
            return false;
        }

        auto &slot = header_->subs[handle.index];
        if (slot.generation != handle.generation)
        {
            LOG_WARN("Subscriber handle is stale, slot was reused");
            return false; // Slot reused; re-register
        }

        uint64_t cursor = slot.cursor.load(std::memory_order_relaxed);
        const uint64_t prod = header_->producer_pos.load(std::memory_order_acquire);

        if (cursor >= prod)
        {
            return false; // Nothing new
        }

        // Check for overrun
        if (isOverrun(cursor))
        {
            LOG_WARN("Subscriber overrun detected, jumping to latest position");
            // Jump to a safe position (latest - some buffer)
            cursor = prod > total_capacity_ / 2 ? prod - total_capacity_ / 2 : 0;
            slot.cursor.store(cursor, std::memory_order_release);
        }

        uint32_t term_idx = getTermIndex(cursor);
        uint32_t term_off = getTermOffset(cursor);
        TermBuffer *term = terms_[term_idx];
        const MessageFrame *frame = reinterpret_cast<const MessageFrame *>(term->data + term_off);

        uint32_t frame_len = frame->frame_length.load(std::memory_order_acquire);
        if (frame_len == 0)
        {
            return false; // Not yet committed
        }

        if (frame->frame_type == MessageFrame::FRAME_TYPE_PADDING)
        {
            // Skip to next term
            cursor += frame_len;
            slot.cursor.store(cursor, std::memory_order_release);
            return true; // Consumed padding, try again
        }

        if (frame->frame_type == MessageFrame::FRAME_TYPE_DATA)
        {
            handler(frame->getPayload(), frame->getPayloadSize(), frame->session_id);
        }

        // Advance this subscriber's cursor
        cursor += frame_len;
        slot.cursor.store(cursor, std::memory_order_release);
        slot.last_heartbeat_ns.store(nowNanos(), std::memory_order_relaxed);

        return true;
    }

    // Helper methods for lock-free ring buffer
    uint32_t SharedMemoryRingBuffer::getTermOffset(uint64_t position) const
    {
        return static_cast<uint32_t>(position & (term_length_ - 1));
    }

    uint32_t SharedMemoryRingBuffer::getTermIndex(uint64_t position) const
    {
        return static_cast<uint32_t>((position / term_length_) % term_count_);
    }

    bool SharedMemoryRingBuffer::producerCanWrite(uint32_t needed_bytes)
    {
        const uint64_t prod = header_->producer_pos.load(std::memory_order_acquire);
        uint64_t min_cons = header_->min_consumer_pos.load(std::memory_order_acquire);

        // Fast path: check against cached min; if too tight, recompute true min
        if ((prod + needed_bytes) - min_cons <= total_capacity_)
        {
            return true;
        }

        min_cons = minOfAllSubscribers(); // O(MAX_SUBSCRIBERS), not too expesnvie when <= 64
        header_->min_consumer_pos.store(min_cons, std::memory_order_release);
        return (prod + needed_bytes) - min_cons <= total_capacity_;
    }

    uint64_t SharedMemoryRingBuffer::minOfAllSubscribers()
    {
        uint64_t min_cursor = header_->producer_pos.load(std::memory_order_acquire);

        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            if (header_->subs[i].active.load(std::memory_order_acquire) == 1)
            {
                uint64_t cursor = header_->subs[i].cursor.load(std::memory_order_acquire);
                min_cursor = std::min(min_cursor, cursor);
            }
        }

        return min_cursor;
    }

    uint64_t SharedMemoryRingBuffer::nowNanos() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    void SharedMemoryRingBuffer::sendProdcuerHeartbeat()
    {
        if (!header_)
            return;
        header_->producer_heartbeat_ns.store(nowNanos(), std::memory_order_release);
    }

    bool SharedMemoryRingBuffer::isProducerAlive(uint64_t timeout_ns) const
    {
        if (!header_)
            return false;
        uint64_t last_heartbeat = header_->producer_heartbeat_ns.load(std::memory_order_acquire);
        uint64_t now = nowNanos();
        bool alive = (last_heartbeat == 0) || ((now - last_heartbeat) <= timeout_ns);

        LOG_DEBUG("Producer alive check - last_heartbeat: " + std::to_string(last_heartbeat) +
                  ", now: " + std::to_string(now) +
                  ", timeout_ns: " + std::to_string(timeout_ns) +
                  ", result: " + std::string(alive ? "true" : "false") +
                  " for topic: " + topic_);

        if (last_heartbeat == 0)
            return true; // Producer hasn't started heartbeating yet
        return (now - last_heartbeat) <= timeout_ns;
    }

    void SharedMemoryRingBuffer::checkSubscriberHeartbeats(uint64_t timeout_ns) const
    {
        if (!header_)
            return;

        uint64_t now = nowNanos();
        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            auto &slot = header_->subs[i];
            if (slot.active.load(std::memory_order_acquire) == 1)
            {
                uint64_t last_heartbeat = slot.last_heartbeat_ns.load(std::memory_order_acquire);
                if (last_heartbeat > 0 && (now - last_heartbeat) > timeout_ns)
                {
                    // Check if process is still alive before cleaning up
                    if (!isProcessAlive(slot.pid))
                    {
                        LOG_WARN("Dead subscriber detected - cleaning up slot: " + std::to_string(i) +
                                 " name: " + std::string(slot.name) + " PID: " + std::to_string(slot.pid) +
                                 " for topic: " + topic_);

                        // Clean up the dead subscriber slot
                        slot.active.store(0, std::memory_order_release);
                        slot.last_heartbeat_ns.store(0, std::memory_order_release);
                        slot.cursor.store(0, std::memory_order_release);
                        slot.pid = 0;
                        slot.generation++; // Bump generation to invalidate any old handles
                        std::memset(slot.name, 0, sizeof(slot.name));

                        // Update min consumer position since we removed a subscriber
                        const_cast<SharedMemoryRingBuffer *>(this)->updateMinConsumerPosition();
                    }
                    else
                    {
                        // Process alive but heartbeat lost - just log warning
                        LOG_WARN("Subscriber heartbeat lost (process alive) - slot: " + std::to_string(i) +
                                 " name: " + std::string(slot.name) + " PID: " + std::to_string(slot.pid) +
                                 " for topic: " + topic_);
                    }
                }
            }
        }
    }

    bool SharedMemoryRingBuffer::isOverrun(uint64_t cursor) const
    {
        const uint64_t prod = header_->producer_pos.load(std::memory_order_acquire);
        return (prod - cursor) > total_capacity_;
    }

    bool SharedMemoryRingBuffer::isProcessAlive(uint32_t pid) const
    {
        if (pid == 0)
            return false;

        // Use kill(pid, 0) to check if process exists without sending signal
        return ::kill(static_cast<pid_t>(pid), 0) == 0;
    }

    void SharedMemoryRingBuffer::updateMinConsumerPosition()
    {
        if (header_)
        {
            header_->min_consumer_pos.store(minOfAllSubscribers(), std::memory_order_release);
        }
    }

    void SharedMemoryRingBuffer::clearAllSubscribers()
    {
        if (!header_)
            return;

        LOG_INFO("Clearing all subscriber slots for topic: " + topic_);

        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            auto &slot = header_->subs[i];
            if (slot.active.load(std::memory_order_acquire) == 1)
            {
                LOG_INFO("Clearing subscriber slot: " + std::to_string(i) +
                         " name: " + std::string(slot.name) + " PID: " + std::to_string(slot.pid));
            }

            // Clear the slot completely
            slot.active.store(0, std::memory_order_release);
            slot.last_heartbeat_ns.store(0, std::memory_order_release);
            slot.cursor.store(0, std::memory_order_release);
            slot.pid = 0;
            slot.generation++; // Bump generation to invalidate any old handles
            std::memset(slot.name, 0, sizeof(slot.name));
        }

        // Reset min consumer position since all subscribers are gone
        header_->min_consumer_pos.store(header_->producer_pos.load(std::memory_order_acquire), std::memory_order_release);
    }

    uint64_t SharedMemoryRingBuffer::getProducerPosition() const
    {
        return header_ ? header_->producer_pos.load(std::memory_order_acquire) : 0;
    }

    uint64_t SharedMemoryRingBuffer::getMinConsumerPosition() const
    {
        return header_ ? header_->min_consumer_pos.load(std::memory_order_acquire) : 0;
    }

    size_t SharedMemoryRingBuffer::getActiveSubscriberCount() const
    {
        if (!header_)
        {
            return 0;
        }

        size_t count = 0;
        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            if (header_->subs[i].active.load(std::memory_order_acquire) == 1)
            {
                count++;
            }
        }
        return count;
    }

    std::vector<std::pair<std::string, uint64_t>> SharedMemoryRingBuffer::getSubscriberInfo() const
    {
        std::vector<std::pair<std::string, uint64_t>> info;
        if (!header_)
        {
            return info;
        }

        const uint64_t producer_pos = getProducerPosition();

        for (uint32_t i = 0; i < MAX_SUBSCRIBERS; ++i)
        {
            if (header_->subs[i].active.load(std::memory_order_acquire) == 1)
            {
                uint64_t cursor = header_->subs[i].cursor.load(std::memory_order_acquire);
                uint64_t lag = producer_pos - cursor;
                std::string name = std::string(header_->subs[i].name) + " (lag: " + std::to_string(lag) + ")";
                info.emplace_back(name, cursor);
            }
        }
        return info;
    }

    TopicChannel::TopicChannel(const std::string &topic)
        : topic_(topic)
    {
        ring_buffer_ = std::make_unique<SharedMemoryRingBuffer>(topic);
    }

    TopicChannel::~TopicChannel()
    {
        cleanup();
    }

    bool TopicChannel::initialize()
    {
        return ring_buffer_->initialize();
    }

    void TopicChannel::cleanup()
    {
        // Unregister the single subscriber if it exists
        if (subscriber_handle_.has_value())
        {
            ring_buffer_->unregisterSubscriber(*subscriber_handle_);
            subscriber_handle_.reset();
            message_handler_.reset();
        }

        if (ring_buffer_)
        {
            ring_buffer_->cleanup();
        }
    }

    bool TopicChannel::publish(const uint8_t *data, uint32_t length, uint32_t session_id)
    {
        return ring_buffer_->write(data, length, session_id);
    }

    // Single subscriber interface
    bool TopicChannel::subscribe(MessageHandler handler, const char *subscriber_name)
    {
        if (!ring_buffer_)
        {
            LOG_ERROR("Ring buffer not initialized for topic: " + topic_);
            return false;
        }

        if (subscriber_handle_.has_value())
        {
            LOG_ERROR("Process already has a subscriber for topic: " + topic_);
            return false;
        }

        auto handle = ring_buffer_->registerSubscriber(subscriber_name);
        if (handle.has_value())
        {
            subscriber_handle_ = handle;
            message_handler_ = std::make_unique<MessageHandler>(std::move(handler));
            LOG_INFO("Subscribed to topic: " + topic_ + " with name: " + (subscriber_name ? subscriber_name : "unnamed"));
            return true;
        }

        LOG_ERROR("Failed to register subscriber for topic: " + topic_);
        return false;
    }

    bool TopicChannel::poll()
    {
        if (!ring_buffer_ || !subscriber_handle_.has_value() || !message_handler_)
        {
            return false;
        }

        return ring_buffer_->poll(*subscriber_handle_, *message_handler_);
    }

    bool TopicChannel::hasSubscription() const
    {
        return subscriber_handle_.has_value();
    }


    size_t TopicChannel::getActiveSubscriberCount() const
    {
        return ring_buffer_ ? ring_buffer_->getActiveSubscriberCount() : 0;
    }

    std::vector<std::pair<std::string, uint64_t>> TopicChannel::getSubscriberInfo() const
    {
        return ring_buffer_ ? ring_buffer_->getSubscriberInfo() : std::vector<std::pair<std::string, uint64_t>>{};
    }

    void TopicChannel::sendProdcuerHeartbeat()
    {
        if (ring_buffer_)
        {
            ring_buffer_->sendProdcuerHeartbeat();
        }
    }

    bool TopicChannel::isProducerAlive(uint64_t timeout_ns) const
    {
        return ring_buffer_ ? ring_buffer_->isProducerAlive(timeout_ns) : false;
    }

    void TopicChannel::checkSubscriberHeartbeats(uint64_t timeout_ns) const
    {
        if (ring_buffer_)
        {
            ring_buffer_->checkSubscriberHeartbeats(timeout_ns);
        }
    }

    void TopicChannel::clearAllSubscribers()
    {
        if (ring_buffer_)
        {
            ring_buffer_->clearAllSubscribers();
        }
    }

    // TopicRegistry implementation
    TopicRegistry &TopicRegistry::instance()
    {
        static TopicRegistry instance;
        return instance;
    }

    TopicRegistry::~TopicRegistry()
    {
        cleanup();
    }

    std::shared_ptr<TopicChannel> TopicRegistry::getOrCreateTopic(const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        auto it = topics_.find(topic);
        if (it != topics_.end())
        {
            return it->second;
        }

        // Create new topic channel
        auto channel = std::make_shared<TopicChannel>(topic);
        if (!channel->initialize())
        {
            LOG_ERROR("Failed to initialize topic channel: " + topic);
            return nullptr;
        }

        topics_[topic] = channel;
        LOG_INFO("Created topic channel: " + topic);
        return channel;
    }

    bool TopicRegistry::removeTopic(const std::string &topic)
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        auto it = topics_.find(topic);
        if (it != topics_.end())
        {
            it->second->cleanup();
            topics_.erase(it);
            LOG_INFO("Removed topic channel: " + topic);
            return true;
        }

        return false;
    }

    void TopicRegistry::cleanup()
    {
        try
        {
            std::lock_guard<std::mutex> lock(topics_mutex_);

            for (auto &[topic, channel] : topics_)
            {
                channel->cleanup();
            }
            topics_.clear();

            // POSIX shared memory objects are automatically cleaned up when unlinked
            // No additional filesystem cleanup needed
            LOG_INFO("Cleaned up all POSIX shared memory objects");
        }
        catch (const std::system_error &e)
        {
            LOG_DEBUG("Unable to acquire topics mutex during cleanup: " + std::string(e.what()));
        }
    }

    size_t TopicRegistry::getTopicCount() const
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        return topics_.size();
    }

    std::vector<std::string> TopicRegistry::getTopicList() const
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);

        std::vector<std::string> topics;
        topics.reserve(topics_.size());

        for (const auto &[topic, channel] : topics_)
        {
            topics.push_back(topic);
        }

        return topics;
    }

} // namespace position_distributor

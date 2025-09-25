#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <functional>
#include <optional>
#include <chrono>

namespace position_distributor
{

    // Constants for lock-free ring buffer
    static constexpr uint32_t MAX_SUBSCRIBERS = 64;
    static constexpr uint32_t CACHELINE = 64;

    // Subscriber slot for multi-consumer support
    struct alignas(CACHELINE) SubscriberSlot
    {
        std::atomic<uint64_t> cursor;            // Last-consumed absolute position
        std::atomic<uint64_t> last_heartbeat_ns; // Liveness tracking
        std::atomic<uint32_t> active;            // 0=free, 1=claimed
        uint32_t pid;                            // Process ID for debugging
        uint32_t generation;                     // Bump on reuse to avoid ABA
        uint32_t _pad;
        char name[32]; // Optional: process/topic label
    };

    // Handle for subscriber registration
    struct SubscriberHandle
    {
        uint32_t index;      // Index into header_->subs[]
        uint32_t generation; // Copy for sanity checks

        bool isValid() const { return index < MAX_SUBSCRIBERS; }
    };

    // Lock-free message frame with proper publish barrier
    struct alignas(32) MessageFrame
    {
        std::atomic<uint32_t> frame_length; // 0 until commit (MUST be first for publish barrier)
        uint16_t frame_type;
        uint16_t reserved;
        uint32_t session_id;
        uint32_t stream_id;
        uint32_t term_id;
        uint32_t term_offset;
        // payload follows...

        static constexpr uint16_t FRAME_TYPE_DATA = 1;
        static constexpr uint16_t FRAME_TYPE_HEARTBEAT = 2;
        static constexpr uint16_t FRAME_TYPE_PADDING = 3;

        MessageFrame() : frame_length(0), frame_type(0), reserved(0),
                         session_id(0), stream_id(0), term_id(0), term_offset(0) {}

        uint8_t *getPayload() { return reinterpret_cast<uint8_t *>(this + 1); }
        const uint8_t *getPayload() const { return reinterpret_cast<const uint8_t *>(this + 1); }

        uint32_t getPayloadSize() const
        {
            uint32_t len = frame_length.load(std::memory_order_acquire);
            return len > sizeof(MessageFrame) ? len - sizeof(MessageFrame) : 0;
        }
    };

    // Shared memory region header with multi-consumer support
    struct SharedMemoryHeader
    {
        static constexpr uint32_t MAGIC_NUMBER = 0xAE70A1D0;
        static constexpr uint32_t CURRENT_VERSION = 2; // Bump version for new layout

        uint32_t magic_number; // Validation magic number
        uint32_t version;      // Schema version
        uint32_t term_length;  // Size of each term (power of 2)
        uint32_t term_count;   // Number of terms (typically 3)

        // Producer (writer) absolute position
        alignas(CACHELINE) std::atomic<uint64_t> producer_pos;

        // Cached min consumer pos (optional fast-path; recompute if stale)
        alignas(CACHELINE) std::atomic<uint64_t> min_consumer_pos;

        // Active term id
        alignas(CACHELINE) std::atomic<uint32_t> active_term_id;
        uint32_t _pad0;

        // Producer heartbeat for liveness detection
        alignas(CACHELINE) std::atomic<uint64_t> producer_heartbeat_ns;

        // Subscriber slots
        alignas(CACHELINE) SubscriberSlot subs[MAX_SUBSCRIBERS];

        // Pad to cache lines
        uint8_t _pad1[CACHELINE - ((sizeof(SubscriberSlot) * MAX_SUBSCRIBERS) % CACHELINE)];

        SharedMemoryHeader()
            : magic_number(MAGIC_NUMBER), version(CURRENT_VERSION), term_length(0), term_count(3),
              producer_pos(0), min_consumer_pos(0), active_term_id(0), _pad0(0), producer_heartbeat_ns(0), _pad1{0}
        {
            // Initialize subscriber slots
            for (auto &slot : subs)
            {
                slot.cursor.store(0);
                slot.last_heartbeat_ns.store(0);
                slot.active.store(0);
                slot.pid = 0;
                slot.generation = 0;
                slot._pad = 0;
                std::memset(slot.name, 0, sizeof(slot.name));
            }
        }

        bool isValid() const
        {
            return magic_number == MAGIC_NUMBER && version == CURRENT_VERSION;
        }
    };

    // Individual term buffer
    struct TermBuffer
    {
        uint32_t term_id;
        uint32_t reserved;
        uint8_t data[]; // Flexible array member

        TermBuffer() : term_id(0), reserved(0) {}
    };

    // Lock-free ring buffer implementation for shared memory
    class SharedMemoryRingBuffer
    {
    public:
        static constexpr uint32_t DEFAULT_TERM_LENGTH = 1024 * 1024; // 1MB
        static constexpr uint32_t DEFAULT_TERM_COUNT = 3;

        SharedMemoryRingBuffer(const std::string &topic, uint32_t term_length = DEFAULT_TERM_LENGTH);
        ~SharedMemoryRingBuffer();

        // Producer interface (lock-free, multi-producer safe)
        bool write(const uint8_t *data, uint32_t length, uint32_t session_id, uint32_t stream_id);

        // Subscriber management
        std::optional<SubscriberHandle> registerSubscriber(const char *name = nullptr);
        void unregisterSubscriber(const SubscriberHandle &handle);

        // Consumer interface (per-subscriber, lock-free)
        bool poll(const SubscriberHandle &handle, std::function<void(const uint8_t *, uint32_t)> handler);

        // Legacy consumer interface (single consumer for backward compatibility)
        bool read(std::function<void(const uint8_t *, uint32_t)> handler);

        // Management
        bool initialize();
        void cleanup();
        bool isInitialized() const { return mmap_ptr_ != nullptr; }

        // Statistics
        uint64_t getProducerPosition() const;
        uint64_t getMinConsumerPosition() const;
        bool hasUnreadData(const SubscriberHandle &handle) const;

        // Debugging/monitoring
        size_t getActiveSubscriberCount() const;
        std::vector<std::pair<std::string, uint64_t>> getSubscriberInfo() const;

        // Heartbeat functionality
        void sendHeartbeat();
        bool isProducerAlive(uint64_t timeout_ns = 5000000000ULL) const; // 5 seconds default
        bool isSubscriberAlive(const SubscriberHandle &handle, uint64_t timeout_ns = 5000000000ULL) const;
        void checkSubscriberHeartbeats(uint64_t timeout_ns = 5000000000ULL) const; // Check all subscribers

    private:
        std::string topic_;
        std::string shm_path_;
        uint32_t term_length_;
        uint32_t term_count_;
        size_t total_size_;
        uint64_t total_capacity_; // Total buffer capacity for backpressure

        int shm_fd_;
        void *mmap_ptr_;
        SharedMemoryHeader *header_;
        TermBuffer **terms_;
        bool is_creator_; // Track if this instance created the shared memory

        // Helper methods
        std::string getSharedMemoryPath() const;
        uint32_t getTermOffset(uint64_t position) const;
        uint32_t getTermIndex(uint64_t position) const;

        // Lock-free producer methods
        bool producerCanWrite(uint32_t needed_bytes);
        uint64_t minOfAllSubscribers();

        // Utilities
        uint64_t nowNanos() const;
        bool isOverrun(uint64_t cursor) const;
    };

    // Topic channel management with multi-subscriber support
    class TopicChannel
    {
    public:
        TopicChannel(const std::string &topic);
        ~TopicChannel();

        bool initialize();
        void cleanup();

        // Publisher interface
        bool publish(const uint8_t *data, uint32_t length, uint32_t session_id);

        // Multi-subscriber interface
        using MessageHandler = std::function<void(const uint8_t *, uint32_t)>;
        std::optional<SubscriberHandle> subscribeMulti(MessageHandler handler, const char *subscriber_name = nullptr);
        void unsubscribe(const SubscriberHandle &handle);

        // Polling interface for registered subscribers
        bool readMessages(const SubscriberHandle &handle);

        // Legacy single-subscriber interface (for backward compatibility)
        bool subscribe(MessageHandler handler);
        void unsubscribe();
        bool readMessages();

        const std::string &getTopic() const { return topic_; }
        bool isInitialized() const { return ring_buffer_ && ring_buffer_->isInitialized(); }

        // Monitoring
        size_t getActiveSubscriberCount() const;
        std::vector<std::pair<std::string, uint64_t>> getSubscriberInfo() const;

        // Heartbeat functionality
        void sendHeartbeat();
        bool isProducerAlive(uint64_t timeout_ns = 5000000000ULL) const; // 5 seconds default
        bool isSubscriberAlive(const SubscriberHandle &handle, uint64_t timeout_ns = 5000000000ULL) const;
        void checkSubscriberHeartbeats(uint64_t timeout_ns = 5000000000ULL) const; // Check all subscribers

    private:
        std::string topic_;
        uint32_t stream_id_; // Hash of topic name
        std::unique_ptr<SharedMemoryRingBuffer> ring_buffer_;

        // Legacy single subscriber support
        bool legacy_subscribed_;
        MessageHandler legacy_message_handler_;
        std::optional<SubscriberHandle> legacy_handle_;

        // Multi-subscriber support
        std::unordered_map<uint32_t, MessageHandler> subscriber_handlers_; // index -> handler
        mutable std::mutex handlers_mutex_;                                // Protect handler map only

        uint32_t calculateStreamId(const std::string &topic);
    };

    // Main shared memory manager
    class SharedMemoryManager
    {
    public:
        static SharedMemoryManager &instance();

        ~SharedMemoryManager();

        // Topic management
        std::shared_ptr<TopicChannel> getOrCreateTopic(const std::string &topic);
        bool removeTopic(const std::string &topic);
        void cleanup();

        // Statistics
        size_t getTopicCount() const;
        std::vector<std::string> getTopicList() const;

    private:
        SharedMemoryManager() = default;

        std::unordered_map<std::string, std::shared_ptr<TopicChannel>> topics_;
        mutable std::mutex topics_mutex_;

        // Disable copy/move
        SharedMemoryManager(const SharedMemoryManager &) = delete;
        SharedMemoryManager &operator=(const SharedMemoryManager &) = delete;
    };

} // namespace position_distributor

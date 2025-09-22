#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <functional>

namespace position_distributor
{

    // Shared memory region header
    struct SharedMemoryHeader
    {
        static constexpr uint32_t MAGIC_NUMBER = 0xAE70A1D0;
        static constexpr uint32_t CURRENT_VERSION = 1;

        uint32_t magic_number;                // Validation magic number
        uint32_t version;                     // Schema version
        uint32_t term_length;                 // Size of each term (power of 2)
        uint32_t term_count;                  // Number of terms (typically 3)
        std::atomic<uint64_t> head_position;  // Producer write position
        std::atomic<uint64_t> tail_position;  // Consumer read position
        std::atomic<uint32_t> active_term_id; // Current active term
        uint32_t reserved[13];                // Reserved for future use (64-byte aligned)

        SharedMemoryHeader()
            : magic_number(MAGIC_NUMBER), version(CURRENT_VERSION), term_length(0), term_count(3), head_position(0), tail_position(0), active_term_id(0), reserved{0}
        {
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

    // Ring buffer implementation for shared memory
    class SharedMemoryRingBuffer
    {
    public:
        static constexpr uint32_t DEFAULT_TERM_LENGTH = 1024 * 1024; // 1MB
        static constexpr uint32_t DEFAULT_TERM_COUNT = 3;

        SharedMemoryRingBuffer(const std::string &topic, uint32_t term_length = DEFAULT_TERM_LENGTH);
        ~SharedMemoryRingBuffer();

        // Producer interface
        bool write(const uint8_t *data, uint32_t length, uint32_t session_id, uint32_t stream_id);

        // Consumer interface
        bool read(std::function<void(const uint8_t *, uint32_t)> handler);

        // Management
        bool initialize();
        void cleanup();
        bool isInitialized() const { return mmap_ptr_ != nullptr; }

        // Statistics
        uint64_t getHeadPosition() const;
        uint64_t getTailPosition() const;
        bool hasUnreadData() const;

    private:
        std::string topic_;
        std::string shm_path_;
        uint32_t term_length_;
        uint32_t term_count_;
        size_t total_size_;

        int shm_fd_;
        void *mmap_ptr_;
        SharedMemoryHeader *header_;
        TermBuffer **terms_;
        bool is_creator_; // Track if this instance created the shared memory

        // Helper methods
        std::string getSharedMemoryPath() const;
        TermBuffer *getActiveTerm();
        uint32_t getTermOffset(uint64_t position) const;
        uint32_t getTermIndex(uint64_t position) const;
        bool claim(uint32_t length, uint32_t &term_offset, TermBuffer *&term);
        void commit(uint32_t term_offset, uint32_t length);

        // Thread safety
        std::mutex write_mutex_;
    };

    // Topic channel management
    class TopicChannel
    {
    public:
        TopicChannel(const std::string &topic);
        ~TopicChannel();

        bool initialize();
        void cleanup();

        // Publisher interface
        bool publish(const uint8_t *data, uint32_t length, uint32_t session_id);

        // Subscriber interface
        using MessageHandler = std::function<void(const uint8_t *, uint32_t)>;
        bool subscribe(MessageHandler handler);
        void unsubscribe();

        // Direct ring buffer access for polling
        bool readMessages();

        const std::string &getTopic() const { return topic_; }
        bool isInitialized() const { return ring_buffer_ && ring_buffer_->isInitialized(); }

    private:
        std::string topic_;
        uint32_t stream_id_; // Hash of topic name
        std::unique_ptr<SharedMemoryRingBuffer> ring_buffer_;
        bool subscribed_;
        MessageHandler message_handler_;

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

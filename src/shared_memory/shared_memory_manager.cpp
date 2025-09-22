#include "position_distributor/shared_memory_manager.h"
#include "position_distributor/logger.h"
#include "position_distributor/sbe_encoding.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <filesystem>
#include <functional>
#include <cstdlib>
#include <algorithm>
#include <cerrno>

namespace position_distributor
{

    // SharedMemoryRingBuffer implementation
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

    bool SharedMemoryRingBuffer::write(const uint8_t *data, uint32_t length, uint32_t session_id, uint32_t stream_id)
    {
        if (!header_ || !data || length == 0)
        {
            return false;
        }

        std::lock_guard<std::mutex> lock(write_mutex_);

        // Calculate frame size (header + payload)
        uint32_t frame_size = sizeof(MessageFrame) + length;

        // Claim space in ring buffer
        uint32_t term_offset;
        TermBuffer *term;
        if (!claim(frame_size, term_offset, term))
        {
            LOG_WARN("Failed to claim space in ring buffer for topic: " + topic_);
            return false;
        }

        // Write message frame
        MessageFrame *frame = reinterpret_cast<MessageFrame *>(term->data + term_offset);
        *frame = MessageFrame(MessageFrame::FRAME_TYPE_DATA, length, session_id, stream_id, term->term_id);
        frame->term_offset = term_offset;

        // Copy payload
        std::memcpy(frame->getPayload(), data, length);

        // Commit the write
        commit(term_offset, frame_size);

        return true;
    }

    bool SharedMemoryRingBuffer::read(std::function<void(const uint8_t *, uint32_t)> handler)
    {
        if (!header_ || !handler)
        {
            return false;
        }

        uint64_t tail = header_->tail_position.load();
        uint64_t head = header_->head_position.load();

        if (tail >= head)
        {
            return false; // No new data
        }

        // Calculate term and offset
        uint32_t term_index = getTermIndex(tail);
        uint32_t term_offset = getTermOffset(tail);
        TermBuffer *term = terms_[term_index];

        // Read message frame
        const MessageFrame *frame = reinterpret_cast<const MessageFrame *>(term->data + term_offset);

        // Validate frame
        if (frame->frame_type == MessageFrame::FRAME_TYPE_DATA && frame->frame_length > sizeof(MessageFrame))
        {
            handler(frame->getPayload(), frame->getPayloadSize());

            // Update tail position
            header_->tail_position.store(tail + frame->frame_length);
            return true;
        }

        return false;
    }

    TermBuffer *SharedMemoryRingBuffer::getActiveTerm()
    {
        uint32_t active_term_id = header_->active_term_id.load();
        return terms_[active_term_id % term_count_];
    }

    uint32_t SharedMemoryRingBuffer::getTermOffset(uint64_t position) const
    {
        return static_cast<uint32_t>(position & (term_length_ - 1));
    }

    uint32_t SharedMemoryRingBuffer::getTermIndex(uint64_t position) const
    {
        return static_cast<uint32_t>((position / term_length_) % term_count_);
    }

    bool SharedMemoryRingBuffer::claim(uint32_t length, uint32_t &term_offset, TermBuffer *&term)
    {
        uint64_t head = header_->head_position.load();
        uint64_t tail = header_->tail_position.load();

        // Check if there's enough space
        if (head - tail + length > term_length_ * (term_count_ - 1))
        {
            return false; // Buffer full
        }

        term_offset = getTermOffset(head);
        uint32_t term_index = getTermIndex(head);
        term = terms_[term_index];

        // Check if message fits in current term
        if (term_offset + length > term_length_)
        {
            // Need to move to next term - add padding to current term
            uint32_t padding_size = term_length_ - term_offset;
            if (padding_size >= sizeof(MessageFrame))
            {
                MessageFrame *padding_frame = reinterpret_cast<MessageFrame *>(term->data + term_offset);
                *padding_frame = MessageFrame(MessageFrame::FRAME_TYPE_PADDING, 0, 0, 0, term->term_id);
                padding_frame->frame_length = padding_size;
            }

            // Move to next term
            header_->head_position.store(head + padding_size);
            header_->active_term_id.store(term->term_id + 1);

            // Recalculate for new term
            head = header_->head_position.load();
            term_offset = getTermOffset(head);
            term_index = getTermIndex(head);
            term = terms_[term_index];
        }

        return true;
    }

    void SharedMemoryRingBuffer::commit(uint32_t term_offset, uint32_t length)
    {
        // Advance head position
        header_->head_position.fetch_add(length);
    }

    uint64_t SharedMemoryRingBuffer::getHeadPosition() const
    {
        return header_ ? header_->head_position.load() : 0;
    }

    uint64_t SharedMemoryRingBuffer::getTailPosition() const
    {
        return header_ ? header_->tail_position.load() : 0;
    }

    bool SharedMemoryRingBuffer::hasUnreadData() const
    {
        return getHeadPosition() > getTailPosition();
    }

    // TopicChannel implementation
    TopicChannel::TopicChannel(const std::string &topic)
        : topic_(topic), stream_id_(calculateStreamId(topic)), subscribed_(false)
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
        unsubscribe();
        if (ring_buffer_)
        {
            ring_buffer_->cleanup();
        }
    }

    bool TopicChannel::publish(const uint8_t *data, uint32_t length, uint32_t session_id)
    {
        return ring_buffer_->write(data, length, session_id, stream_id_);
    }

    bool TopicChannel::subscribe(MessageHandler handler)
    {
        if (subscribed_)
        {
            return false;
        }

        message_handler_ = handler;
        subscribed_ = true;
        return true;
    }

    void TopicChannel::unsubscribe()
    {
        subscribed_ = false;
        message_handler_ = nullptr;
    }

    bool TopicChannel::readMessages()
    {
        if (!subscribed_ || !message_handler_ || !ring_buffer_)
        {
            return false;
        }

        // Read from the ring buffer and call the message handler
        return ring_buffer_->read(message_handler_);
    }

    uint32_t TopicChannel::calculateStreamId(const std::string &topic)
    {
        std::hash<std::string> hasher;
        return static_cast<uint32_t>(hasher(topic));
    }

    // SharedMemoryManager implementation
    SharedMemoryManager &SharedMemoryManager::instance()
    {
        static SharedMemoryManager instance;
        return instance;
    }

    SharedMemoryManager::~SharedMemoryManager()
    {
        cleanup();
    }

    std::shared_ptr<TopicChannel> SharedMemoryManager::getOrCreateTopic(const std::string &topic)
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

    bool SharedMemoryManager::removeTopic(const std::string &topic)
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

    void SharedMemoryManager::cleanup()
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

    size_t SharedMemoryManager::getTopicCount() const
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        return topics_.size();
    }

    std::vector<std::string> SharedMemoryManager::getTopicList() const
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

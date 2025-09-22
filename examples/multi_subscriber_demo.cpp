#include "position_distributor/shared_memory_manager.h"
#include "position_distributor/logger.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <signal.h>
#include <vector>
#include <atomic>

using namespace position_distributor;

std::atomic<bool> running(true);

void signalHandler(int signal)
{
    std::cout << "\nShutting down..." << std::endl;
    running = false;
}

void subscriberWorker(int id, const std::string &topic, const std::string &name)
{
    auto &manager = SharedMemoryManager::instance();
    auto channel = manager.getOrCreateTopic(topic);

    if (!channel)
    {
        std::cerr << "Failed to create topic channel: " << topic << std::endl;
        return;
    }

    // Use new multi-subscriber API
    auto handle = channel->subscribeMulti([id](const uint8_t *data, uint32_t length)
                                          { std::cout << "[Subscriber " << id << "] Received " << length << " bytes" << std::endl; }, name.c_str());

    if (!handle.has_value())
    {
        std::cerr << "Failed to register subscriber " << id << std::endl;
        return;
    }

    std::cout << "Subscriber " << id << " (" << name << ") registered with handle index "
              << handle->index << std::endl;

    int messages_received = 0;
    while (running)
    {
        if (channel->readMessages(handle.value()))
        {
            messages_received++;
        }
        else
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    std::cout << "Subscriber " << id << " received " << messages_received << " messages total" << std::endl;
    channel->unsubscribe(handle.value());
}

void publisherWorker(const std::string &topic)
{
    auto &manager = SharedMemoryManager::instance();
    auto channel = manager.getOrCreateTopic(topic);

    if (!channel)
    {
        std::cerr << "Failed to create topic channel: " << topic << std::endl;
        return;
    }

    int message_count = 0;
    while (running)
    {
        std::string message = "Hello from producer! Message #" + std::to_string(++message_count);

        if (channel->publish(reinterpret_cast<const uint8_t *>(message.c_str()),
                             message.length(), 12345))
        {
            if (message_count % 100 == 0)
            {
                std::cout << "[Publisher] Sent " << message_count << " messages. "
                          << "Active subscribers: " << channel->getActiveSubscriberCount() << std::endl;
            }
        }
        else
        {
            std::cout << "[Publisher] Failed to publish message " << message_count << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    std::cout << "Publisher sent " << message_count << " messages total" << std::endl;
}

int main()
{
    signal(SIGINT, signalHandler);

    std::cout << "=== Lock-Free Multi-Subscriber Demo ===" << std::endl;
    std::cout << "Starting 1 publisher and 3 subscribers..." << std::endl;

    const std::string topic = "demo_topic";

    // Start publisher
    std::thread publisher_thread(publisherWorker, topic);

    // Start multiple subscribers
    std::vector<std::thread> subscriber_threads;
    subscriber_threads.emplace_back(subscriberWorker, 1, topic, "FastSubscriber");
    subscriber_threads.emplace_back(subscriberWorker, 2, topic, "SlowSubscriber");
    subscriber_threads.emplace_back(subscriberWorker, 3, topic, "MonitorSubscriber");

    // Let them run for a bit
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Show statistics
    auto &manager = SharedMemoryManager::instance();
    auto channel = manager.getOrCreateTopic(topic);
    if (channel)
    {
        std::cout << "\n=== Statistics ===" << std::endl;
        std::cout << "Active subscribers: " << channel->getActiveSubscriberCount() << std::endl;

        auto info = channel->getSubscriberInfo();
        for (const auto &[name, cursor] : info)
        {
            std::cout << "  " << name << " - cursor: " << cursor << std::endl;
        }
    }

    std::cout << "\nPress Ctrl+C to stop..." << std::endl;

    // Wait for shutdown
    publisher_thread.join();
    for (auto &t : subscriber_threads)
    {
        t.join();
    }

    std::cout << "Demo completed!" << std::endl;
    return 0;
}

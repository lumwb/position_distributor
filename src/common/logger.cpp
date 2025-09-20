#include "position_distributor/logger.h"
#include <chrono>
#include <iomanip>

namespace position_distributor
{

    Logger &Logger::instance()
    {
        static Logger instance;
        return instance;
    }

    void Logger::log(LogLevel level, const std::string &message)
    {
        if (level < level_)
            return;

        std::lock_guard<std::mutex> lock(mutex_);

        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) %
                  1000;

        std::cout << std::put_time(std::localtime(&time_t), "%H:%M:%S")
                  << "." << std::setfill('0') << std::setw(3) << ms.count()
                  << " [" << levelToString(level) << "] " << message << std::endl;
    }

    std::string Logger::levelToString(LogLevel level) const
    {
        switch (level)
        {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO ";
        case LogLevel::WARN:
            return "WARN ";
        case LogLevel::ERROR:
            return "ERROR";
        default:
            return "UNKNOWN";
        }
    }

    // Standalone convenience functions
    void logDebug(const std::string &message)
    {
        Logger::instance().debug(message);
    }

    void logInfo(const std::string &message)
    {
        Logger::instance().info(message);
    }

    void logWarning(const std::string &message)
    {
        Logger::instance().warn(message);
    }

    void logError(const std::string &message)
    {
        Logger::instance().error(message);
    }

} // namespace position_distributor

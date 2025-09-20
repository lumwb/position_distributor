#pragma once

#include <string>
#include <iostream>
#include <sstream>
#include <mutex>

namespace position_distributor
{

    enum class LogLevel
    {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };

    class Logger
    {
    public:
        static Logger &instance();

        void setLevel(LogLevel level) { level_ = level; }
        LogLevel getLevel() const { return level_; }

        void log(LogLevel level, const std::string &message);
        void debug(const std::string &message) { log(LogLevel::DEBUG, message); }
        void info(const std::string &message) { log(LogLevel::INFO, message); }
        void warn(const std::string &message) { log(LogLevel::WARN, message); }
        void error(const std::string &message) { log(LogLevel::ERROR, message); }

    private:
        Logger() = default;
        std::string levelToString(LogLevel level) const;

        LogLevel level_ = LogLevel::INFO;
        std::mutex mutex_;
    };

// Convenience macros
#define LOG_DEBUG(msg) position_distributor::Logger::instance().debug(msg)
#define LOG_INFO(msg) position_distributor::Logger::instance().info(msg)
#define LOG_WARN(msg) position_distributor::Logger::instance().warn(msg)
#define LOG_ERROR(msg) position_distributor::Logger::instance().error(msg)

    // Standalone convenience functions
    void logDebug(const std::string &message);
    void logInfo(const std::string &message);
    void logWarning(const std::string &message);
    void logError(const std::string &message);

} // namespace position_distributor

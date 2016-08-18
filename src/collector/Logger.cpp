#include "Config.h"
#include "Logger.h"

#include <blackhole/builder.hpp>
#include <blackhole/config/json.hpp>
#include <blackhole/record.hpp>
#include <blackhole/registry.hpp>
#include <blackhole/root.hpp>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <syslog.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace {

std::shared_ptr<blackhole::logger_t> s_logger;
std::shared_ptr<blackhole::logger_t> s_elliptics_logger;

// File names and severity are saved to reset logger when receiving SIGHUP.
std::string s_app_log_file;
std::string s_elliptics_log_file;
int s_severity_min = 4;

inline int gettid()
{
    return syscall(SYS_gettid);
}

// TODO: Move this into configuration file.
const char * const json_base = R"END(
{
  "root": [
    {
      "type": "blocking",
      "formatter": {
        "type": "string",
        "sevmap": ["DEBUG", "NOTICE", "INFO", "WARNING", "ERROR"],
        "pattern": "{timestamp} {process}/{lwp} {severity:s}: {message}, attrs: [{...}]"
      },
      "sinks": [
        {
          "type": "asynchronous",
          "factor": 20,
          "overflow": "drop",
          "sink": {
            "type": "file",
            "flush": 1
          }
        }
      ]
    }
  ]
}
)END";

blackhole::root_logger_t make_root_logger(
        const std::string & file, int severity_min)
{
    auto registry = blackhole::registry::configured();

    auto json = [&]() {
        // Parse base JSON into DOM document and set file path.
        rapidjson::Document doc;
        if (doc.Parse(json_base).HasParseError())
            throw std::logic_error{"make_root_logger: unable to parse json_base"};

        rapidjson::Value val;
        val.SetString(file.c_str(), file.size());

        doc["root"][0]["sinks"][0]["sink"].AddMember("path", val, doc.GetAllocator());

        // Serialize document into string buffer.
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        return std::string{buffer.GetString(), buffer.GetSize()};
    }();

    blackhole::root_logger_t root = registry->builder<blackhole::config::json_t>(
            std::istringstream{json}).build("root");

    root.filter([severity_min](const blackhole::record_t &record) {
        return record.severity() >= severity_min;
    });

    return root;
}

void reset_logger()
{
    if (s_app_log_file.empty())
        return;

    {
        auto root = make_root_logger(s_app_log_file, s_severity_min);
        *s_logger = std::move(root);
    }

    if (s_elliptics_log_file.empty())
        return;

    if (s_elliptics_log_file == s_app_log_file) {
        // Pointers are already allocated and have the same value.
        return;
    }

    {
        auto root = make_root_logger(s_elliptics_log_file, s_severity_min);
        *s_elliptics_logger = std::move(root);
    }
}

void log_error(const char *text, int errnum)
{
    char buf[32] = {'\0'};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    strerror_r(errnum, buf, sizeof(buf));
#pragma GCC diagnostic pop

    syslog(LOG_ERR, "%s%s", text, buf);
}

void *sighup_thread_func(void *arg)
{
    pthread_setname_np(pthread_self(), "logger_sighup");

    sigset_t set;

    // Block all signals.
    sigfillset(&set);
    int rc = sigprocmask(SIG_BLOCK, &set, nullptr);

    if (rc < 0) {
        log_error("stopping SIGHUP handling thread: sigprocmask: ", errno);
        return nullptr;
    }

    sigemptyset(&set);
    sigaddset(&set, SIGHUP);

    while (1) {
        int signum = 0;
        rc = sigwait(&set, &signum);

        if (rc != 0) {
            log_error("stopping SIGHUP handling thread: sigwait: ", rc);
            break;
        }

        reset_logger();
    }

    return nullptr;
}

int block_sighup()
{
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGHUP);

    int rc = sigprocmask(SIG_BLOCK, &set, nullptr);
    if (rc < 0) {
        log_error("not starting SIGHUP handling thread: sigprocmask: ", errno);
        return -1;
    }

    return 0;
}

void unblock_sighup()
{
    sigset_t set;

    sigemptyset(&set);
    sigaddset(&set, SIGHUP);

    int rc = sigprocmask(SIG_UNBLOCK, &set, nullptr);
    if (rc < 0)
        log_error("failed to unblock SIGHUP: sigprocmask: ", errno);
}

void start_sighup_thread()
{
    int rc = block_sighup();

    if (rc < 0)
        return;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_t thread;
    rc = pthread_create(&thread, &attr, sighup_thread_func, nullptr);

    if (rc < 0) {
        log_error("failed to create SIGHUP handling thread: ", errno);
        unblock_sighup();
        return;
    }
}

} // unnamed namespace

namespace app {

namespace logging {

void init_logger(const std::string & app_log_file,
        const std::string &elliptics_log_file, int severity_min)
{
    start_sighup_thread();

    s_app_log_file = app_log_file;
    s_elliptics_log_file = elliptics_log_file;
    s_severity_min = severity_min;

    {
        auto root = make_root_logger(app_log_file, severity_min);
        s_logger.reset(new blackhole::root_logger_t{std::move(root)});
    }

    if (app_log_file != elliptics_log_file) {
        auto root = make_root_logger(elliptics_log_file, severity_min);
        s_elliptics_logger.reset(new blackhole::root_logger_t{std::move(root)});
    } else {
        s_elliptics_logger = s_logger;
    }
}

blackhole::logger_t & logger()
{
    return *s_logger;
}

blackhole::logger_t & elliptics_logger()
{
    return *s_elliptics_logger;
}

DefaultAttributes::DefaultAttributes()
    : blackhole::scope::holder_t(logger(), {{"lwp", gettid()}})
{}

} // namespace logging

} // namespace app

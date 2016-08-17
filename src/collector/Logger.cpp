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

#include <sys/syscall.h>
#include <unistd.h>

namespace {

std::shared_ptr<blackhole::logger_t> s_logger;
std::shared_ptr<blackhole::logger_t> s_elliptics_logger;

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

} // unnamed namespace

namespace app {

namespace logging {

void init_logger(const std::string & app_log_file,
        const std::string &elliptics_log_file, int severity_min)
{
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

set(COLLECTOR_SOURCES_COMMON
    Backend.cpp
    Collector.cpp
    Config.cpp
    ConfigParser.cpp
    Couple.cpp
    Discovery.cpp
    Filter.cpp
    FilterParser.cpp
    FS.cpp
    Group.cpp
    GroupHistoryEntry.cpp
    Host.cpp
    Inventory.cpp
    Job.cpp
    Logger.cpp
    Metrics.cpp
    Namespace.cpp
    Node.cpp
    Parser.cpp
    Round.cpp
    StatsParser.cpp
    Storage.cpp
    )

add_library(collector_common STATIC ${COLLECTOR_SOURCES_COMMON})
target_link_libraries(collector_common ${COLLECTOR_LIBRARIES})

add_executable(mastermind-collector
    Clock.cpp
    WorkerApplication.cpp
    main.cpp
    )

target_link_libraries(mastermind-collector collector_common)

add_custom_target(tarball ALL
    COMMAND tar czf mastermind-collector.tar.gz mastermind-collector
    DEPENDS mastermind-collector)

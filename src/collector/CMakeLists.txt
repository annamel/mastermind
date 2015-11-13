add_executable(mastermind-collector
    Discovery.cpp Node.cpp Parser.cpp Host.cpp
    StatsParser.cpp Metrics.cpp Namespace.cpp
    Storage.cpp Group.cpp Couple.cpp
    ConfigParser.cpp WorkerApplication.cpp CocaineHandlers.cpp
    FS.cpp FilterParser.cpp Backend.cpp
    Filter.cpp Round.cpp Collector.cpp Job.cpp main.cpp
    GroupHistoryEntry.cpp
    Inventory.cpp
    )

target_link_libraries(mastermind-collector
    ${elliptics_cpp_LIBRARY} ${elliptics_client_LIBRARY}
    ${CocaineNative_LIBRARIES} ${Cocaine_LIBRARY}
    ${CURL_LIBRARIES} boost_system boost_thread msgpack rt
     dispatch pthread_workqueue kqueue ev mongoclient)

add_custom_target(tarball ALL
    COMMAND tar czf mastermind-collector.tar.gz mastermind-collector
    DEPENDS mastermind-collector)

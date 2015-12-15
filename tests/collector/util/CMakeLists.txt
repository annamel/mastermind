add_library(util STATIC
    app.cpp
    Clock.cpp
    StorageSnapshot.cpp
    StorageUpdater.cpp
    )

target_link_libraries(util collector_common)

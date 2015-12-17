add_library(util STATIC
    app.cpp
    )

target_link_libraries(util ${COLLECTOR_LIBRARIES})

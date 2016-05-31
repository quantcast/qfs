add_library (qfsmonitorplugin SHARED qfs_monitor.cc)
add_dependencies (qfsmonitorplugin qcdio-shared)
target_link_libraries (qfsmonitorplugin qcdio-shared)
set_target_properties (qfsmonitorplugin PROPERTIES OUTPUT_NAME "qfs_monitor")

project(projectCAF)
cmake_minimum_required(VERSION 3.5.1)
set(CMAKE_CXX_STANDARD 14)
set(CAF_INCLUDE_ALL /usr/local/include/caf)
set(RAPIDJSON_INCLUDE_ALL /usr/local/include/rapidjson)

include_directories(${CAF_INCLUDE_ALL})
include_directories(${RAPIDJSON_INCLUDE_ALL})
link_directories(/usr/local/lib)


macro(add executable folder source)
  add_executable(${executable} ${folder}/${source}.cpp )

  target_link_libraries(${executable} "libcaf_core.so")
  target_link_libraries(${executable} "libcaf_io.so")
endmacro()


add(dist_test . dist)
add(dist_test2 stream_dist dist)



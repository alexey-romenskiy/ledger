cmake_minimum_required(VERSION 3.26.3)
project(broker C)

set(CMAKE_C_STANDARD_REQUIRED 17)

add_definitions(-D_GNU_SOURCE)

find_package(PkgConfig REQUIRED)
pkg_check_modules(ZLIB REQUIRED IMPORTED_TARGET zlib)
pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)
pkg_check_modules(OPENSSL REQUIRED IMPORTED_TARGET openssl)

add_executable(broker main.c utils.c)

target_link_libraries(broker PRIVATE PkgConfig::ZLIB)
target_link_libraries(broker PRIVATE PkgConfig::LIBURING)
target_link_libraries(broker PRIVATE PkgConfig::OPENSSL)

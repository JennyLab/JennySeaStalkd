cmake_minimum_required(VERSION 3.10)
project(JennySeaStalkd CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Encuentra el paquete Seastar.
# Es posible que necesites ajustar CMAKE_PREFIX_PATH si Seastar
# no está instalado en una ruta estándar o no usaste `make install`.
set(Seastar_DIR "/opt/seastar/build/release")
find_package(Seastar REQUIRED)

add_executable(hellokitty test_hellokitty.cpp)
add_executable(beanstar_server beanstar_server.cpp)

# Enlaza con las bibliotecas de Seastar
target_link_libraries(hellokitty PRIVATE Seastar::seastar)
target_link_libraries(beanstar_server PRIVATE Seastar::seastar)

# Seastar recomienda ciertas flags de compilación
target_compile_options(hellokitty PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter # Seastar usa [[maybe_unused]] pero a veces hay warnings

)

# Seastar recomienda ciertas flags de compilación
target_compile_options(beanstar_server PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter # Seastar usa [[maybe_unused]] pero a veces hay warnings
    
)



# Si usas DPDK con Seastar, podrías necesitar flags adicionales
if(Seastar_WITH_DPDK)
    message(STATUS "Compilando con soporte DPDK para Seastar")
    # target_link_libraries(hellokitty PRIVATE Seastar::dpdk)
    # target_link_libraries(beanstar_server PRIVATE Seastar::dpdk)
endif()

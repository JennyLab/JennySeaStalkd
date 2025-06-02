#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh> // Para seastar::cout y seastar::endl
#include <seastar/util/log.hh>    // Para el logger

// Logger específico para nuestra aplicación
seastar::logger app_logger("hello_kitty");

int main(int argc, char** argv) {
    seastar::app_template app; // Objeto base de la aplicación Seastar

    // Configura opciones de la aplicación si es necesario
    // app.add_options()("mi_opcion", "descripción de mi opción");

    // Ejecuta la aplicación. La lambda contiene la lógica principal.
    return app.run(argc, argv, [] () -> seastar::future<> {
        // Esto se ejecuta en el primer shard/core por defecto
        // después de que Seastar haya inicializado su reactor.

        app_logger.info("¡Hola desde Seastar en el shard {}!", seastar::this_shard_id());

        // seastar::cout es el equivalente asíncrono de std::cout
        co_await seastar::smp::invoke_on_all([] {
            // Esta lambda se ejecutará en cada core que Seastar maneje
            app_logger.info("¡Hello From core {}!", seastar::this_shard_id());
            seastar::print("Hello Kitty!!! from Seastar in tha core %u!\n", seastar::this_shard_id());
        });

        // Indica que la función principal ha terminado exitosamente.
        // Para servidores que corren indefinidamente, aquí devolverías un future
        // que se resuelve cuando el servidor deba parar.
        co_return seastar::make_ready_future<>();
    });
}

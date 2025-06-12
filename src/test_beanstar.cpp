#include <boost/asio.hpp>
#include <iostream>

int main() {
    using namespace boost::asio;
    io_context io;
    ip::tcp::socket socket(io);
    socket.connect({ip::address::from_string("127.0.0.1"), 11300});
    
    std::string put_cmd = "put 0 0 60 11\r\nhola mundo\r\n";
    write(socket, buffer(put_cmd));

    boost::asio::streambuf response;
    read_until(socket, response, "\r\n");
    std::istream is(&response);
    std::string line;
    std::getline(is, line);
    std::cout << "Respuesta: " << line << "\n";

    return 0;
}

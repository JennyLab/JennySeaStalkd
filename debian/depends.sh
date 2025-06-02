#!/bin/bash


function version_assert() {
    echo -n "Invalid version: "
    head -n1 /etc/issue
    exit 0
}

version=`cat /etc/issue`
uid=`id -u`

grep -q "Debian GNU/Linux 13" /etc/issue || version_assert
if [[ "${uid}" != "0" ]];
then
    echo "Use root or sudo"
    exit 0
fi



printf "Update Debian...\t\t"
apt-get update 2>&1 1>>/dev/null && echo "[OK]" || echo "[ERROR]"
printf "Upgrade Debian...\t\t"
apt-get upgrade -yqf 2>&1 1>>/dev/null && echo "[OK]" || echo "[ERROR]"

echo -n "Install packages... "
apt-get install -yqf ninja-build ragel libhwloc-dev libnuma-dev git libpciaccess-dev libprotobuf-dev protobuf-compiler \
    libcrypto++-dev libaio-dev libcap-dev libtool cmake g++ gcc libgnutls28-dev liblz4-dev libsctp-dev xfslibs-dev \
    systemtap-sdt-dev libyaml-cpp-dev 2>&1 1>>/dev/null && echo "[OK]" || echo "[ERROR]"

sleep 2
clear
cd /opt
git clone --recursive https://github.com/scylladb/seastar.git /opt/seastar
cd /opt/seastar
echo -n "CMake Seastar... "
cmake -G Ninja -DSEASTAR_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo . 2>&1 1>>/dev/null && echo "[OK]" || echo ["ERROR"]
sleep 2
clear
echo -n "Compile with ninja... "
sleep 1
ninja 2>&1 1>>/dev/null && echo "[OK]" || echo ["ERROR"]
sleep 5
clear
echo "Geat!! Compile Seastar... Go to Make Your Code ;)"

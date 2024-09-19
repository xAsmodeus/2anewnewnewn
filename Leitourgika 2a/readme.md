# Bank System Project

gcc -o central_server central_server.c -lpthread
gcc -o branch_server branch_server.c -lpthread
gcc -o client client.c
gcc -o process_load process_load.c


./central_server
./branch_server 1
./branch_server 2
./client

./process_load 1 load_department_1.dat &
./process_load 2 load_department_2.dat &

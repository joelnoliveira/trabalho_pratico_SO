all:	system sensor
system:	system_manager.o
	gcc -Wall -Wextra system_manager.o -o system
sensor:	sensor.o shared_functions.o
	gcc -Wall -Wextra sensor.o shared_functions.o -o sensor
system_manager.o:	system_manager.c
	gcc -Wall -Wextra system_manager.c -c
sensor.o:	sensor.c shared_functions.h
	gcc -Wall -Wextra sensor.c -c
shared_functions.o: shared_functions.c
	gcc -Wall -Wextra shared_functions.c -c
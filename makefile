all:	home_iot sensor user_console
home_iot:	system_manager.o
	gcc -Wall -Wextra -pthread system_manager.o -o home_iot
sensor:	sensor.o shared_functions.o
	gcc -Wall -Wextra sensor.o shared_functions.o -o sensor
user_console: user_console.o shared_functions.o
	gcc -Wall -Wextra user_console.o shared_functions.o -o user_console
system_manager.o:	system_manager.c
	gcc -Wall -Wextra system_manager.c -c
sensor.o:	sensor.c shared_functions.h
	gcc -Wall -Wextra sensor.c -c
user_console.o:	user_console.c shared_functions.h
	gcc -Wall -Wextra user_console.c -c
shared_functions.o: shared_functions.c
	gcc -Wall -Wextra shared_functions.c -c
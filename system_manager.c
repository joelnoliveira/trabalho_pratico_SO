#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define SENSOR_PIPE "SENSOR_PIPE"
#define CONSOLE_PIPE "CONSOLE_PIPE"

int main(int argc, char const *argv[]){
    /*if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        perror("Cannot create CONSOLE_PIPE: ");
        exit(0);
    }*/
    if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        perror("Cannot create SENSOR_PIPE: ");
        exit(0);
    }
    // Opens the CONSOLE_PIPE for reading
    int console_fd,sensor_fd;
    /*if ((console_fd = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
        perror("Cannot open CONSOLE_PIPE for reading: ");
        exit(0);
    }*/
    // Opens the SENSOR_PIPE for reading
    if ((sensor_fd = open(SENSOR_PIPE, O_RDONLY)) < 0) {
        perror("Cannot open SENSOR_PIPE for reading: ");
        exit(0);
    }
    const char s[77];
    while (1){
        if(read(sensor_fd,s,sizeof(s))==-1)printf("Erro\n");
        printf("Recived from pipe: %s\n",s);
    }
    return 0;
}

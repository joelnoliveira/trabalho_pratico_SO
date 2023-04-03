#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

int main(int argc, char const *argv[]){
    /*
    *argv[1]= id;
    *argv[2]= sleep_time;
    *argv[3]= chave;
    *argv[4]= min;
    *argv[5]= max;
    */
   if(argc < 6){
    printf("Sintaxe do comando: \n$ sensor {identificador do sensor} {intervalo entre envios em segundos(>=0)} {chave} {valor inteiro mínimo a ser enviado} {valor inteiro maximo a ser enviado}\n");
    exit(0);
    }
   int value,fd;
   srand(time(NULL));
   if ((fd = open("SENSOR_PIPE", O_WRONLY)) < 0) {
        perror("Cannot open pipe for writing");
        exit(0);
    }
   while (1){
    //value = (rand()%(*argv[5]-*argv[4]+1))-*argv[4];
    value = 1;
    write(fd,&value,sizeof(int));
    sleep(2);
   }
    return 0;
}

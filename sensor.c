#include <stdio.h>
//#include <stdlib.h> já esta incluido em shared_functions.c
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include "shared_functions.h"
#include <time.h>

unsigned int msg_count = 0;
int fd;

void sigpipe_and_sigint_handler(){
    close(fd);
    exit(0);
}
void sigtstp_handler(){
    printf("Total mensagens enviadas: %d\n",msg_count);
}
int main(int argc, char const *argv[]){
    /*
    *argv[1]= id;
    *argv[2]= sleep_time;
    *argv[3]= chave;
    *argv[4]= min;
    *argv[5]= max;
    */
   
   if(argc < 6 || !is_int(1,argv[2]) || !is_int(0,argv[4]) || !is_int(0,argv[5])){
    printf("Sintaxe do comando: \n$ sensor {identificador do sensor} {intervalo entre envios em segundos(>=0)} {chave} {valor inteiro mínimo a ser enviado} {valor inteiro maximo a ser enviado}\n");
    exit(0);
    }
   if (!checker(0,argv[1])){
    printf("O identificador de um sensor deve ser um código alfanumérico com um tamanho mínimo de 3 caracteres e tamanho máximo de 32 caracteres!\n");
    exit(0);
   }
   if (!checker(1,argv[3])){
    printf("A chave é uma string com tamanho entre 3 e 32 caracteres (inclusive) formada por uma combinação de dígitos, caracteres alfabéticos e underscore (_)!\n");
    exit(0);
   }
   

   int value,min,max,sleep_time;
   struct sigaction new,new2;
   min = atoi(argv[4]);
   max = atoi(argv[5]);
   sleep_time = atoi(argv[2]);
   srand(time(NULL));

   new.sa_handler = sigtstp_handler;
   new2.sa_handler = sigpipe_and_sigint_handler;
   new.sa_flags = new2.sa_flags = 0;
   sigaddset(&new.sa_mask,SIGTSTP);
   sigfillset(&new2.sa_mask);
   sigaction(SIGPIPE,&new2,NULL);
   sigaction(SIGINT,&new2,NULL);
   sigaction(SIGTSTP,&new,NULL);

   if ((fd = open("SENSOR_PIPE", O_WRONLY)) < 0) {
        perror("Cannot open SENSOR_PIPE for writing");
        exit(0);
   }

   int write_return;
   char string[67];//tamanho max visto que tanto o id como a chave só podem ter no max 32 car cada um +'/0' + 2*'#'
   sprintf(string,"%s#%s#",argv[1],argv[3]);
   char temp[77];//tamanho da string + 10 caracteres que são o max espaço possível ocupado por um int + '/0' + 2*'#'

   while (1){
    value = (rand()%(max-min+1))+min;
    sprintf(temp,"%s%d",string,value);
    write_return =  write(fd,&temp,strlen(temp)+1);//includes '\0'
    if(write_return==-1)printf("SENSOR_PIPE writing error\n");
    msg_count++;
    sleep(sleep_time);
   }
    return 0;
}
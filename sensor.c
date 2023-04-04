#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <signal.h>

unsigned int msg_count = 0;
int fd;

/// @brief checker will check if the string is whitin size limit wich are predefined and if it is alphanumeric
/// @param accepts_underline if > 0 accpets non-alphanumeric character '_' 
/// @return 1 if the string checks, otherwise 0
int checker(short int accepts_underline,const char string[]){
    int lenght = strlen(string);//string lenght
    if(lenght>32 || lenght<3)return 0;
    for (int i = 0; i < lenght; i++){
        if(!isalnum(string[i])){
            if (accepts_underline){
                if(string[i]!='_')return 0;
            }else return 0;
        }
    }
    return 1;
    
}
int is_int(short int is_unsigned,const char string[]){
    int lenght = (int)strlen(string);
    long int value = atol(string);

    if(!is_unsigned){
        if (value < INT_MIN || value > INT_MAX)return 0;
    }else if (value < 0 || value > UINT_MAX)return 0;//UINT_MAX from limits.h contains the max number for a unsigned int
    
    for (int i = 0; i < lenght; i++){
        if(string[i]<48 || string[i]>57){
            if(i==0){//ex.: because in i=0 could be a '-' from a negative number
                if (lenght <= 1)return 0;// so it doesn't accept only '-'
                else if (!(string[i]=='-' || string[i]== '+'))return 0;
            }else return 0;
        }
    }

    return 1;
}
void sigtstp_handler(int signo){
    close(fd);
    printf("Total mensagens enviadas: %d\n",msg_count);
    exit(0);
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
   struct sigaction new;
   min = atoi(argv[4]);
   max = atoi(argv[5]);
   sleep_time = atoi(argv[2]);
   srand(time(NULL));

   new.sa_handler = sigtstp_handler;
   new.sa_flags = 0;
   sigaddset(&new.sa_mask,SIGTSTP);
   sigaction(SIGTSTP,&new,NULL);

   if ((fd = open("SENSOR_PIPE", O_WRONLY)) < 0) {
        perror("Cannot open pipe for writing");
        exit(0);
   }

   char string[66];//tamanho max visto que tanto o id como a chave só podem ter no max 32 car cada um
   sprintf(string,"%s#%s#",argv[1],argv[3]);
   char temp[77];//tamanho da string + '\0' + 10 caracteres que são o max espaço possível ocupado por um unsigned int 
   while (1){
    value = (rand()%(max-min+1))+min;
    sprintf(temp,"%s%d\0",string,value);
    write(fd,&temp,strlen(temp)+1);//+1 por causa de '/0'
    msg_count++;
    sleep(sleep_time);
   }
    return 0;
}

/*Trabalho Elaborado por:
Joel Oliveira 2021215037
João Querido  2021240660
*/
#include "shared_functions.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <pthread.h>
#include <stdbool.h>

#define DEBUG

typedef struct {
long mtype;
char content[89];//id (max 32 char)+5*10 (max int = 10 char)+ 5 spaces + '\n'
}MSG;

int fd,msqid;
pthread_t receiver;
long mtype;

void *receive(){
    MSG msg;
    while (1){
        if(msgrcv(msqid, &msg, sizeof(msg)-sizeof(long),mtype, 0)==-1){
            perror("Error reciving message");
            exit(0);
        }
        printf("%s\n",msg.content);
    }
    return NULL;
}
void create_thread(){
    int erro;
    erro = pthread_create(&receiver,NULL,receive,NULL);
    if(erro != 0){
        printf("%s\n",strerror(erro));
    }
}
void sigpipe_and_sigint_handler(int signo){
    if(signo==SIGPIPE)printf("Error: Broken Pipe\n");
    close(fd);
    close(msqid);
    pthread_cancel(receiver);
    pthread_join(receiver,NULL);
    exit(0);
}
int main(int argc, char const *argv[]){
    //argv[1] = id

    if(argc < 2){
        printf("Sintaxe do comando a executar na linha de comando:\n$ user_console {identificador da consola}\n");
        exit(0);
    }
    if(!is_int(-1,argv[1])){
        printf("O parâmetro identificador da consola tem de ser um valor inteiro superior a 0\n");
        exit(0);
    }
    
    if ((fd = open("CONSOLE_PIPE", O_WRONLY)) < 0) {
        perror("Cannot open CONSOLE_PIPE for writing");
        exit(0);
    }
    key_t key = ftok("user_console.c",'B');
    if (key==-1){
        perror("Error on ftok key generation");
        exit(0);
    }
    
    msqid = msgget(key,IPC_CREAT|0700);
    if(msqid == -1){
        perror("Cannot get message queue");
        close(fd);
        exit(0);
    }
    struct sigaction new;
    new.sa_handler = sigpipe_and_sigint_handler;
    new.sa_flags = 0;
    sigfillset(&new.sa_mask);
    sigaction(SIGPIPE,&new,NULL);
    sigaction(SIGINT,&new,NULL);
    mtype = atol(argv[1]);
    create_thread();
    bool accepted;
    int write_return;
    const char car[] = " ";
    char input[100];
    char output[123];//tamanho máx possível:id da consola(max 32 carac)#function()[0-6]#[id](max 32 caracteres)#[chave](max 32 caracteres)#
    //[min](inteiro 2^31, comprimento max = 10 caracteres)#[max](inteiro) + '/0'
    const char menu[]="-> exit - Sai do User Console\n-> stats - Apresenta estatísticas referentes aos dados enviados pelos sensores\n-> reset - Limpa todas as estatísticas calculadas até ao momento pelo sistema (relativa a todos os sensores, criados por qualquer User Console)\n-> sensors - Lista todos os Sensors que enviaram dados ao sistema\n-> add_alert [id] [chave] [min] [max] - Adiciona uma nova regra de alerta ao sistema\n-> remove_alert [id] - Remove uma regra de alerta do sistema\n-> list_alerts - Lista todas as regras de alerta que existem no sistema\n";

    #ifdef DEBUG
    printf("Everything seems alright, let's start reading some commands!\n");
    #endif
    printf("%s",menu);
    while (1){
        fgets(input,100,stdin);
        if(strncmp("stats",input,5)==0){
            sprintf(output,"%s#1",argv[1]);
            accepted = 1;
        }else if (strncmp("reset",input,5)==0){
            sprintf(output,"%s#2",argv[1]);
            accepted = 1;
        }else if (strncmp("sensors",input,7)==0){
            sprintf(output,"%s#3",argv[1]);
            accepted = 1;
        }else if (strncmp("add_alert",input,9)==0){
            char *s1,*s2,*s3,*s4;
            char sintaxe[] = "Verifique a sintaxe: add_alert [id] [chave] [min] [max]\n";
            strtok(input, car);//ignores "add_alert"
            s1 = strtok(0, car);
            if (s1 == 0 || !checker(0,s1)){//token == 0 so if it is no more input
                printf("O id de um alerta é um código alfanumérico com um tamanho mínimo de 3 caracteres e máximo de 32 caracteres!\n");
                printf("%s",sintaxe);
                continue;
            }
            
            s2 = strtok(0, car);
            if (s2 == 0 ||!checker(1,s2)){
                printf("A chave é uma string com tamanho entre 3 e 32 caracteres (inclusive) formada por uma combinação de dígitos, caracteres alfabéticos e underscore (_)!\n");
                printf("%s",sintaxe);
                continue;
            }
            s3 = strtok(0, car);
            if (s3 == 0 || !is_int(0,s3)){
                printf("[min] tem de ser um inteiro!\n");
                printf("%s",sintaxe);
                continue;
            }
            s4 = strtok(0,"\n");
            if (s4 == 0 || !is_int(0,s4)){
                printf("[max] tem de ser um inteiro!\n");
                printf("%s",sintaxe);
                continue;
            }
            int min,max;
            min = atoi(s3);
            max = atoi(s4);
            if(min > max && max < min){
                printf("[min] não pode ser maior do que [max], nem vice-versa!\n");
                printf("%s",sintaxe);
                continue;
            }
            sprintf(output,"%s#4#%s#%s#%d#%d",argv[1],s1,s2,min,max);
            accepted = 1;
        }else if (strncmp("remove_alert",input,12)==0){
            char sintaxe[] = "Verifique a sintaxe: remove_alert [id]\n";
            char *id;//token
            strtok(input, car);//ignores "remove_alert"
            id = strtok(0,"\n");
            if (id == 0 || !checker(0,id)){//token == 0 so if it is no more input
                printf("O id de um alerta é um código alfanumérico com um tamanho mínimo de 3 caracteres e máximo de 32 caracteres!\n");
                printf("%s",sintaxe);
                continue;
            }
            sprintf(output,"%s#5#%s",argv[1],id);
            accepted = 1;
        }else if (strncmp("list_alerts",input,11)==0){
            sprintf(output,"%s#6",argv[1]);
            accepted = 1;
        }else if (strncmp("exit",input,4)==0){
            sigpipe_and_sigint_handler(SIGTERM);
        }else printf("Bad Command\n%s",menu);
        if (accepted){
            accepted = 0;
            write_return = write(fd,output,strlen(output)+1);//includes '\0'
            if (write_return==-1)printf("user_console %s: error writing on CONSOLE_PIPE\n",argv[1]); 
        }
    }
    
    return 0;
}

#include "shared_functions.h"
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>

#define DEBUG

int main(int argc, char const *argv[]){
    //argv[1] = id

    if(argc < 2){
        printf("Sintaxe do comando a executar na linha de comando:\n$ user_console {identificador da consola}\n");
        exit(0);
    }
    if(!checker(0,argv[1])){
        printf("O identificador da consola deve ser um código alfanumérico com um tamanho mínimo de 3 caracteres e tamanho máximo de 32 caracteres!\n");
        exit(0);
    }
    int fd;
    if ((fd = open("CONSOLE_PIPE", O_WRONLY)) < 0) {
        perror("Cannot open CONSOLE_PIPE for writing");
        exit(0);
    }
    int accepted,write_return;
    const char car[] = " ";
    char s[128];//tamanho máx possível "add_alert"+' '+[id](max 32 caracteres)+' '+[chave](max 32 caracteres)+
    //[min](inteiro 2^31, comprimento max = 10 caracteres)+' '+[max](inteiro)   +'\n' + 32 

    const char menu[]="● exit - Sai do User Console\n● stats - Apresenta estatísticas referentes aos dados enviados pelos sensores\n● reset - Limpa todas as estatísticas calculadas até ao momento pelo sistema (relativa a todos os sensores, criados por qualquer User Console)\n● sensors - Lista todos os Sensors que enviaram dados ao sistema\n● add_alert [id] [chave] [min] [max] - Adiciona uma nova regra de alerta ao sistema\n● remove_alert [id] - Remove uma regra de alerta do sistema\n● list_alerts - Lista todas as regras de alerta que existem no sistema\n";

    #ifdef DEBUG
    printf("Everything seems alright, let's start reading some commands!\n");
    #endif
    printf("%s",menu);
    while (1){
        fgets(s,97,stdin);
        if(strncmp("stats",s,5)==0){
            sprintf(s,"%s#1",argv[1]);
            accepted = 1;
        }else if (strncmp("reset",s,5)==0){
            sprintf(s,"%s#2",argv[1]);
            accepted = 1;
        }else if (strncmp("sensors",s,7)==0){
            sprintf(s,"%s#3",argv[1]);
            accepted = 1;
        }else if (strncmp("add_alert",s,9)==0){
            printf("É um para adicionar um alerta\n");
            char *s1,*s2,*s3,*s4;
            char sintaxe[] = "Verifique a sintaxe: add_alert [id] [chave] [min] [max]\n";
            strtok(s, car);//ignores "add_alert"
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

            s4 = strtok(0, car);
            s4[strlen(s4)-1]=0;// replaces '\n' with '\0'
            if (s4 == 0 || !is_int(0,s4)){
                printf("[max] tem de ser um inteiro!\n");
                printf("%s",sintaxe);
                continue;
            }
            accepted = 1;
            sprintf(s,"%s#4#%s#%s#%s#%s",argv[1],s1,s2,s3,s4);
        }else if (strncmp("remove_alert",s,12)==0){
            char sintaxe[] = "Verifique a sintaxe: remove_alert [id]\n";
            char *id;//token
            strtok(s, car);//ignores "remove_alert"
            id = strtok(0, car);
            if (id == 0 || !checker(0,id)){//token == 0 so if it is no more input
                printf("O id de um alerta é um código alfanumérico com um tamanho mínimo de 3 caracteres e máximo de 32 caracteres!\n");
                printf("%s",sintaxe);
                continue;
            }
            sprintf(s,"%s#5#%s",argv[1],id);
            accepted = 1;
        }else if (strncmp("list_alerts",s,11)==0){
            sprintf(s,"%s#6",argv[1]);
            accepted = 1;
        }else if (strncmp("exit",s,4)==0){
            break;
        }else printf("%s",menu);
        if (accepted){
            write_return = write(fd,s,strlen(s)+1);//+1 to include '\0'
            if (write_return==-1)printf("user_console %s: error writing on CONSOLE_PIPE\n",argv[1]);
            
        }
        accepted = 0;
        
        
        
    }
    
    return 0;
}

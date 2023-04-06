#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#define SENSOR_PIPE "SENSOR_PIPE"
#define CONSOLE_PIPE "CONSOLE_PIPE"
#define CONFIG_FILE "config_file.txt"
#define DEBUG

int console_fd,sensor_fd;
pthread_t sensor_reader,console_reader,dispatcher;
FILE *log_file,*config_file;

struct INTERNAL_QUEUE{
    const char msg[123];
    char *next;
};
struct INTERNAL_QUEUE_HEAD{
    char *front;
    char *prioritised;//the one who points t
};

void *console(void *param){
    // Opens the CONSOLE_PIPE for reading;
    #ifdef DEBUG
    printf("Passou aqui console\n");
    #endif
    if ((console_fd = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
        perror("Cannot open CONSOLE_PIPE for reading: ");
        exit(0);
    }
    int read_return;
    char s[123];
    
    while (1){
        read_return = read(console_fd,s,sizeof(s));
        if(read_return > 0){
            printf("Recived from CONSOLE_PIPE: %s\n",s);
        }else if (read_return == 0)break;
        else printf("Erro\n");
    }
    #ifdef DEBUG
    printf("Console going to exit\n");
    #endif
    //pthread_exit(0);
    return NULL;
    
}
void *sensor(void *param){
    // Opens the SENSOR_PIPE for reading
    #ifdef DEBUG
    printf("Passou aqui sensor\n");
    #endif
    if ((sensor_fd = open(SENSOR_PIPE, O_RDONLY)) < 0) {
        perror("Cannot open SENSOR_PIPE for reading: ");
        exit(0);
    }
    int read_return;
    char s[77];
    
    while (1){
        read_return = read(sensor_fd,s,sizeof(s));
        if(read_return > 0){
            printf("Recived from SENSOR_PIPE: %s\n",s);
        }else if (read_return == 0)break;
        else printf("Erro\n");
    }
    #ifdef DEBUG
    printf("Console going to exit\n");
    #endif
    //pthread_exit(0);
    return NULL;
}
void *dispatch(void *param){

    return NULL;
}
int main(int argc, char const *argv[]){
    if (argc < 2){
        printf("Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n");
        exit(0);
    }
    if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        perror("Cannot create CONSOLE_PIPE: ");
        exit(0);
    }
    if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        perror("Cannot create SENSOR_PIPE: ");
        exit(0);
    }
    config_file = fopen(argv[1],"r");
    if(config_file == NULL){
        perror("config_file error: ");
        exit(0);
    }
    log_file = fopen("log.txt","w");
    if(log_file == NULL){
        perror("log_file error: ");
        exit(0);
    }


    pthread_create(&console_reader,NULL,console,NULL);
    pthread_create(&sensor_reader,NULL,sensor,NULL);
    pthread_create(&dispatcher,NULL,dispatch,NULL);
    
    pthread_join(console_reader,NULL);
    pthread_join(sensor_reader,NULL);
    pthread_join(dispatcher,NULL);
    
    fclose(config_file);
    fclose(log_file);
    return 0;
}

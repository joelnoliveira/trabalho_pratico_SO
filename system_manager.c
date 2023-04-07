#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>

#define SENSOR_PIPE "SENSOR_PIPE"
#define CONSOLE_PIPE "CONSOLE_PIPE"
#define LOG_FILE "log.txt"
#define DEBUG

int console_fd,sensor_fd,cur_queue_sz,cur_n_workers,cur_keys_n,cur_sensors_n,cur_alerts_n;
/// @brief QUEUE_SZ, N_WORKERS, MAX_KEYS, MAX_SENSORS, MAX_ALERTS
int config[5];
pthread_t sensor_reader,console_reader,dispatcher;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t queue_cond = PTHREAD_COND_INITIALIZER;
FILE *log_file,*config_file;

typedef struct INTERNAL_QUEUE{
    char msg[123];
    short int priority;
    struct INTERNAL_QUEUE *next;
}INTERNAL_QUEUE;

typedef struct INTERNAL_QUEUE_HEAD{
    INTERNAL_QUEUE *start;
    INTERNAL_QUEUE *end;
}INTERNAL_QUEUE_HEAD;

INTERNAL_QUEUE_HEAD *head;

void add_to_queue(short int priority,char content[]){
    INTERNAL_QUEUE *ptr,*aux_ptr,*pre_aux_ptr;

    pthread_mutex_lock(&queue_mutex);
    if(cur_queue_sz < config[0] || priority > 0){// if the queue is not full or if it has priority
        while(cur_queue_sz >= config[0]){//if the queue is full but the content has priority it waits unitl the queue has some space left
            #ifdef DEBUG
            pthread_mutex_lock(&print_mutex);
            printf("Priority content waiting for queue to free 1 space\n");
            pthread_mutex_unlock(&print_mutex);
            #endif
            pthread_cond_wait(&queue_cond,&queue_mutex);
            #ifdef DEBUG
            pthread_mutex_lock(&print_mutex);
            printf("Priority content waited and now will procced\n");
            pthread_mutex_unlock(&print_mutex);
            #endif
        }
        ptr = (INTERNAL_QUEUE*)malloc(sizeof(INTERNAL_QUEUE));
        strcpy(ptr->msg,content);
        ptr->priority = priority;
        ptr->next = NULL;

        if (head->start == NULL){//if the queue is empty
            head->start = ptr;
            head->end = ptr;
        }else if(!priority){//if it isn't empty and doesn't have priority it goes to the end
            head->end->next = ptr;
            head->end = ptr;
        }else{
            aux_ptr = head->start;
            pre_aux_ptr = head->start;
            while(aux_ptr->priority > 0){
                pre_aux_ptr = aux_ptr;
                aux_ptr = aux_ptr->next;
            }
            if(aux_ptr != pre_aux_ptr){
                pre_aux_ptr->next = ptr;
                ptr->next = aux_ptr;
            }else{
                head->start = ptr;
                ptr->next = aux_ptr;
            }
        }
        cur_queue_sz++;
    }else{
        #ifdef DEBUG
        printf("QUEUE is FULL\n");
        #endif
        }
    pthread_mutex_unlock(&queue_mutex);
}

/// @brief pops the first element of the queue
short int pop_queue(char dest_string[]){
    INTERNAL_QUEUE *ptr;
    ptr = head->start;
    if (ptr == NULL)return 0;//nothing to pop
    head->start = ptr->next;
    strcpy(dest_string,ptr->msg);
    if(head->start == NULL)head->end = NULL;//if the queue is now empty
    cur_queue_sz--;
    pthread_cond_signal(&queue_cond);
    free(ptr);
    return 1;
} 
void *console(){
    // Opens the CONSOLE_PIPE for reading;
    #ifdef DEBUG
    pthread_mutex_lock(&print_mutex);
    printf("Console thread starting!\n");
    pthread_mutex_unlock(&print_mutex);
    #endif
    time_t t;
    time(&t);
    pthread_mutex_lock(&print_mutex);
    fprintf(log_file,"%s Console thread starting!\n",ctime(&t));
    pthread_mutex_unlock(&print_mutex);
    if ((console_fd = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
        time(&t);
        pthread_mutex_lock(&print_mutex);
        perror("Cannot open CONSOLE_PIPE for reading");
        fprintf(log_file,"%s Cannot open CONSOLE_PIPE for reading: %s",ctime(&t),strerror(errno));
        pthread_mutex_unlock(&print_mutex);
        exit(0);
    }
    #ifdef DEBUG
    pthread_mutex_lock(&print_mutex);
    printf("CONSOLE_PIPE opened!\n");
    pthread_mutex_unlock(&print_mutex);
    #endif
    int read_return;
    char s[123];
    
    while (1){
        read_return = read(console_fd,s,sizeof(s));
        time(&t);
        if(read_return > 0){
            #ifdef DEBUG
            pthread_mutex_lock(&print_mutex);
            printf("Recived from CONSOLE_PIPE: %s\n",s);
            pthread_mutex_unlock(&print_mutex);
            #endif
            add_to_queue(1,s);
        }else if (read_return == -1){
            pthread_mutex_lock(&print_mutex);
            perror("Error reading from CONSOLE_PIPE\n");
            fprintf(log_file,"%s Error reading from CONSOLE_PIPE: %s\n",ctime(&t),strerror(errno));
            pthread_mutex_unlock(&print_mutex);
        }
    }
    return NULL;
    
}
void *sensor(){
    // Opens the SENSOR_PIPE for reading
    #ifdef DEBUG
    pthread_mutex_lock(&print_mutex);
    printf("Sensor thread starting!\n");
    pthread_mutex_unlock(&print_mutex);
    #endif

    time_t t;
    time(&t);
    pthread_mutex_lock(&print_mutex);
    fprintf(log_file,"%s Sensor thread starting!\n",ctime(&t));
    pthread_mutex_unlock(&print_mutex);

    if ((sensor_fd = open(SENSOR_PIPE, O_RDONLY)) < 0) {
        time(&t);
        pthread_mutex_lock(&print_mutex);
        perror("Cannot open SENSOR_PIPE for reading");
        fprintf(log_file,"%s Cannot open SENSOR_PIPE for reading: %s",ctime(&t),strerror(errno));
        pthread_mutex_unlock(&print_mutex);
        exit(0);
    }
    #ifdef DEBUG
    pthread_mutex_lock(&print_mutex);
    printf("SENSOR_PIPE opened!\n");
    pthread_mutex_unlock(&print_mutex);
    #endif
    int read_return;
    char s[77];
    
    while (1){
        read_return = read(sensor_fd,s,sizeof(s));
        time(&t);

        if(read_return > 0){
            #ifdef DEBUG
            pthread_mutex_lock(&print_mutex);
            printf("Recived from SENSOR_PIPE: %s\n",s);
            pthread_mutex_unlock(&print_mutex);
            #endif
            add_to_queue(0,s);
        }else if (read_return == -1){
            pthread_mutex_lock(&print_mutex);
            perror("Error reading from SENSOR_PIPE\n");
            fprintf(log_file,"%s Error reading from SENSOR_PIPE: %s\n",ctime(&t),strerror(errno));
            pthread_mutex_unlock(&print_mutex);
        }
    }
    return NULL;
}
void *dispatch(){
    #ifdef DEBUG
    pthread_mutex_lock(&print_mutex);
    printf("Dispatch thread starting!\n");
    pthread_mutex_unlock(&print_mutex);
    #endif
    time_t t;
    time(&t);
    pthread_mutex_lock(&print_mutex);
    fprintf(log_file,"%s Dispatch thread starting!\n",ctime(&t));
    pthread_mutex_unlock(&print_mutex);

    while (1){
        char s[123];
        int x = pop_queue(s);

        if(x) printf("%s\n",s);
        else{
            #ifdef DEBUG
            pthread_mutex_lock(&print_mutex);
            printf("QUEUE is empty! Nothing to pop!\n");
            pthread_mutex_unlock(&print_mutex);
            #endif
        }
        sleep(10);
    }
    return NULL;
}
void sigint_handler(){
    INTERNAL_QUEUE *ptr;
    time_t t;
    time(&t);
    pthread_mutex_lock(&print_mutex);
    fprintf(log_file,"%s SIGINT recived\n",ctime(&t));
    fclose(config_file);
    pthread_mutex_lock(&queue_mutex);

    while (head->start!=NULL){
        ptr = head->start;
        head->start = ptr->next;
        free(ptr);
    }
    free(head);
    pthread_mutex_destroy(&print_mutex);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_cond);
    close(sensor_fd);
    close(console_fd);
    unlink(SENSOR_PIPE);
    unlink(CONSOLE_PIPE);
    time(&t);
    fprintf(log_file,"%s HOME_IOT SIMULATOR CLOSING\n",ctime(&t));
    fclose(log_file);
    exit(0);
}
int main(int argc, char const *argv[]){
    log_file = fopen(LOG_FILE,"w");
    if(log_file == NULL){
        perror("log_file error");
        exit(0);
    }
    time_t t;
    time(&t);
    fprintf(log_file,"%s HOME_IOT SIMULATOR STARTING\n",ctime(&t));
    if (argc < 2){
        time(&t);
        printf("Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n");
        fprintf(log_file,"%s Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n",ctime(&t));
        fclose(log_file);
        exit(0);
    }
    if(access(argv[1],R_OK)==0){//tests if the file exits and if it has read permissions
        config_file = fopen(argv[1],"r");
    }else{
        time(&t);
        perror("Config_file error");
        fprintf(log_file,"%s Config_file error: %s",ctime(&t),strerror(errno));
        fclose(log_file);
        exit(0);
    }
   
    char s[11];
    for (int i = 0; i < 5; i++){
        if(fgets(s,11,config_file)==NULL){
            time(&t);
            printf("Parâmetros insuficientes no ficheiro de configurações!\n");
            fprintf(log_file,"%s Parâmetros insuficientes no ficheiro de configurações!\n",ctime(&t));
            fclose(log_file);
            fclose(config_file);
            exit(0);
        }
        config[i]=atoi(s);
        if (config[i] < 1){//the last element of the config file (MAX_ALERTS) can be 0
            time(&t);
            if (i<4 || config[i]<0){//but cannot be less than 0
                printf("Existe um ou mais parâmetros incorretos!\n");
                fprintf(log_file,"%s Existe um ou mais parâmetros incorretos!\n",ctime(&t));
                fclose(log_file);
                fclose(config_file);
                exit(0);
            }
        }
        
    }
    
    if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        time(&t);
        perror("Cannot create CONSOLE_PIPE");
        fprintf(log_file,"%s Cannot create CONSOLE_PIPE: %s\n",ctime(&t),strerror(errno));
        fclose(log_file);
        fclose(config_file);
        exit(0);
    }
    if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        time(&t);
        perror("Cannot create SENSOR_PIPE");
        fprintf(log_file,"%s Cannot create SENSOR_PIPE: %s\n",ctime(&t),strerror(errno));
        fclose(log_file);
        fclose(config_file);
        exit(0);
    }
    
    cur_queue_sz = cur_alerts_n = cur_keys_n = cur_n_workers = cur_sensors_n = 0;

    head = (INTERNAL_QUEUE_HEAD*)malloc(sizeof(INTERNAL_QUEUE_HEAD));
    head->start = NULL;
    head->end = NULL;

    pthread_create(&console_reader,NULL,console,NULL);
    pthread_create(&sensor_reader,NULL,sensor,NULL);
    pthread_create(&dispatcher,NULL,dispatch,NULL);
    
    struct sigaction new;

    new.sa_handler = sigint_handler;
    new.sa_flags = 0;
    sigfillset(&new.sa_mask);
    sigaction(SIGINT,&new,NULL);

    pthread_join(console_reader,NULL);
    pthread_join(sensor_reader,NULL);
    pthread_join(dispatcher,NULL);
    return 0;
}

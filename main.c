#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <limits.h>

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
int main(int argc, char const *argv[]){
    int i = is_int(1,"999999999999999999999999999");
    printf("%d\n",i);
    return 0;
}

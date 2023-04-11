/*Trabalho Elaborado por:
Joel Oliveira 2021215037
João Querido  2021240660
*/
#include "shared_functions.h"

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
    if (lenght<1)return 0;
    
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
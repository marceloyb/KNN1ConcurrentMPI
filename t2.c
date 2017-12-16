#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include <string.h>
#include <math.h>

char buffer[50];

char *get_rec(FILE *file){
	int i = 0, j;
	char c;
	for (c = fgetc(file); c != ','; c = fgetc(file)){
		if (c == EOF)
			return EOF;
    if (c == '\n'){
     i = 0;
     c = fgetc(file);
    }
		buffer[i] = c;
    i++;
  }
  return buffer;
}


int main (int argc, char *argv[]){
  // declarações
  int linhadamatrizteste = 0;
  int linhatesteatual;
  int natributos, nlinhasbase = 6746, nlinhasteste = 2248;
  float min;
  float distanciaMinima;
  int i, j, k, id, nproc, procdest;
  MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &id);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  MPI_Status st;
  float atributoi;
  char *atributo = malloc(sizeof(char)*50);
  char *nomearquivobase = (char*)malloc(20 * sizeof(char));
  char *nomearquivoteste = (char*)malloc(20 * sizeof(char));
 	FILE *ARQUIVOBASE;
  FILE *ARQUIVOTESTE;
  nomearquivobase = argv[1];
  nomearquivoteste = argv[2];
  natributos = atoi(argv[3]);
  float matrizbase[nlinhasbase][natributos], matrizteste[nlinhasteste][natributos];

  // processo root
  if (id == 0){
    ARQUIVOBASE = fopen(nomearquivobase, "r");
    ARQUIVOTESTE = fopen(nomearquivoteste, "r");
    for (i = 0; i < nlinhasbase; i++){
      for (j = 0; j < natributos; j++){
        atributo = get_rec(ARQUIVOBASE);
        atributoi = atof(atributo);
        matrizbase[i][j] = atributoi;
      }
    }
    for (i = 0; i < nlinhasteste; i++){
      for (j = 0; j < natributos; j++){
        atributo = get_rec(ARQUIVOTESTE);
        atributoi = atof(atributo);
        matrizteste[i][j] = atributoi;
      }
    }


    for (procdest = 1; procdest < nproc; procdest++){
      MPI_Send(&matrizbase, natributos*nlinhasbase, MPI_FLOAT, procdest, 1, MPI_COMM_WORLD);
      MPI_Send(&matrizteste, natributos*nlinhasteste, MPI_FLOAT, procdest, 1, MPI_COMM_WORLD);
    }

    do{
      for (i = 1; i <= nproc-1; i++){
        k = linhadamatrizteste + i - 1;
        if (k > nlinhasteste){
          break;
        }
        MPI_Send(&k, 1, MPI_INT, i, 3, MPI_COMM_WORLD);
      }

      if (linhadamatrizteste >= nlinhasteste){
        break;
      }

      for(i = 1; i <= nproc-1; i++){
        MPI_Recv(&distanciaMinima, 1, MPI_FLOAT, i, 2, MPI_COMM_WORLD, &st);
        MPI_Recv(&linhatesteatual, 1, MPI_INT, i, 2, MPI_COMM_WORLD, &st);
        printf ("\nlinha %i: %f", linhatesteatual, distanciaMinima);
        linhadamatrizteste++;
        if (linhatesteatual+1 >= nlinhasteste){
          break;
        }
      }
    } while(linhadamatrizteste < nlinhasteste);
  }

  else{
    float soma, minlinha;
    MPI_Recv(&matrizbase, natributos*nlinhasbase, MPI_FLOAT, 0, 1, MPI_COMM_WORLD, &st);
    MPI_Recv(&matrizteste, natributos*nlinhasteste, MPI_FLOAT, 0, 1, MPI_COMM_WORLD, &st);

    do{
      MPI_Recv(&k, 1, MPI_INT, 0, 3, MPI_COMM_WORLD, &st);
      min = 999999;
      if (k > nlinhasteste){
        break;
      }
      for (i = 0; i < nlinhasbase; i ++){
        soma = 0;
        for (j = 0; j < natributos; j++){
          soma = soma + powf((matrizteste[k][j] - matrizbase[i][j]), 2);
        }
        soma = sqrtf(soma);
        if (soma < min){
          min = soma;
        }
      }
      MPI_Send(&min, 1, MPI_FLOAT, 0, 2, MPI_COMM_WORLD);
      MPI_Send(&k, 1, MPI_FLOAT, 0, 2, MPI_COMM_WORLD);
      if (k + nproc > nlinhasteste){
        break;
      }
    }while (k < nlinhasteste);

  }
  MPI_Finalize();
}

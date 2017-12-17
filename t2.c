#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

#define MANAGER 0
#define TAG_ASK_FOR_NUMBER 1
#define TAG_SEND_NUMBER 2
#define TAG_SEND_ANSWER 3
MPI_Datatype MPI_answer, MPI_task;

typedef struct task{
  int k;
  int valid;
}task;

typedef struct answer{
  int linha;
  float min;
}answer;

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

int main (int argc, char*argv[]){
  int id, i, j, k, nproc, procdest, idcomm, controlelinha = 0;
  MPI_Init(&argc, &argv);
  int lengthsa[2], lengthst[2];
  MPI_Datatype typesa[2], typest[2];
  MPI_Aint desloca[2], desloct[2];
  lengthsa[0] = 1; lengthst[0] = 1;
  lengthsa[1] = 1; lengthst[1] = 1;
  desloca[0] = offsetof (answer, linha); desloct[0] = offsetof (task, k);
  desloca[1] = offsetof (answer, min); desloct[1] = offsetof (task, valid);
  typesa[0] = MPI_INT; typest[0] = MPI_INT;
  typesa[1] = MPI_FLOAT; typest[1] = MPI_INT;
  MPI_Type_create_struct(2, lengthsa, desloca, typesa, &MPI_answer);
  MPI_Type_commit(&MPI_answer);
  MPI_Type_create_struct(2, lengthst, desloct, typest, &MPI_task);
  MPI_Type_commit(&MPI_task);
  MPI_Comm_rank(MPI_COMM_WORLD, &id);
  MPI_Comm_size(MPI_COMM_WORLD, &nproc);
  MPI_Status st;


  int linhadamatrizteste = 0;
  int linhatesteatual;
  int natributos, nlinhasbase = 6746, nlinhasteste = 2248;
  float min;
  float distanciaMinima;
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

  // processo gerente
  if (id == 0){
    int sender, *finished, tag;
    task t;
    answer ans;
    finished = (int*)calloc(nproc,sizeof(int));
    // le os arquivos e passa para a memoria
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

    while(controlelinha < nlinhasteste){
      MPI_Recv(&ans, 1, MPI_answer, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
      sender = st.MPI_SOURCE;
      tag = st.MPI_TAG;
      if (tag == TAG_ASK_FOR_NUMBER){
        t.k = linhadamatrizteste;
        linhadamatrizteste++;
        if (t.k < nlinhasteste){
          t.valid = 1;
          MPI_Send(&t, 1, MPI_task, sender, TAG_SEND_NUMBER, MPI_COMM_WORLD);
        }
        else{
          t.valid = 0;
          finished[sender] = 1;
          MPI_Send(&t, 1, MPI_task, sender, TAG_SEND_NUMBER, MPI_COMM_WORLD);
        }
      }
      else if(tag == TAG_SEND_ANSWER){
        printf ("\nlinha %i: %f", ans.linha, ans.min);
        controlelinha++;
      }
    }
    int num_finished;
    finished[0] = 1;
    for (i = 0; i < nproc; i++){
      num_finished += finished[i];
    }
    while (num_finished < nproc){
      MPI_Recv(&ans, 1, MPI_answer, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
      t.valid = 0;
      sender = st.MPI_SOURCE;
      MPI_Send(&t, 1, MPI_task, sender, TAG_SEND_NUMBER, MPI_COMM_WORLD);
      num_finished++;
    }
  }

  // processos trabalhadores
  else{
    answer a;
    task t;
    float soma, minlinha;
    MPI_Recv(&matrizbase, natributos*nlinhasbase, MPI_FLOAT, 0, 1, MPI_COMM_WORLD, &st);
    MPI_Recv(&matrizteste, natributos*nlinhasteste, MPI_FLOAT, 0, 1, MPI_COMM_WORLD, &st);
    while(1){
      min = 999999;
      MPI_Send(&a, 1, MPI_answer, MANAGER, TAG_ASK_FOR_NUMBER, MPI_COMM_WORLD);
      MPI_Recv(&t, 1, MPI_task, MANAGER, TAG_SEND_NUMBER, MPI_COMM_WORLD, &st);

      if (!t.valid){
        break;
      }

      for (i = 0; i < nlinhasbase; i ++){
        soma = 0;
        for (j = 0; j < natributos; j++){
          soma = soma + powf((matrizteste[t.k][j] - matrizbase[i][j]), 2);
        }
        soma = sqrtf(soma);
        if (soma < min){
          min = soma;
        }
      }
      a.min = min;
      a.linha = t.k;
      MPI_Send(&a, 1, MPI_answer, MANAGER, TAG_SEND_ANSWER, MPI_COMM_WORLD);
    }
  }

  MPI_Type_free(&MPI_task);
  MPI_Type_free(&MPI_answer);
  MPI_Finalize();
}

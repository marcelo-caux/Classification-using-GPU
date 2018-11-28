#include <iostream> 
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdlib.h>
#include <locale>
#include <string>
#include <limits>
#include <time.h>
#include <stdio.h>
#include <iomanip>
#include <sys/time.h>

using namespace std;

//------------ Kernel de Processamento
__global__ void Classif(int* d_dados, int* d_class, long dsize, int colsIn, int colsOut) 

{
	int i=(threadIdx.x * colsIn) + (blockIdx.x * blockDim.x * colsIn);
	int o=(threadIdx.x * colsOut) + (blockIdx.x * blockDim.x * colsOut);
	int VlOpen,VlHigh,VlLow,VlClose,classe;

	if (i<=dsize) {
		VlOpen = d_dados[i+1];
		VlHigh = d_dados[i+2];
		VlLow = d_dados[i+3];
		VlClose = d_dados[i+4];
		
		classe=(VlOpen==VlClose ? 512: VlOpen>VlClose ? 256:1024)+(VlLow<VlOpen ? 1:4)+(VlLow<VlClose ? 2:8)+(VlHigh>VlOpen ? 16:64)+(VlHigh>VlClose ? 32:128);

		d_class[o]=d_dados[i];
		d_class[o+1]=classe;
	}
}

//--------------------- Funcoes de tempo --------------------------------
std::string DataHora()
{
  time_t rawtime;
  struct tm * timeinfo;
  char buffer [20];

  time ( &rawtime );
  timeinfo = localtime ( &rawtime );
  strftime (buffer,20,"%F %H-%M-%S",timeinfo);
  return buffer;
}
/* funcao de tempo */
double calcula_tempo(const unsigned long int ini, const unsigned long int fim)
{
  double r;

  if(fim >= ini)
    r = ((double)(fim - ini)) / CLOCKS_PER_SEC;
  else
    r = ((double)( (fim + (unsigned long int)-1) - ini)) / CLOCKS_PER_SEC;
  return r;
}



//------- Classif_paralela:: / std::string ---------------------------

void Classif_GPU(const char * nome, long plins, int nthd, const char * sthd){

	char 			arq[256];
	//char 			arqo[256];
	//std::ifstream 	fin;
	int 			colsIn=5, colsOut=2;
	long 			lins,i, c, dsize, csize;
	//int 			classe,VlOpen,VlHigh,VlLow,VlClose;
	int 			v_blocos,v_threads;
	std::string		sIndice,sVlOpen,sVlHigh,sVlLow,sVlClose;
	unsigned long int 	t_ini;
	unsigned long int 	t_fin;
	unsigned long int 	t_tmp;

	std::string dateStr,fn,fnl,s_threads;
	/*--- define variaveis de tempo -------------*/
	timeval start, end;
	double delta;

	dateStr=DataHora();

	std::cout<<"  <DataHora > = "<<dateStr<<std::endl;

	/* tempo inicial */
  	t_ini  = (unsigned long int) clock;
 	gettimeofday(&start, NULL); //marcador de início do processamento

  	/* -- define as dimensões dos vetores que serão criados em logar de matrizes */
  	/* -- dsize define o tamanho do vetor de dados em função do numero de linhas e colunas*/
  	dsize=plins*colsIn;

   	/* -- csize define o tamanho do vetor de classificacao em função do numero de linhas e colunas*/
  	csize=plins*colsOut;

  	/* -- Cria os vetores que conterão os dados lidos do arquivo e a classificação */
  	int *h_dados;
  	int *h_class;

  	//std::cout<<"dsize= "<< dsize <<" csize= "<< csize<<std::endl;

  	size_t d_nbytes=dsize * sizeof(int);
  	size_t c_nbytes=csize * sizeof(int);

  	cudaMallocManaged ((void**)&h_dados, d_nbytes);
  	cudaMallocManaged ((void**)&h_class, c_nbytes);

  	//h_dados[0]=0;
  	//h_dados[1]=1;
	//std::cout<<"h_dados[0]= "<< h_dados[0] <<" h_dados[1]= "<< h_dados[1]<<std::endl;  	

  	lins=plins-0; 
  	std::cout<<"  <inicializou lns> = "<<lins<<std::endl;
  	/* -----  Abre o arquivo csv e inicia a carga dos vetores ------------------- */
	strcpy(arq,nome);
	ifstream fin(arq);
	if (fin.is_open()) 
	{	  	
	  	t_tmp=(unsigned long int) clock();

	  	/*---  carrega o arquivo no vetor host h_dados e inicializa h_class, transformando valores float em int*/
	  	i=0;
	  	c=0;
	  	while (fin.good())
	  	{
			getline(fin,sIndice,',');
	      	getline(fin,sVlOpen,',');
	      	getline(fin,sVlHigh,',');
	      	getline(fin,sVlLow,',');
	      	getline(fin,sVlClose,'\n');
	      	//std::cout<<"sIndice= "<< sIndice <<"sVlOpen= "<< sVlOpen<<"sVlHigh= "<< sVlHigh<<"sVlLow= "<< sVlLow<<"sVlClose= "<< sVlClose<<std::endl;
	      	//h_dados[i]=std::stoi(sIndice);
	      	h_dados[i]=std::atoi(sIndice.c_str());
	      	//h_dados[i+1]=static_cast<int>(std::stof(sVlOpen,NULL)*100);
	      	h_dados[i+1]=static_cast<int>(std::atof(sVlOpen.c_str())*100);
	      	h_dados[i+2]=static_cast<int>(std::atof(sVlHigh.c_str())*100);
	      	h_dados[i+3]=static_cast<int>(std::atof(sVlLow.c_str())*100);
	      	h_dados[i+4]=static_cast<int>(std::atof(sVlClose.c_str())*100);

	      	h_class[c]=0;
	      	h_class[c+1]=0;
	      	
	      	i+=colsIn;
	      	c+=colsOut;
	    }

	    //std::cout<<"  <Carregou h_dados com "<< i <<" posições e h_class com "<< c << " posicoes"<<std::endl;
	    /*--- Calcula o número de blocos e threads em função do número de registros 
			i = número de posições geradas para o vetor vezes o número de colunas de entrada (colsIn)
			Fixei as threads em 256
			Para processar todas as linhas do arquivo de entrada, plins, uso i/colsIN que tem o mesmo valor de plins
			assim, para 17.000.000 de registros a classificar tremos:
			v_blocos=ceil((85.000.000/5)/256)=66406,26 ==> 66407 blocos
	    ---*/

	    v_threads=nthd;
	    s_threads=std::string(sthd);
		//s_threads = "64";
	    //v_blocos=ceil((i/colsIn)/v_threads);
	    v_blocos=(int)ceil((float)lins/v_threads);
	    //std::cout<<"  <Calculou v_blocos com "<< v_blocos <<" threads com "<< v_threads <<std::endl;

	    /*--- invoca o kernel de classificação ---*/

	    Classif<<<v_blocos,v_threads>>>(h_dados, h_class, dsize, colsIn, colsOut);

	    /*--- copia de volta o vetor de classicação --*/

	    cudaDeviceSynchronize();
	    //std::cout<<"  <Sincronizou -------------------"<<std::endl;

	    fnl="log/Classif_KernelT"+ s_threads +dateStr+".log.txt";
	    //arqo=fnl.c_str();
	  	std::ofstream mylog (fnl.c_str());
	  	//std::ofstream mylog (arqo);
	  	mylog<<"Processado em "<< dateStr <<std::endl;
	  	mylog<<"Processado em "<< v_blocos <<" blocos com "<< v_threads <<" threads"<<std::endl;
		mylog<<"Tempo total de classificaçao (ler CSV e classificar via kernel)= "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;

		/*----   fecha o arquivo de entrada de registros a classificar*/
	    fin.close();
	   	//mylog<<"Tempo decorrido até o final da classificaçao= "<< calcula_tempo(t_ini, (unsigned long int) clock()) <<std::endl;

	   	/*--- cria o nome do arquivo csv de saída com as classificações ----*/
	    //fn="/home/UFF/GPU/Trabalho/Dados/Classif_Kernel"+dateStr+".csv";
	    fn="csv/Classif_KernelT"+ s_threads +dateStr+".csv";
	    //std::cout<<std::endl<<fn <<std::endl;
	    t_tmp=(unsigned long int) clock();

	    /*--- abre o csv de saída ---*/
	    std::ofstream myfile (fn.c_str());
	    myfile<<"Indice,IdClasse"<<std::endl;

	    /*---  exporta o conteúdo do vetor h_class  ---*/
	    for (i=0; i<csize; i+=colsOut)
	  	{
	  		myfile<<h_class[i]<<','<<h_class[i+1]<<"\n";
	  	}
	  	myfile.close();
		mylog<<"Tempo para exportar classificaçao para CSV= "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;

	  	// desaloca a matriz << no Thtrust a desalocação dos vetores é transparente ---------------
		//mylog<<"Tempo para free matriz = "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;	  	
		/* tempo final */
	  	t_fin  = (unsigned long int) clock();
	  	mylog<<"Total de registros classificados= "<< lins <<std::endl;
	  	mylog<<"Tempo total de processamento= "<< setprecision(6) << calcula_tempo(t_ini, t_fin) <<std::endl;

	  	gettimeofday(&end, NULL);
    	delta = ((end.tv_sec  - start.tv_sec) * 1000000u + end.tv_usec - start.tv_usec) / 1.e6;
    	mylog<<"Tempo total de processamento 2 = "<< delta <<std::endl;

	  	mylog.close();
	  	std::cout<<std::endl<<"Tempo total de processamento= "<< calcula_tempo(t_ini, t_fin) <<std::endl;
	  	std::cout<<"Tempo total de processamento 2 = "<< delta <<std::endl;
 	}   
 	else
 	{
 		std::cout<<std::endl<<"Erro na abertura do arquivo "<< nome <<std::endl;
 	}
}



//---------------------------------------------------------------------------
int main(int argc, char * argv[]) 
{   
  long nlin=0;
  int nthd=0;
  if (argc < 4){
    std::cout<<"Digite o nome do arquivo de entrada e a quantidade de registros e quantas threads"<<std::endl;
    abort();
  }
  // File
  std::cout<<"  <Arquivo de entrada> = "<<argv[1]<<std::endl;
   
  //nlin=std::stol(argv[2]);
  nlin=std::atol(argv[2]);
  nthd=std::atoi(argv[3]);
  /* processa a classificaçao */
  std::cout<<"  <Qtd Registros> = "<<nlin<<std::endl;
  Classif_GPU(argv[1],nlin,nthd,argv[3]); 
} 

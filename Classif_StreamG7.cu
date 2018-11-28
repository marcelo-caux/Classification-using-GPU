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
	//int classe;

	if (i<=dsize) {
		VlOpen = d_dados[i+1];
		VlHigh = d_dados[i+2];
		VlLow = d_dados[i+3];
		VlClose = d_dados[i+4];
		
		classe=(VlOpen==VlClose ? 512: VlOpen>VlClose ? 256:1024)+(VlLow<VlOpen ? 1:4)+(VlLow<VlClose ? 2:8)+(VlHigh>VlOpen ? 16:64)+(VlHigh>VlClose ? 32:128);
		//classe=(d_dados[i+1]==d_dados[i+4] ? 512: d_dados[i+1]>d_dados[i+4] ? 256:1024)+(d_dados[i+3]<d_dados[i+1] ? 1:4)+(d_dados[i+3]<d_dados[i+4] ? 2:8)+(d_dados[i+2]>d_dados[i+1] ? 16:64)+(d_dados[i+2]>d_dados[i+4] ? 32:128);

		d_class[o]=d_dados[i];
		//d_class[o]=12;
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
  strftime (buffer,20,"%F %H%M%S",timeinfo);
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
	long 			lins,i, c, last_i_proc, last_c_proc;
	int				dsize, csize, st_dsize, st_csize, partes, st_gatilho;
	//int 			classe,VlOpen,VlHigh,VlLow,VlClose;
	int 			v_blocos,v_threads, streams_processados, d_deslocamento,c_deslocamento;
	std::string		sIndice,sVlOpen,sVlHigh,sVlLow,sVlClose;
	unsigned long int 	t_ini;
	unsigned long int 	t_fin;
	unsigned long int 	t_tmp;
	unsigned long int 	t_tmp1;
	unsigned long int 	t_tmp2;
	unsigned long int 	t_tmp3;
	unsigned long int 	t_tmp4;

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

  	/* ----- Calcula o tamanho dos streams, de acordo com o numero de partes -----------*/
  	partes=40;
  	st_dsize=0;
  	st_csize=0;
  	st_dsize=(int)floor((int)dsize/partes);
  	st_csize=(int)floor((int)csize/partes);

  	/* ----- Calcula o ponto de executar os streams, de acordo com o numero de partes, mas a cada gatilho executa 2 streams -----------*/
  	st_gatilho=(int)floor((int)dsize/partes);
  	st_gatilho*=2;

  	/* -- Cria os vetores que conterão os dados lidos do arquivo e a classificação */
  	int *h_dados;
  	int *h_class;

 	int *d_dados_0;
  	int *d_class_0;
 	int *d_dados_1;
  	int *d_class_1;

  	/*-------------------------- Define os streams ----------------------------------------*/
  	cudaStream_t strm0, strm1;
  	cudaStreamCreate(&strm0);
   	cudaStreamCreate(&strm1);

  	std::cout<<" vai alocar memoria na GPU st_dsize= "<< st_dsize <<" st_csize= "<< st_csize<<std::endl;

 	/*-------------------------- Aloca os vetores no device ----------------------------------------*/
  	cudaMalloc((void**) &d_dados_0, st_dsize * sizeof(int));
  	cudaMalloc((void**) &d_class_0, st_csize * sizeof(int));
 	cudaMalloc((void**) &d_dados_1, st_dsize * sizeof(int));
  	cudaMalloc((void**) &d_class_1, st_csize * sizeof(int));

  	/*-------------------------- Aloca os vetores no host ----------------------------------------*/
 	cudaHostAlloc((void**) &h_dados, dsize*sizeof(int),cudaHostAllocDefault);
 	cudaHostAlloc((void**) &h_class, csize*sizeof(int),cudaHostAllocDefault);

 	lins=plins-0; 
  	std::cout<<"  <inicializou lns> = "<<lins<<std::endl;

  	/*--- pega o num de threads digitadas e calcula os blocos ------------------------- */
    v_threads=nthd;
    s_threads=std::string(sthd);
    v_blocos=(int)ceil((float)(lins/partes)/v_threads);
    std::cout<<"  <Calculou v_blocos com "<< v_blocos <<" threads com "<< v_threads<<" st_gatilho com "<< st_gatilho <<" dsize="<<dsize<<std::endl;


  	/* -----  Abre o arquivo csv e inicia a carga dos vetores ------------------- */
	strcpy(arq,nome);
	ifstream fin(arq);

    t_tmp1=(unsigned long int) clock();

	if (fin.is_open()) 
	{	  	
	  	t_tmp=(unsigned long int) clock();

	  	/*---  carrega o arquivo no vetor host h_dados e inicializa h_class, transformando valores float em int*/
	  	i=0;
	  	c=0;
	  	streams_processados=0;
	  	c_deslocamento=0;
	  	d_deslocamento=0;
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
	      	
	      	//std::cout<<"Indice= "<< h_dados[i] <<"VlOpen= "<< h_dados[i+1]<<"VlHigh= "<< h_dados[i+2]<<"sVlLow= "<< h_dados[i+3]<<"VlClose= "<< h_dados[i+4]<<std::endl;
	  		
	  		/*--- Se atingiu o ponto de transferir os dados (st_gatilho) ou atingiu o último indice de dados -----------
			---- st_dsize-colsOut significa o último registro do stream, st_dsize é o inicio do próximo stream --------
	  		-------------------- copia os vetores e dispara o kernel -------------------------------------------------*/

	      	if ((i>0) && (i<dsize)) {
		  		if ((i % st_gatilho) == 0)
		  		{
		  			c_deslocamento=streams_processados*st_csize;
		  			d_deslocamento=streams_processados*st_dsize;

		  			//std::cout<<"i= "<< i <<" st_dsize= "<< st_dsize<<" d_deslocamento= "<< d_deslocamento<<" c_deslocamento= "<<c_deslocamento<<" streams_processados= "<< streams_processados<<std::endl;

		  			cudaMemcpyAsync(d_dados_0,h_dados+d_deslocamento,st_dsize * sizeof(int),cudaMemcpyHostToDevice, strm0);
		  			cudaMemcpyAsync(d_class_0,h_class+c_deslocamento,st_csize * sizeof(int),cudaMemcpyHostToDevice, strm0);
	    			/*--- invoca o kernel de classificação ---*/
	    			Classif<<<v_blocos,v_threads,0, strm0>>>(d_dados_0, d_class_0, st_dsize, colsIn, colsOut);
		  			cudaMemcpyAsync(h_class+c_deslocamento,d_class_0,st_csize * sizeof(int),cudaMemcpyDeviceToHost, strm0);

	    			streams_processados++;
		  			c_deslocamento=streams_processados*st_csize;
		  			d_deslocamento=streams_processados*st_dsize; 

		  			//std::cout<<"i= "<< i <<" st_dsize= "<< st_dsize<<" d_deslocamento= "<< d_deslocamento<<" c_deslocamento= "<<c_deslocamento<<" streams_processados= "<< streams_processados<<std::endl;		

		  			cudaMemcpyAsync(d_dados_1,h_dados+d_deslocamento,st_dsize * sizeof(int),cudaMemcpyHostToDevice, strm1);
		  			cudaMemcpyAsync(d_class_1,h_class+c_deslocamento,st_csize * sizeof(int),cudaMemcpyHostToDevice, strm1);
	    			/*--- invoca o kernel de classificação ---*/
	    			Classif<<<v_blocos,v_threads,0, strm1>>>(d_dados_1, d_class_1, st_dsize, colsIn, colsOut);
		  			cudaMemcpyAsync(h_class+c_deslocamento,d_class_1,st_csize * sizeof(int),cudaMemcpyDeviceToHost, strm1);

	    			streams_processados++;
	    			last_i_proc=i;
	    			last_c_proc=c;
		  		}
	  		} else {
	  			if (i == dsize) {
		  			c_deslocamento=csize-last_c_proc; //((streams_processados*st_csize)+st_csize);
		  			d_deslocamento=dsize-last_i_proc; //((streams_processados*st_dsize)+st_dsize);

		  			//std::cout<<"i= "<< i <<" st_dsize= "<< st_dsize<<" d_deslocamento= "<< d_deslocamento<<" c_deslocamento= "<<c_deslocamento<<" streams_processados= "<< streams_processados<<std::endl;

		  			cudaMemcpyAsync(d_dados_0,h_dados+d_deslocamento,st_dsize * sizeof(int),cudaMemcpyHostToDevice, strm0);
		  			cudaMemcpyAsync(d_class_0,h_class+c_deslocamento,st_csize * sizeof(int),cudaMemcpyHostToDevice, strm0);
	    			/*--- invoca o kernel de classificação ---*/
	    			Classif<<<v_blocos,v_threads,0, strm0>>>(d_dados_0, d_class_0, st_dsize, colsIn, colsOut);
		  			cudaMemcpyAsync(h_class+c_deslocamento,d_class_0,st_csize * sizeof(int),cudaMemcpyDeviceToHost, strm0);	  				
	  			}
	  		}

	      	i+=colsIn;
	      	c+=colsOut;
	    }

	    std::cout<<"  <Carregou h_dados com "<< i <<" posições e h_class com "<< c << " posicoes"<<std::endl;

	   	t_tmp2=(unsigned long int) clock();

	    std::cout<<"  <Calculou v_blocos com "<< v_blocos <<" lins=" << lins << " threads com "<< v_threads <<std::endl;
	    std::cout<<"  <dsize "<< dsize << " colsIn="<<colsIn<<" colsOut="<< colsOut<<std::endl;
	    t_tmp3=(unsigned long int) clock();
	    cudaStreamSynchronize(strm0);
	    cudaStreamSynchronize(strm1);
	    t_tmp4=(unsigned long int) clock();

	    //std::cout<<"  <Sincronizou -------------------"<<std::endl;

	    fnl="log/Classif_StreamG7-T"+ s_threads +dateStr+".log.txt";
	    //arqo=fnl.c_str();
	  	std::ofstream mylog (fnl.c_str());
	  	//std::ofstream mylog (arqo);
	  	mylog<<"Processado em "<< dateStr <<std::endl;
	  	mylog<<"Processado em "<< v_blocos <<" blocos com "<< v_threads <<" threads, com "<< partes <<" partes"<<std::endl;
		mylog<<"Tempo total de classificaçao (ler CSV e classificar via stream/kernel)= "<< calcula_tempo(t_tmp1, t_tmp2) <<std::endl;
		//mylog<<"Tempo total de cópia host >> device = "<< calcula_tempo(t_tmp1, t_tmp2) <<std::endl;
		mylog<<"Tempo total de Stream Synchronize >> host = "<< calcula_tempo(t_tmp3, t_tmp4) <<std::endl;

		/*----   fecha o arquivo de entrada de registros a classificar*/
	    fin.close();

	   	/*--- cria o nome do arquivo csv de saída com as classificações ----*/
	    //fn="/home/UFF/GPU/Trabalho/Dados/Classif_Kernel"+dateStr+".csv";
	    fn="csv/Classif_StreamT"+ s_threads +dateStr+".csv";
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

	    t_tmp=(unsigned long int) clock();

	  	/*-------------  libera memoria ------------------------*/
	  	cudaFree(d_dados_0);
	  	cudaFree(d_class_0);
	  	cudaFree(d_dados_1);
	  	cudaFree(d_class_1);
	  	cudaFreeHost(h_dados);
	  	cudaFreeHost(h_class);

		mylog<<"Tempo para liberar memoria GPU= "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;

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

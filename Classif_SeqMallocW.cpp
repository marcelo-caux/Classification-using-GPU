
#include <iostream> 
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdlib.h>
#include <locale>
#include <string>
#include <limits>
#include <time.h>
#include <sys/time.h>

using namespace std;

//---------------------------------------------------------------------------
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

//--------------------------------------------------------------------------- Classif_Seq:: / std::string
void Classif_Seq(const char * nome, long plins){

	char 			arq[256];
	//std::ifstream 	fin;
	int 			colsIn=5, colsOut=2;
	long 			Indice,lins,i;
	float 			VlOpen,VlHigh,VlLow,VlClose;
	int 			classe;
	std::string		sIndice,sVlOpen,sVlHigh,sVlLow,sVlClose;
	unsigned long int 	t_ini;
	unsigned long int 	t_fin;
	unsigned long int 	t_tmp;

	std::string dateStr,fn,fnl;
	/*--- define variaveis de tempo -------------*/
	timeval start, end;
	double delta;

	dateStr=DataHora();

	std::cout<<"  <DataHora Inincio > = "<<dateStr<<std::endl;

	/* tempo inicial */
  	t_ini  = (unsigned long int) clock();
  	gettimeofday(&start, NULL); //marcador de início do processamento

  	lins=plins-0; 
  	//std::cout<<"  <inicializou lins> = "<<lins<<std::endl;
	strcpy(arq,nome);
	ifstream fin(arq);
	if (fin.is_open()) 
	{
		//std::cout<<"  <Vai criar Matriz Calssif> = "<<lins<<std::endl;		
		long **Classif = (long**)malloc(lins * sizeof(long*)); //Aloca um Vetor de Ponteiros
		//std::cout<<"  <Matriz Calssif criada> = "<<lins<<std::endl;
	  	
	  	t_tmp=(unsigned long int) clock();

	  	i=0;
	  	while (fin.good())
	  	{
			getline(fin,sIndice,',');
	      	getline(fin,sVlOpen,',');
	      	getline(fin,sVlHigh,',');
	      	getline(fin,sVlLow,',');
	      	getline(fin,sVlClose,'\n');
	      	//std::cout<<"sIndice= "<< sIndice <<"sVlOpen= "<< sVlOpen<<"sVlHigh= "<< sVlHigh<<"sVlLow= "<< sVlLow<<"sVlClose= "<< sVlClose<<std::endl;
	      	Indice=std::atoi(sIndice.c_str());
	      	VlOpen=static_cast<int>(std::atof(sVlOpen.c_str())*100);
	      	VlHigh=static_cast<int>(std::atof(sVlHigh.c_str())*100);
	      	VlLow=static_cast<int>(std::atof(sVlLow.c_str())*100);
	      	VlClose=static_cast<int>(std::atof(sVlClose.c_str())*100);
	      	classe=0;
	      	classe=(VlOpen==VlClose ? 512: VlOpen>VlClose ? 256:1024)+(VlLow<VlOpen ? 1:4)+(VlLow<VlClose ? 2:8)+(VlHigh>VlOpen ? 16:64)+(VlHigh>VlClose ? 32:128);
	      	Classif[i] = (long*) malloc(colsOut * sizeof(long)); //Aloca um Vetor de Inteiros para cada posição do Vetor de Ponteiros.
	      	Classif[i][0]=Indice;
	      	Classif[i][1]=classe;
	      	
	      	i++;
	    }
	    fnl="log/Classif_Seq"+dateStr+".log.txt";
	  	std::ofstream mylog (fnl.c_str());
	  	mylog<<"Processado em "<< dateStr <<std::endl;
		mylog<<"Tempo total de classificaçao (ler CSV e classificar)= "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;
	    fin.close();
	   	//mylog<<"Tempo decorrido até o final da classificaçao= "<< calcula_tempo(t_ini, (unsigned long int) clock()) <<std::endl;
	    fn="csv/Classif_Seq"+dateStr+".csv";
	    //std::cout<<std::endl<<fn <<std::endl;
	    t_tmp=(unsigned long int) clock();
	    std::ofstream myfile (fn.c_str());
	    myfile<<"Indice,IdClasse"<<std::endl;
	    for (i=0; i<lins; ++i)
	  	{
	  		//myfile<<Classif[i][0]<<','<<Classif[i][1]<<std::endl;
	  		myfile<<Classif[i][0]<<','<<Classif[i][1]<<"\n";
	  	}
	  	myfile.close();
		mylog<<"Tempo para exportar classificaçao para CSV= "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;
	  	// desaloca a matriz
	  	t_tmp=(unsigned long int) clock();
	  	for (i=0; i<lins;i++)
	  	{
	  		free(Classif[i]);
	  	}
	  	free(Classif);
		mylog<<"Tempo para free matriz = "<< calcula_tempo(t_tmp, (unsigned long int) clock()) <<std::endl;	  	
		/* tempo final */
	  	t_fin  = (unsigned long int) clock();
	  	mylog<<"Total de registros classificados= "<< lins <<std::endl;
	  	mylog<<"Tempo total de processamento= "<< calcula_tempo(t_ini, t_fin) <<std::endl;

    	gettimeofday(&end, NULL);
    	delta = ((end.tv_sec  - start.tv_sec) * 1000000u + end.tv_usec - start.tv_usec) / 1.e6;
    	mylog<<"Tempo total de processamento 2 = "<< delta <<std::endl;
	  	mylog.close();

	  	dateStr=DataHora();

		std::cout<<"  <DataHora termino > = "<<dateStr<<std::endl;

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
  if (argc < 3){
    std::cout<<"Digite o nome do arquivo de entrada e a quantidade de registros"<<std::endl;
    abort();
  }
  // File
  std::cout<<"  <Arquivo de entrada> = "<<argv[1]<<std::endl;
   
  nlin=std::atol(argv[2]);
  /* processa a classificaçao */
  std::cout<<"  <Qtd Registros> = "<<nlin<<std::endl;
  Classif_Seq(argv[1],nlin); 
} 


# Classification-using-GPU
This repository keeps all code and data used in the experiment of power up a classifier using GPU hardware
All code was write in C++ and CUDA 9.2
Input dataset is CSV, each record represents 1 minute of daily quotes, one day has 1440 records, and has the following format:
  Index long int  << Index number of a Bitcoin quote in original database 
  VlOpen float    << Bitcoin value at second zero of current record
  VlHigh float    << Maximum Bitcoin value along the minute of current record
  VlLow  float    << Minimum Bitcoin value along the minute of current record
  VlClose float   << last Bitcoin value along the minute of current record, at second 59
  
Output dataset is CSV, each record contains the class off one input record, format is:
  Index long int  << Index number of a Bitcoin quote in original database 
  Class int       << Calculated class according to classification rule

How to run programs
  Sequential code
  Compile .cpp using g++ or nvcc >> nvcc source.cpp -o execfile
  ./execfile datafile.csv
  
  Parallel code
  Compile .cpp using nvcc >> nvcc source.cu -o execfile
  ./execfile datafile.csv NumberOfThreads   (can be any number of threads you want, 64, 256 ... up to 1024)

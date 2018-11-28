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

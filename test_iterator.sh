rm ./iterator_model
g++ -O3 -o iterator_model iterator_model.cpp;
prun -np 1 time ./iterator_model
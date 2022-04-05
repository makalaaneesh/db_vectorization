rm ./vectorized_model
g++ -O3 -o vectorized_model vectorized_model.cpp
prun -np 1 time ./vectorized_model
rm ./vectorized_model
g++ -O3 -mavx2 -o vectorized_model vectorized_model.cpp
./vectorized_model
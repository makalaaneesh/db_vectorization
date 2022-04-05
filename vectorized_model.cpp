#include<iostream>
#include <fstream>
#include <sstream>
#include <immintrin.h>

using namespace std;
// Dealing with only ints
// try just select itself should be faster in vector format. less trips to disk
// try a + b
// try a == b
int vectorized_length;

class Tuple{
        public:
        int *integers;
        size_t len;

        void print(){
            printf("[");
            for(size_t i=0; i<len; i++){
                printf("%d,", integers[i]);
            }
            printf("]\n");
        }
};

class Executor{
        public:
        virtual struct Tuple** next() = 0;
};

class SequentialScanExecutor: public Executor  {
        public:
        string filename;
        ifstream fs;

        SequentialScanExecutor(string _filename){
            filename = _filename;
            fs.open(filename);

        }

//        https://stackoverflow.com/questions/14600489/reading-data-file-into-2d-array-c
        struct Tuple** next(){
            Tuple ** results_vector = (struct Tuple **) malloc(sizeof(struct Tuple *) * vectorized_length);
            int v;
            // TODO: read all at once.
            for (v = 0; v < vectorized_length; ++v) {
                string line;
                getline(fs, line);
                if (fs){
                    istringstream iss(line);
                    results_vector[v] = (struct Tuple*) malloc(sizeof(struct Tuple));
                    results_vector[v]->len = 1;
                    results_vector[v]->integers = (int *) malloc(sizeof(int) * 1);
//            sample_tuple->print();
                    for(size_t i = 0; i < 1; i++){
                        iss >> results_vector[v]->integers[i];
                    }
                } else{
                    break;
                }
            }
            if (v == 0){
                return NULL;
            }
            return results_vector;
//            string line;
//            getline(fs, line);
//            if (fs){
//
//                istringstream iss(line);
//                Tuple *sample_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
//                sample_tuple->len = 2;
//                sample_tuple->integers = (int *) malloc(sizeof(int) * 2);
////            sample_tuple->print();
//                for(size_t i = 0; i < 2; i++){
//                    iss >> sample_tuple->integers[i];
//                }
//
//                return sample_tuple;
//            } else{
//                return NULL;
//            }
        }
};

//class SequentialScanMemoryExecutor: public Executor  {
//        public:
//        string filename;
//        int* table;
//        size_t i;
//        size_t len;
//
//        SequentialScanMemoryExecutor(int *_table, size_t _len){
//            table = _table;
//            len = _len;
//            i = 0;
//        }
//
//        struct Tuple** next(){
//            Tuple ** results_vector = (struct Tuple **) malloc(sizeof(struct Tuple *) * vectorized_length);
//            if (i >=len){
//                return NULL;
//            }
//            for (int v = 0; v < vectorized_length; ++v) {
//                results_vector[v] = (struct Tuple*) malloc(sizeof(struct Tuple));
//                results_vector[v]->len = 2;
//                results_vector[v]->integers = (int *) malloc(sizeof(int) * 2);
//                results_vector[v]->integers[0] = table[i++];
//                results_vector[v]->integers[1] = table[i++];
//                if (i>=len) break;
//            }
//
//            return results_vector;
//        }
//};


class AggregationOperationExecutor: public Executor {
        public:
        string op;
        size_t columnindex;
        Executor* childExecutor;
        size_t aggValue;
        size_t len;
        bool computed;

        AggregationOperationExecutor(string _op, size_t _columnindex, Executor* _childExecutor){
            op = _op;
            columnindex = _columnindex;
            childExecutor = _childExecutor;
            aggValue = 0;
            computed = 0;
            len = 0;
        }

        struct Tuple** next(){
            if (computed){
                return NULL;
            }
            Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
            result_tuple->integers = (int *) malloc(sizeof(int) * 1);
            result_tuple->len = 1;
            __m256i aggVector = _mm256_setzero_si256();
            __m256i aggVectorTemp;

            Tuple** input_tuple_vector;
            while (true) {

                input_tuple_vector = childExecutor->next();
                if (input_tuple_vector == NULL){
                    break;
                }
//                for (int v = 0; v < vectorized_length; ++v) {
//                }
                aggVectorTemp = _mm256_set_epi32(input_tuple_vector[0]->integers[columnindex], input_tuple_vector[1]->integers[columnindex], input_tuple_vector[2]->integers[columnindex], input_tuple_vector[3]->integers[columnindex],
                                                 input_tuple_vector[4]->integers[columnindex], input_tuple_vector[5]->integers[columnindex], input_tuple_vector[6]->integers[columnindex], input_tuple_vector[7]->integers[columnindex]);
                aggVector = _mm256_add_epi32(aggVector, aggVectorTemp);
                len += 8;

//                for (int v = 0; v < vectorized_length; ++v) {
//                    if (input_tuple_vector[v] == NULL) break;
////                    input_tuple_vector[v]->print();
//                    aggValue += input_tuple_vector[v]->integers[columnindex];
//                    len++;
////                    free(input_tuple_vector[v]);
//                }
//                free(input_tuple_vector);
            }
            __attribute__ ((aligned (32))) int output[vectorized_length];
//            _mm256_store_epi32(output, aggVector);
//            __m256i ones = _mm256_set1_epi32(1);
//            __m256i mask = _mm256_cmpgt_epi32(aggVector, ones);
//            _mm256_maskstore_epi32(output, mask, aggVector);
            _mm256_store_si256((__m256i*)output, aggVector);
            for (int v = 0; v < vectorized_length; ++v) {
                printf("%d\t", output[v]);
                aggValue += output[v];
            }
            printf("\n");
//            printf("len=%lu\n", len);
            aggValue = aggValue/len;

            result_tuple->integers[0] = aggValue;
            computed = 1;
            Tuple ** results_vector = (struct Tuple **) malloc(sizeof(struct Tuple *) * vectorized_length);
            results_vector[0] = result_tuple;
            return results_vector;
        }
};

//class SelectionExecutor: public Executor {
//        public:
//        size_t* columnIndices;
//        size_t numColumns;
//        Executor* childExecutor;
//
//        SelectionExecutor(size_t* _columnIndices, size_t _numColumns, Executor* _childExecutor){
//            columnIndices = _columnIndices;
//            numColumns = _numColumns;
//            childExecutor = _childExecutor;
//        }
//
//        struct Tuple* next(){
//            Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
//            result_tuple->integers = (int *) malloc(sizeof(int) * numColumns);
//            result_tuple->len = numColumns;
//
//            Tuple* input_tuple = childExecutor->next();
//            if (input_tuple == NULL){
//                return input_tuple;
//            }
//            for(size_t i=0; i<numColumns; i++){
//                result_tuple->integers[i] = input_tuple->integers[columnIndices[i]];
//            }
//            return result_tuple;
//        }`
//};

int main(){
    vectorized_length = 8;
//
//    size_t len = 1000000*2;
//    int * table = (int *) malloc(sizeof(int) * len);
//    for (int i = 0; i < len; ++i) {
//        table[i] = i;
//    }

    Tuple** final_result;

//    // SELECT sum(a) from table;
//    SequentialScanMemoryExecutor sse(table, len);
    SequentialScanExecutor sse("sample_table");
    AggregationOperationExecutor aoe("+", 0, &sse);
    while (true){
        final_result = aoe.next();
        if (final_result == NULL) break;
        final_result[0]->print();
    }


}
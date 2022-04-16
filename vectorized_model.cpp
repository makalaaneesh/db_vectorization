#include<iostream>
#include <immintrin.h>
#include <time.h>

using namespace std;
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

class SequentialScanMemoryExecutor: public Executor  {
        public:
        string filename;
        int* table;
        size_t i;
        size_t len;
        Tuple ** results_vector;

        SequentialScanMemoryExecutor(int *_table, size_t _len){
            table = _table;
            len = _len;
            i = 0;
            results_vector = (struct Tuple **) malloc(sizeof(struct Tuple *) * vectorized_length);
            for (int v = 0; v < vectorized_length; ++v) {
                results_vector[v] = (struct Tuple*) malloc(sizeof(struct Tuple));
                results_vector[v]->len = 1;
                results_vector[v]->integers = (int *) malloc(sizeof(int) * 1);
            }
        }

        struct Tuple** next(){
            if (i >=len){
                return NULL;
            }
            for (int v = 0; v < vectorized_length; ++v) {
                results_vector[v]->integers[0] = table[i++];
                if (i>=len) break;
            }

            return results_vector;
        }
};


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

                aggVectorTemp = _mm256_set_epi32(input_tuple_vector[0]->integers[columnindex], input_tuple_vector[1]->integers[columnindex], input_tuple_vector[2]->integers[columnindex], input_tuple_vector[3]->integers[columnindex],
                                                 input_tuple_vector[4]->integers[columnindex], input_tuple_vector[5]->integers[columnindex], input_tuple_vector[6]->integers[columnindex], input_tuple_vector[7]->integers[columnindex]);
                aggVector = _mm256_add_epi32(aggVector, aggVectorTemp);
                len += 8;
            }
            __attribute__ ((aligned (32))) int output[vectorized_length];
            _mm256_store_si256((__m256i*)output, aggVector);
            for (int v = 0; v < vectorized_length; ++v) {
                aggValue += output[v];
            }
            aggValue = aggValue/len;

            result_tuple->integers[0] = aggValue;
            computed = 1;
            Tuple ** results_vector = (struct Tuple **) malloc(sizeof(struct Tuple *) * vectorized_length);
            results_vector[0] = result_tuple;
            return results_vector;
        }
};


int main(){
    srand(42);
    vectorized_length = 8;

    size_t len = 100000000;
    int * table = (int *) malloc(sizeof(int) * len);
    for (int i = 0; i < len; ++i) {
        table[i] = rand()%(100) + 1;
    }
    Tuple** final_result;

    SequentialScanMemoryExecutor sse(table, len);
    AggregationOperationExecutor aoe("+", 0, &sse);

    struct timespec before;
    struct timespec after;
    clock_gettime(CLOCK_MONOTONIC, &before);
    while (true){
        final_result = aoe.next();
        if (final_result == NULL) break;
        final_result[0]->print();
    }
    clock_gettime(CLOCK_MONOTONIC, &after);
    double time = (double)(after.tv_sec - before.tv_sec) +
                  (double)(after.tv_nsec - before.tv_nsec) / 1e9;
    printf("took %f seconds\n", time);


}
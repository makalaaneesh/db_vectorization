#include<iostream>
#include <time.h>

using namespace std;

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
    virtual struct Tuple* next() = 0;
};


class SequentialScanMemoryExecutor: public Executor  {
public:
    string filename;
    int* table;
    size_t i;
    size_t len;
    Tuple *sample_tuple;

    SequentialScanMemoryExecutor(int *_table, size_t _len){
        table = _table;
        len = _len;
        i = 0;
        sample_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
        sample_tuple->len = 1;
        sample_tuple->integers = (int *) malloc(sizeof(int) * 1);
    }

    struct Tuple* next(){
        if (i >=len){
            return NULL;
        }
        sample_tuple->integers[0] = table[i++];

        return sample_tuple;
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

    struct Tuple* next(){
        if (computed){
            return NULL;
        }
        Tuple* result_tuple = (struct Tuple*) malloc(sizeof(struct Tuple));
        result_tuple->integers = (int *) malloc(sizeof(int) * 1);
        result_tuple->len = 1;

        while (true) {
            Tuple* input_tuple = childExecutor->next();
            if (input_tuple == NULL){
                break;
            }
            aggValue += input_tuple->integers[columnindex];
            len++;
        }
        aggValue = aggValue/len;

        result_tuple->integers[0] = aggValue;
        computed = 1;
        return result_tuple;
    }
};


int main(){
    srand(42);
    size_t len = 100000000;
    int * table = (int *) malloc(sizeof(int) * len);
    for (int i = 0; i < len; ++i) {
        table[i] = rand()%(100) + 1;
    }

    Tuple* final_result;


    SequentialScanMemoryExecutor sse(table, len);
    AggregationOperationExecutor aoe("+", 0, &sse);

    struct timespec before;
    struct timespec after;
    clock_gettime(CLOCK_MONOTONIC, &before);
    while (true){
        final_result = aoe.next();
        if (final_result == NULL) break;
        final_result->print();
    }
    clock_gettime(CLOCK_MONOTONIC, &after);
    double time = (double)(after.tv_sec - before.tv_sec) +
                  (double)(after.tv_nsec - before.tv_nsec) / 1e9;
    printf("took %f seconds\n", time);


}
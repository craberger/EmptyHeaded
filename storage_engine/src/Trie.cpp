/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The top level datastructure. This class holds the methods to create the 
* trie from a table. The TrieBlock class holds the more interesting methods.
******************************************************************************/
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"
#include "Trie.hpp"
#include "trie/TrieBlock.hpp"
#include "utils/ParMMapBuffer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Annotation.hpp"

typedef hybrid layout;

template<class A,class M>
void Trie<A,M>::save(){
  memoryBuffers->save();
}

template<class A,class M>
Trie<A,M>* Trie<A,M>::load(std::string path){
  Trie<A,M>* ret = new Trie<A,M>();

  //fixme load from binary
  ret->annotated = false;
  ret->num_columns = 2;
  ret->num_rows = 2;

  ret->memoryBuffers = new M(path,1000*2*sizeof(size_t));
  ret->memoryBuffers->load();

  return ret;
}


template<class A,class M>
void recursive_foreach(
  const bool annotated,
  M* memoryBuffers,
  TrieBlock<layout,M> *current, 
  const size_t level, 
  const size_t num_levels,
  std::vector<uint32_t>* tuple,
  const std::function<void(std::vector<uint32_t>*,A)> body){

  if(level+1 == num_levels){
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      tuple->push_back(a_d);
      if(annotated)
        assert(false);
        //body(tuple,current->get_data(a_i,a_d));
      else 
        body(tuple,(A)0);
      tuple->pop_back();
    });
  } else {
    current->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
      //if not done recursing and we have data
      tuple->push_back(a_d);
      if(current->get_block(a_i,a_d,memoryBuffers) != NULL){
        recursive_foreach<A,M>(
          annotated,
          memoryBuffers,
          current->get_block(a_i,a_d,memoryBuffers),
          level+1,
          num_levels,
          tuple,
          body);
      }
      tuple->pop_back(); //delete the last element
    });
  }
}

/*
* Write the trie to a binary file 
*/
template<class A,class M>
void Trie<A,M>::foreach(const std::function<void(std::vector<uint32_t>*,A)> body){
  std::vector<uint32_t>* tuple = new std::vector<uint32_t>();
  TrieBlock<layout,M>* head = (TrieBlock<layout,M>*)memoryBuffers->get_address(0,0);
  head->set.data = (uint8_t*)((uint8_t*)head + sizeof(TrieBlock<layout,M>));
  head->next = (NextLevel*)((uint8_t*)head + sizeof(TrieBlock<layout,M>) + head->set.number_of_bytes);

  head->set.foreach_index([&](uint32_t a_i, uint32_t a_d){
    tuple->push_back(a_d);
    if(num_columns > 1 && head->get_block(a_i,a_d,memoryBuffers) != NULL){
      recursive_foreach<A,M>(
        annotated,
        memoryBuffers,
        head->get_block(a_i,a_d,memoryBuffers),
        1,
        num_columns,
        tuple,
        body);
    } else if(annotated) {
      std::cout << "annotated not implemented yet" << std::endl;
      assert(false);
      //body(tuple,head->get_data(a_i,a_d));
    } else{
      body(tuple,(A)0); 
    }
    tuple->pop_back(); //delete the last element
  });
}


/*
/////////////////////////////////////////////////////
Constructor code from here down
/////////////////////////////////////////////////////
*/


/*
* Recursive sort function to get the relation in order for the trie.
*/
struct SortColumns{
  std::vector<std::vector<uint32_t>> *columns; 
  SortColumns(std::vector<std::vector<uint32_t>> *columns_in){
    columns = columns_in;
  }
  bool operator()(uint32_t i, uint32_t j) const {
    for(size_t c = 0; c < columns->size(); c++){
      if(columns->at(c).at(i) != columns->at(c).at(j)){
        return columns->at(c).at(i) < columns->at(c).at(j);
      }
    }
    return false;
  }
};

/*
* Given a range of values figure out the distinct values to go in the set.
* Helper method for the constructor
*/
std::tuple<size_t,size_t> produce_ranges(
  size_t start, 
  size_t end, 
  size_t *next_ranges, 
  uint32_t *data,
  uint32_t *indicies, 
  std::vector<uint32_t> * current){

  size_t range = 0;

  size_t num_distinct = 0;
  size_t i = start;
  while(true){
    const size_t start_range = i;
    const uint32_t cur = current->at(indicies[i]);
    uint32_t prev = cur;

    next_ranges[num_distinct] = start_range;
    data[num_distinct] = cur;

    ++num_distinct;
    range = cur;

    while(cur == prev){
      if((i+1) >= end)
        goto FINISH;
      prev = current->at(indicies[++i]);
    }
  }
  FINISH:
  next_ranges[num_distinct] = end;
  return std::tuple<size_t,size_t>(num_distinct,range+1);
}

/*
* Produce a TrieBlock
*/
template<class B, class A>
std::pair<size_t,B*> build_block(
  const size_t tid,  
  A *data_allocator, 
  const size_t set_size, 
  uint32_t *set_data_buffer){

  B *block = (B*)data_allocator->get_next(tid,sizeof(B));
  const size_t offset = (size_t)block-(size_t)data_allocator->get_address(tid);

  const size_t set_range = (set_size > 1) ? (set_data_buffer[set_size-1]-set_data_buffer[0]) : 0;
  const size_t set_alloc_size =  layout::get_number_of_bytes(set_size,set_range);
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size);
  block->set = Set<layout>::from_array(set_data_in,set_data_buffer,set_size);

  assert(set_alloc_size >= block->set.number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-block->set.number_of_bytes);

  //return tuple<offset,block>
  return std::make_pair(offset,block);
}

void encode_tail(size_t start, size_t end, uint32_t *data, std::vector<uint32_t> *current, uint32_t *indicies){
  for(size_t i = start; i < end; i++){
    *data++ = current->at(indicies[i]);
  }
}
/*
* Recursively build the trie. Terminates when we hit the number of levels.
*/
template<class B, class M, class A>
void recursive_build(
  const size_t index, 
  const size_t start, 
  const size_t end, 
  const uint32_t data, 
  B* prev_block, 
  const size_t level, 
  const size_t num_levels, 
  const size_t tid, 
  std::vector<std::vector<uint32_t>> *attr_in,
  M *data_allocator, 
  std::vector<size_t*> *ranges_buffer, 
  std::vector<uint32_t*> *set_data_buffer, 
  uint32_t *indicies,
  std::vector<A>* annotation){

  uint32_t *sb = set_data_buffer->at(level*NUM_THREADS+tid);
  encode_tail(start,end,sb,&attr_in->at(level),indicies);

  auto retTup = build_block<B,M>(tid,data_allocator,(end-start),sb);
  const size_t offset = std::get<0>(retTup);
  B *tail = std::get<1>(retTup);
  prev_block->set_block(index,data,tid,offset);

  if(level < (num_levels-1)){
    tail->init_pointers(tid,data_allocator);
    auto tup = produce_ranges(start,end,ranges_buffer->at(level*NUM_THREADS+tid),set_data_buffer->at(level*NUM_THREADS+tid),indicies,&attr_in->at(level));
    const size_t set_size = std::get<0>(tup);
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(level*NUM_THREADS+tid)[i];
      const size_t next_end = ranges_buffer->at(level*NUM_THREADS+tid)[i+1];
      const uint32_t next_data = set_data_buffer->at(level*NUM_THREADS+tid)[i];        
      recursive_build<B,M,A>(
        i,
        next_start,
        next_end,
        next_data,
        tail,
        level+1,
        num_levels,
        tid,
        attr_in,
        data_allocator,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation);
    }
  } else if(annotation->size() != 0){
    /*
    tail->alloc_data(tid,data_allocator);
    for(size_t i = start; i < end; i++){
      uint32_t data_value = attr_in->at(level).at(indicies[i]);
      R annotationValue = annotation->at(indicies[i]);
      tail->set_data(i-start,data_value,annotationValue);
    }
    */
  }
}

//builds the trie from an encoded relation
template<class A, class M>
Trie<A,M>::Trie(
  std::string path,
  std::vector<uint32_t>* max_set_sizes, 
  std::vector<std::vector<uint32_t>> *attr_in,
  std::vector<A>* annotation){

  annotated = annotation->size() > 0;
  num_rows = attr_in->at(0).size();
  num_columns = attr_in->size();
  //fixme: add estimate
  memoryBuffers = new M(path,1*1073741824);
  std::cout << path << std::endl;
  
  assert(num_columns != 0  && num_rows != 0);

  //Setup indices buffer
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = indicies;
  for(size_t i = 0; i < num_rows; i++){
    *iterator++ = i; 
  }
  
  //sort the relation
  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  std::vector<size_t*> *ranges_buffer = new std::vector<size_t*>();
  std::vector<uint32_t*> *set_data_buffer = new std::vector<uint32_t*>();
  
  //set up temporary buffers needed for the build
  //fixme: add estimate
  ParMemoryBuffer *tmp_data = new ParMemoryBuffer(1000*2*sizeof(size_t));
  for(size_t i = 0; i < num_columns; i++){
    for(size_t t = 0; t < NUM_THREADS; t++){
      size_t* ranges = (size_t*)tmp_data->get_next(t,sizeof(size_t)*(max_set_sizes->at(i)+1));
      uint32_t* sd = (uint32_t*)tmp_data->get_next(t,sizeof(uint32_t)*(max_set_sizes->at(i)+1));
      ranges_buffer->push_back(ranges);
      set_data_buffer->push_back(sd); 
    }
  }

  //Find the ranges for distinct values in the head
  auto tup = produce_ranges(0,
    num_rows,
    ranges_buffer->at(0),
    set_data_buffer->at(0),
    indicies,
    &attr_in->at(0));
  const size_t head_size = std::get<0>(tup);
  const size_t head_range = std::get<1>(tup);
  
  //Build the head set.
  auto retTup = 
    build_block<TrieBlock<layout,M>,M>(
      0,
      memoryBuffers,
      head_size,
      set_data_buffer->at(0));
  TrieBlock<layout,M>* new_head = std::get<1>(retTup);

  size_t cur_level = 1;
  if(num_columns > 1){
    new_head->init_pointers(0,memoryBuffers);
    
    par::for_range(0,head_range,100,[&](size_t tid, size_t i){
      (void) tid;
      new_head->next[i].index = -1;
    });

    par::for_range(0,head_size,100,[&](size_t tid, size_t i){
      //some sort of recursion here
      const size_t start = ranges_buffer->at(0)[i];
      const size_t end = ranges_buffer->at(0)[i+1];
      const uint32_t data = set_data_buffer->at(0)[i];

      recursive_build<TrieBlock<layout,M>,M,A>(
        i,
        start,
        end,
        data,
        new_head,
        cur_level,
        num_columns,
        tid,
        attr_in,
        memoryBuffers,
        ranges_buffer,
        set_data_buffer,
        indicies,
        annotation);
    });
  } else if(annotation->size() > 0){
    /*
    new_head->alloc_data(0,data_allocator);
    for(size_t i = 0; i < head_size; i++){
      const uint32_t data = set_data_buffer->at(0)[i]; 
      R annotationValue = annotation->at(indicies[i]);
      new_head->set_data(i,data,annotationValue);
    }
    */
  }
  
  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges
}


template struct Trie<void*,ParMemoryBuffer>;
template struct Trie<long,ParMemoryBuffer>;
template struct Trie<int,ParMemoryBuffer>;
template struct Trie<float,ParMemoryBuffer>;
template struct Trie<double,ParMemoryBuffer>;

template struct Trie<void*,ParMMapBuffer>;
template struct Trie<long,ParMMapBuffer>;
template struct Trie<int,ParMMapBuffer>;
template struct Trie<float,ParMMapBuffer>;
template struct Trie<double,ParMMapBuffer>;
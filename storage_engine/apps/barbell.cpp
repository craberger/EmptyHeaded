
#include "barbell.hpp"
#include "utils/thread_pool.hpp"
#include "utils/parallel.hpp"
#include "Trie.hpp"
#include "TrieBuilder.hpp"
#include "TrieIterator.hpp"
#include "utils/timer.hpp"
#include "utils/ParMemoryBuffer.hpp"
#include "Encoding.hpp"

void Query_0::run_0() {
  thread_pool::initializeThreadPool();
  Trie<void *, ParMemoryBuffer> *Trie_Edge_0_1 = NULL;
  {
    auto start_time = timer::start_clock();
    Trie_Edge_0_1 = Trie<void *, ParMemoryBuffer>::load(
        "/Users/egan/Documents/Projects/EmptyHeaded/examples/graph/data/"
        "facebook/db/relations/Edge/Edge_0_1");
    timer::stop_clock("LOADING Trie Edge_0_1", start_time);
  }

  auto e_loading_node = timer::start_clock();
  Encoding<long> *Encoding_node = Encoding<long>::from_binary(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/graph/data/facebook/"
      "db/encodings/node/");
  (void)Encoding_node;
  timer::stop_clock("LOADING ENCODINGS node", e_loading_node);

  auto query_timer = timer::start_clock();
  Trie<long, ParMemoryBuffer> *Trie_Barbell_ = new Trie<long, ParMemoryBuffer>(
      "/Users/egan/Documents/Projects/EmptyHeaded/examples/graph/data/facebook/"
      "db/relations/Barbell/Barbell_",
      0, true);
  par::reducer<size_t> num_rows_reducer(
      0, [](size_t a, size_t b) { return a + b; });
  Trie<long, ParMemoryBuffer> *Trie_bag_1_a_b_c_0 =
      new Trie<long, ParMemoryBuffer>("/Users/egan/Documents/Projects/"
                                      "EmptyHeaded/examples/graph/data/"
                                      "facebook/db/relations/bag_1_a_b_c",
                                      1, true);
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_1_a_b_c_0, 3);
    Builders.trie->encodings.push_back((void *)Encoding_node);
    const ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_c(
        Trie_Edge_0_1);
    const ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_b_c(
        Trie_Edge_0_1);
    const ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_b(
        Trie_Edge_0_1);
    Builders.build_set(Iterators_Edge_a_c.head, Iterators_Edge_a_b.head);
    Builders.allocate_annotation();
    Builders.par_foreach_builder([&](const size_t tid, const uint32_t a_i,
                                     const uint32_t a_d) {
      TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_c =
          Iterators_Edge_a_c.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_b_c =
          Iterators_Edge_b_c.iterators.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_b =
          Iterators_Edge_a_b.iterators.at(tid);
      Iterator_Edge_a_c->get_next_block(0, a_d);
      Iterator_Edge_a_b->get_next_block(0, a_d);
      const size_t count_b = Builder->build_aggregated_set(
          Iterator_Edge_b_c->get_block(0), Iterator_Edge_a_b->get_block(1));
      long annotation_b = (long)0;
      Builder->foreach_aggregate([&](const uint32_t b_d) {
        Iterator_Edge_b_c->get_next_block(0, b_d);
        const long intermediate_b = (long)1 * 1;
        const size_t count_c = Builder->build_aggregated_set(
            Iterator_Edge_a_c->get_block(1), Iterator_Edge_b_c->get_block(1));
        num_rows_reducer.update(tid, count_c);
        long annotation_c = (long)0;
        const long intermediate_c = (long)1 * 1 * 1 * intermediate_b * count_c;
        annotation_c += intermediate_c;
        annotation_b += annotation_c;
      });
      Builder->set_annotation(annotation_b, a_i, a_d);
    });
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    timer::stop_clock("BAG bag_1_a_b_c TIME", bag_timer);
  }
  Trie<long, ParMemoryBuffer> *Trie_bag_1_x_y_z_0 = Trie_bag_1_a_b_c_0;
  Trie<long, ParMemoryBuffer> *Trie_bag_0_a_x_ = Trie_Barbell_;
  {
    auto bag_timer = timer::start_clock();
    num_rows_reducer.clear();
    ParTrieBuilder<long, ParMemoryBuffer> Builders(Trie_bag_0_a_x_, 2);
    const ParTrieIterator<void *, ParMemoryBuffer> Iterators_Edge_a_x(
        Trie_Edge_0_1);
    const ParTrieIterator<long, ParMemoryBuffer> Iterators_bag_1_a_b_c_a(
        Trie_bag_1_a_b_c_0);
    const ParTrieIterator<long, ParMemoryBuffer> Iterators_bag_1_x_y_z_x(
        Trie_bag_1_x_y_z_0);
    Builders.build_aggregated_set(Iterators_Edge_a_x.head,
                                  Iterators_bag_1_a_b_c_a.head);
    par::reducer<long> annotation_a(0, [&](long a, long b) { return a + b; });
    Builders.par_foreach_aggregate([&](const size_t tid, const uint32_t a_d) {
      TrieBuilder<long, ParMemoryBuffer> *Builder = Builders.builders.at(tid);
      TrieIterator<void *, ParMemoryBuffer> *Iterator_Edge_a_x =
          Iterators_Edge_a_x.iterators.at(tid);
      TrieIterator<long, ParMemoryBuffer> *Iterator_bag_1_a_b_c_a =
          Iterators_bag_1_a_b_c_a.iterators.at(tid);
      TrieIterator<long, ParMemoryBuffer> *Iterator_bag_1_x_y_z_x =
          Iterators_bag_1_x_y_z_x.iterators.at(tid);
      Iterator_Edge_a_x->get_next_block(0, a_d);
      const long intermediate_a =
          (long)1 * Iterator_bag_1_a_b_c_a->get_annotation(0, a_d);
      const size_t count_x =
          Builder->build_aggregated_set(Iterator_Edge_a_x->get_block(1),
                                        Iterator_bag_1_x_y_z_x->get_block(0));
      num_rows_reducer.update(tid, count_x);
      long annotation_x = (long)0;
      Builder->foreach_aggregate([&](const uint32_t x_d) {
        const long intermediate_x =
            (long)1 * 1 * Iterator_bag_1_x_y_z_x->get_annotation(0, x_d) *
            intermediate_a;
        annotation_x += intermediate_x;
      });
      annotation_a.update(tid, annotation_x);
    });
    Builders.trie->annotation = annotation_a.evaluate(0);
    Builders.trie->num_rows = num_rows_reducer.evaluate(0);
    std::cout << "NUM ROWS: " << Builders.trie->num_rows
              << " ANNOTATION: " << Builders.trie->annotation << std::endl;
    timer::stop_clock("BAG bag_0_a_x TIME", bag_timer);
  }
  result_0 = (void *)Trie_Barbell_;
  std::cout << "NUMBER OF ROWS: " << Trie_Barbell_->num_rows << std::endl;
  timer::stop_clock("QUERY TIME", query_timer);

  thread_pool::deleteThreadPool();
}

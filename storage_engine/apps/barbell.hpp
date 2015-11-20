#include <vector>
#include <stdint.h>
#include <tuple>
#include "Trie.hpp"
#include "Encoding.hpp"

struct application{
  public:
    virtual void run_0() = 0;
};

struct ParMMapBuffer;
struct ParMemoryBuffer;

//template types are the types of the attributes, followed by the type of the annotation
struct Query_0 : public application {
  void* result_0;

	Query_0(){}
	void run_0();
};

application* init_app(){
  return new Query_0();
}
int main () {
  application* q = init_app();
  q->run_0();
}

#ifdef GOOGLE_TEST
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif

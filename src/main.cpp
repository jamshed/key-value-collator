
#include "Key_Value_Collator.hpp"

#include <cstdint>
#include <cstddef>


int main(int argc, char* argv[])
{
    (void)argc;
    (void)argv;

    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator;
    kv_collator.close_deposit_stream();

    return 0;
}

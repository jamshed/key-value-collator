
#include "Key_Value_Collator.hpp"

#include <cstdint>
#include <cstddef>


int main(int argc, char* argv[])
{
    (void)argc;

    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    typedef key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator_t;
    kv_collator_t kv_collator(argv[1]);

    kv_collator_t::buf_t& buf = kv_collator.get_buffer();
    for(uint32_t i = 0; i < 20000000u; ++i)
        buf.emplace_back(i, i * 2lu);

    kv_collator.return_buffer(buf);

    kv_collator.close_deposit_stream();

    return 0;
}

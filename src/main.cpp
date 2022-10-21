
#include "Key_Value_Collator.hpp"

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <thread>
#include <random>
#include <chrono>


int main(int argc, char* argv[])
{
    (void)argc;
    const std::string work_pref(argv[1]);
    const uint32_t thread_count = std::atoi(argv[2]);

    // typedef std::chrono::high_resolution_clock::time_point time_point_t;
    constexpr auto now = std::chrono::high_resolution_clock::now;
    const auto duration = [](const std::chrono::nanoseconds& d) { return std::chrono::duration_cast<std::chrono::duration<double>>(d).count(); };


    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    typedef key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator_t;
    kv_collator_t kv_collator(argv[1]);

    const auto t_0 = now();
    std::vector<std::thread> worker;
    worker.reserve(thread_count);
    for(uint32_t i = 0; i < thread_count; ++i)
        worker.emplace_back(
            [&kv_collator]()
            {
                constexpr uint32_t min = 0;
                constexpr uint32_t max = std::numeric_limits<uint32_t>::max();

                std::random_device rd;
                std::mt19937 rng(rd());
                std::uniform_int_distribution<uint32_t> uni(min, max);

                constexpr std::size_t buf_sz = 10 * 1024 * 1024; // 10MB.
                constexpr std::size_t buf_elem = buf_sz / sizeof(std::pair<key_t, val_t>);
                for(uint32_t i = 0; i < 10; ++i)    // Each producer deposits 10 buffers.
                {
                    kv_collator_t::buf_t& buf = kv_collator.get_buffer();
                    for(std::size_t j = 0; j < buf_elem; ++j)
                        buf.emplace_back(uni(rd), uni(rd));

                    kv_collator.return_buffer(buf);
                }
            }
        );

    for(uint32_t i = 0; i < thread_count; ++i)
        worker[i].join();

    kv_collator.close_deposit_stream();

    const auto t_1 = now();
    std::cout << "Deposited all key-val pairs in " << duration(t_1 - t_0) << " seconds.\n";

    kv_collator.collate(thread_count);
    const auto t_2 = now();
    std::cout << "Collation done in " << duration(t_2 - t_1) << " seconds.\n";

    return 0;
}

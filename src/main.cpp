
#include "Key_Value_Collator.hpp"

#include <cstdint>
#include <cstddef>
#include <vector>
#include <set>
#include <unordered_set>
#include <cstdlib>
#include <thread>
#include <random>
#include <chrono>


bool is_correct(const std::string& work_pref, const uint32_t thread_count)
{
    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    typedef key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator_t;
    kv_collator_t kv_collator(work_pref, thread_count * 2);

    std::vector<std::thread> worker;
    worker.reserve(thread_count);
    std::vector<std::vector<key_t>> v(thread_count);
    for(uint32_t i = 0; i < thread_count; ++i)
        worker.emplace_back(
            [&kv_collator](std::vector<key_t>& keys)
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

                    std::for_each(buf.cbegin(), buf.cend(), [&keys](const auto& p){keys.emplace_back(p.first);});
                    kv_collator.return_buffer(buf);
                }
            },
            std::ref(v[i])
        );

    for(uint32_t i = 0; i < thread_count; ++i)
        worker[i].join();

    kv_collator.close_deposit_stream();

    std::set<key_t> s;
    std::for_each(v.cbegin(), v.cend(), [&s](const auto& vec)
        { std::for_each(vec.cbegin(), vec.cend(), [&s](const auto p){ s.insert(p); }); });
    std::cout << "Unique keys deposited: " << s.size() << "\n";

    kv_collator.collate(thread_count);


    kv_collator_t::iter_t it = kv_collator.begin();
    const kv_collator_t::iter_t end = kv_collator.end();
    std::vector<key_t> vec_it;
    while(it != end)
    {
        vec_it.emplace_back(*it);
        ++it;
    }

    std::cout << "Iterated over unique-key count: " << vec_it.size() << "\n";

    std::sort(vec_it.begin(), vec_it.end());
    return vec_it == std::vector<key_t>(s.cbegin(), s.cend());
}


void perf_check(const std::string& work_pref, const uint32_t thread_count)
{
    // typedef std::chrono::high_resolution_clock::time_point time_point_t;
    constexpr auto now = std::chrono::high_resolution_clock::now;
    const auto duration = [](const std::chrono::nanoseconds& d) { return std::chrono::duration_cast<std::chrono::duration<double>>(d).count(); };


    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    typedef key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator_t;
    kv_collator_t kv_collator(work_pref, thread_count * 2);

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

    kv_collator.collate(thread_count, true);
    const auto t_2 = now();
    std::cout << "Collation done in " << duration(t_2 - t_1) << " seconds.\n";

    std::cout << "Total key-value pair count:   " << kv_collator.pair_count() << "\n";
    std::cout << "Unique count:                 " << kv_collator.unique_key_count() << "\n";
    std::cout << "Frequency of a mode key:      " << kv_collator.mode_frequency() << "\n";
}


bool is_correct_batched_read(const std::string& work_pref, const uint32_t thread_count)
{
    typedef uint32_t key_t;
    typedef std::size_t val_t;
    typedef key_value_collator::Identity_Functor<key_t> hasher_t;
    typedef key_value_collator::Key_Value_Collator<key_t, val_t, hasher_t> kv_collator_t;
    kv_collator_t kv_collator(work_pref, thread_count * 2);

    std::vector<std::thread> worker;
    worker.reserve(thread_count);
    std::vector<std::vector<key_t>> v(thread_count);
    for(uint32_t i = 0; i < thread_count; ++i)
        worker.emplace_back(
            [&kv_collator](std::vector<key_t>& keys)
            {
                constexpr uint32_t min = 0;
                constexpr uint32_t max = std::numeric_limits<uint32_t>::max();

                std::random_device rd;
                std::mt19937 rng(rd());
                std::uniform_int_distribution<uint32_t> uni(min, max);

                constexpr std::size_t buf_sz = 10 * 1024 * 1024; // 10MB.
                constexpr std::size_t buf_elem = buf_sz / sizeof(std::pair<key_t, val_t>);
                for(uint32_t i = 0; i < 1; ++i)    // Each producer deposits 10 buffers.
                {
                    kv_collator_t::buf_t& buf = kv_collator.get_buffer();
                    for(std::size_t j = 0; j < buf_elem; ++j)
                        buf.emplace_back(uni(rd), uni(rd));

                    std::for_each(buf.cbegin(), buf.cend(), [&keys](const auto& p){keys.emplace_back(p.first);});
                    kv_collator.return_buffer(buf);
                }
            },
            std::ref(v[i])
        );

    for(uint32_t i = 0; i < thread_count; ++i)
        worker[i].join();

    kv_collator.close_deposit_stream();

    std::set<key_t> s;
    std::for_each(v.cbegin(), v.cend(), [&s](const auto& vec)
        { std::for_each(vec.cbegin(), vec.cend(), [&s](const auto p){ s.insert(p); }); });
    std::cout << "Unique keys deposited: " << s.size() << "\n";

    kv_collator.collate(thread_count);

    std::cout << "Done collating\n";


    std::unordered_set<key_t> keys;
    kv_collator_t::iter_t it = kv_collator.begin();
    constexpr std::size_t buf_mem = 10lu * 1024lu * 1024lu; // 10MB.
    constexpr std::size_t buf_elem = buf_mem / sizeof(kv_collator_t::key_val_pair_t);
    kv_collator_t::key_val_pair_t* const buf = new kv_collator_t::key_val_pair_t[buf_elem];
    while(true)
    {
        const std::size_t read_elem = it.read(buf, buf_elem);
        if(!read_elem)
            break;

        std::for_each(buf, buf + read_elem, [&keys](const auto& p){ keys.insert(p.first); });
    }

    delete[] buf;

    std::vector<key_t> vec_it(keys.cbegin(), keys.cend());
    std::cout << "Iterated over unique-key count: " << vec_it.size() << "\n";

    std::sort(vec_it.begin(), vec_it.end());
    return vec_it == std::vector<key_t>(s.cbegin(), s.cend());

    return true;
}


int main(int argc, char* argv[])
{
    (void)argc;
    const std::string work_pref(argv[1]);
    const uint32_t thread_count = std::atoi(argv[2]);

    perf_check(work_pref, thread_count);

    // std::cout << "Collated collection is " << (is_correct(work_pref, thread_count) ? "correct" : "incorrect") << "\n";

    // std::cout << "Collated collection is " << (is_correct_batched_read(work_pref, thread_count) ? "correct" : "incorrect") << "\n";

    return 0;
}

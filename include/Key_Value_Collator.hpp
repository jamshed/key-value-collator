
#ifndef KEY_VALUE_COLLATOR_HPP
#define KEY_VALUE_COLLATOR_HPP



#include "Spin_Lock.hpp"

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <atomic>
#include <utility>
#include <fstream>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <thread>
#include <cassert>


// =============================================================================

namespace key_value_collator
{


template <typename T_obj_> class Object_Pool;

// A class to: collate a collection of key-value pairs, deposited from multiple
// producers; and to iterate over the collated key-value collection. Keys are of
// type `T_key_`, values are of type `T_val_`, and the keys are hashed to their
// corresponding partitions with `operator()(T_key_ key)` of class `T_hasher_`.
template <typename T_key_, typename T_val_, typename T_hasher_>
class Key_Value_Collator
{
private:

    typedef std::pair<T_key_, T_val_> key_val_pair_t;

    const T_hasher_ hash;   // Hasher object to hash the keys to a numerical address-space.

    const std::string work_file_pref;   // Path to the temporary working files used by the collator.

    static constexpr char work_file_pref_default[] = ".";   // Default value for the temporary working files' prefixes.
    static constexpr char partition_file_ext[] = ".part";   // File extensions of the temporary partition files.

    static constexpr std::size_t partition_count = (1 << 7);    // Number of partitions for the keys.
    static constexpr std::size_t partition_buf_mem = (1LU * 1024 * 1024);   // Maximum memory for a partition buffer: 1MB.
    static constexpr std::size_t partition_buf_elem_th = partition_buf_mem / sizeof(key_val_pair_t);    // Maximum number of pairs to keep in a partition buffer.

    typedef std::vector<key_val_pair_t> buf_t;  // Type of the data buffers.
    std::vector<buf_t> partition_buf;   // `partition_buf[i]` is the in-memory buffer for partition `i`.
    std::vector<std::ofstream> partition_file;  // `partition_file[i]` is the disk-storage file for partition `i`.

    Object_Pool<buf_t*> free_buf_pool;  // Buffers to copy-in incoming data from the producers.
    Object_Pool<buf_t*> full_buf_pool;  // Deposited data from the producers.

    const std::size_t buf_count;    // Number of concurrent buffers for the producers.
    static constexpr std::size_t buf_count_default = 16;    // Default value for the concurrent buffer count.

    std::thread* mapper;    // The background thread mapping key-value pairs to corresponding partitions.
    std::atomic<bool> stream_incoming;  // Flag denoting whether the incoming key-value streams have ended or not.


    // Returns the disk-file path for the partition `p_id`.
    const std::string partition_file_path(std::size_t p_id) const;

    // Maps the key-value pairs from the producers to the partitions
    // corresponding to the keys.
    void map();

    // Maps the key-value pairs from the data buffer `buf` to the partitions
    // corresponding to the keys.
    void map_buffer(const std::vector<key_val_pair_t>& buf);

    // Returns the corresponding partition ID for the key `key`.
    std::size_t get_partition_id(const T_key_& key) const;

    // Flushes the buffer of the partition with ID `p_id` to disk and
    // clears the buffer.
    void flush(std::size_t p_id);


public:


    // Constructs a key-value pair collection object to collate (and iterate
    // over) key-value pairs produced from multiple producers. Temporary disk-
    // files used throughout the process will be stored at the path-prefix
    // `work_file_pref`. `buf_count` concurrent buffers would be used to store
    // and process the deposited data. It should be set to at least the number
    // of producers to avoid throttling of the producers; and a good heuristic
    // choice for this is twice the number of producers.
    Key_Value_Collator(const std::string& work_file_pref = work_file_pref_default, std::size_t buf_count = buf_count_default);

    ~Key_Value_Collator();

    // Deposits the buffer content of `buf` to the collator.
    void deposit(const std::vector<key_val_pair_t>& buf);

    // Closes the deposit stream incoming from the producers and flushes the
    // remaining in-memory content to disk. All deposit operations from the
    // producers must be made before invoking this.
    void close_deposit_stream();
};


// A collection of objects of type `T_obj_`, ready to be used by multiple
// threads.
template <typename T_obj_>
class Object_Pool
{
private:

    std::vector<T_obj_> pool;   // The object collection.
    Spin_Lock lock_;    // Mutual-exclusion lock to avoid concurrent updates of the pool.
    std::atomic<std::size_t> size_; // Number of elements in the pool.


public:

    // Constructs an empty object-pool.
    Object_Pool(): size_(0)
    {}


    // Adds the object `obj` to the pool.
    void push(const T_obj_& obj)  // Should use move operand?
    {
        lock_.lock();
        pool.emplace_back(obj);
        size_.fetch_add(1, std::memory_order_acquire);  // size_++;
        lock_.unlock();
    }


    // Returns `true` iff the pool is empty.
    bool empty() const { return size_ == 0; }


    // Returns the size of the pool.
    std::size_t size() const { return size_; }


    // Tries to fetch an object to `obj` from the pool. Returns `true` iff
    // such an object is found.
    bool fetch(T_obj_& obj)
    {
        if(empty())
            return false;

        bool success = false;

        lock_.lock();

        if(!pool.empty())
        {
            size_.fetch_sub(1, std::memory_order_acquire);  // size_--;
            obj = pool.back();   // std::move(pool.back());
            pool.pop_back();

            success = true;
        }

        lock_.unlock();

        return success;
    }
};


template <typename T_key_, typename T_val_, typename T_hasher_>
inline Key_Value_Collator<T_key_, T_val_, T_hasher_>::Key_Value_Collator(const std::string& work_file_pref, const std::size_t buf_count):
    hash(),
    work_file_pref(work_file_pref),
    partition_buf(partition_count),
    partition_file(partition_count),
    buf_count(buf_count),
    mapper(nullptr),
    stream_incoming(true)
{
    static_assert(partition_buf_elem_th > 0, "Invalid configuration for partition buffer memory.");

    for(std::size_t p_id = 0; p_id < partition_count; ++p_id)
    {
        partition_buf[p_id].reserve(partition_buf_elem_th);
        partition_file[p_id].open(partition_file_path(p_id), std::ios::out | std::ios::binary);
    }

    for(std::size_t i = 0; i < buf_count; ++i)
        free_buf_pool.push(new buf_t());

    mapper = new std::thread(&Key_Value_Collator<T_key_, T_val_, T_hasher_>::map, this);
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline Key_Value_Collator<T_key_, T_val_, T_hasher_>::~Key_Value_Collator()
{
    if(!full_buf_pool.empty() || free_buf_pool.size() != buf_count || mapper->joinable())
    {
        std::cerr << "Collator destructed while unprocessed buffers remained. Aborting.\n";
        std::exit(EXIT_FAILURE);
    }


    // Release memory of the partition-buffers.
    buf_t* buf_p = nullptr;
    while(!free_buf_pool.empty())
    {
        free_buf_pool.fetch(buf_p);
        delete buf_p;
    }

    delete mapper;


    // Remove the partition-files.
    for(std::size_t p_id = 0; p_id < partition_count; ++p_id)
        if(std::remove(partition_file_path(p_id).c_str()))
        {
            std::cerr << "Error removing temporary files. Aborting.\n";
            std::exit(EXIT_FAILURE);
        }
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline const std::string Key_Value_Collator<T_key_, T_val_, T_hasher_>::partition_file_path(const std::size_t p_id) const
{
    return work_file_pref + "." + std::to_string(p_id) + partition_file_ext;
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline void Key_Value_Collator<T_key_, T_val_, T_hasher_>::map()
{
    buf_t* buf_p;

    while(stream_incoming || !full_buf_pool.empty())
        if(full_buf_pool.fetch(buf_p))
        {
            map_buffer(*buf_p);

            buf_p->clear();
            free_buf_pool.push(buf_p);
        }
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline void Key_Value_Collator<T_key_, T_val_, T_hasher_>::map_buffer(const std::vector<Key_Value_Collator::key_val_pair_t>& buf)
{
    for(const auto& key_val_pair : buf)
    {
        const std::size_t p_id = get_partition_id(key_val_pair.first);
        auto& p_buf = partition_buf[p_id];
        p_buf.emplace_back(key_val_pair);

        assert(p_buf.size() <= partition_buf_elem_th);
        if(p_buf.size() == partition_buf_elem_th)
            flush(p_id);
    }
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline void Key_Value_Collator<T_key_, T_val_, T_hasher_>::flush(const std::size_t p_id)
{
    auto& buf  = partition_buf[p_id];
    auto& file = partition_file[p_id];
    if(!file.write(reinterpret_cast<const char*>(buf.data()), buf.size() * sizeof(key_val_pair_t)))
    {
        std::cerr << "Error writing to partition file(s) of the collator. Aborting.\n";
        std::exit(EXIT_FAILURE);
    }

    buf.clear();
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline std::size_t Key_Value_Collator<T_key_, T_val_, T_hasher_>::get_partition_id(const T_key_& key) const
{
    return hash(key) & (partition_count - 1);
}


template <typename T_key_, typename T_val_, typename T_hasher_>
inline void Key_Value_Collator<T_key_, T_val_, T_hasher_>::close_deposit_stream()
{
    stream_incoming = false;
    if(!mapper->joinable())
    {
        std::cerr << "Early termination encountered for the key-mapper thread. Aborting.\n";
        std::exit(EXIT_FAILURE);
    }

    mapper->join();


    // Flush the remaining in-memory partition contents, release their memory, and close the in-disk partitions.
    for(std::size_t p_id = 0; p_id < partition_count; ++p_id)
    {
        if(!partition_buf[p_id].empty())
            flush(p_id);

        buf_t().swap(partition_buf[p_id]);

        partition_file[p_id].close();
    }
}


template <typename T_key_, typename T_val_, typename T_hasher_>
const char Key_Value_Collator<T_key_, T_val_, T_hasher_>::partition_file_ext[];


template <typename T_key_>
class Identity_Functor
{
public:

    T_key_ operator()(const T_key_& key) const { return key; }
};

}



#endif

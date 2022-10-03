
#ifndef KEY_VALUE_COLLATOR_HPP
#define KEY_VALUE_COLLATOR_HPP



#include "concurrentqueue/concurrentqueue.h"

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <atomic>
#include <utility>
#include <fstream>
#include <thread>


// =============================================================================

namespace key_value_collator
{

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
    static constexpr std::size_t partition_buf_elem = partition_buf_mem / sizeof(key_val_pair_t);   // Maximum number of pairs to keep in a partition buffer.

    std::vector<std::vector<key_val_pair_t>> partition_buf; // `partition_buf[i]` is the in-memory buffer for partition `i`.
    std::vector<std::ofstream> partition_file;  // `partition_file[i]` is the disk-storage file for partition `i`.

    moodycamel::ConcurrentQueue<std::vector<key_val_pair_t>*> producer_buf_q;   // Buffer spaces to copy-in incoming data from the producers.

    std::thread* mapper;    // The background thread mapping key-value pairs to corresponding partitions.
    std::atomic<bool> stream_incoming;  // Flag denoting whether the incoming key-value streams have ended or not.


    // Returns the disk-file path for the partition `partition_id`.
    const std::string partition_file_path(std::size_t partition_id) const;

    // Maps the key-value pairs from the producers to the partitions
    // corresponding to the keys.
    void map();

    // Maps the key-value pairs from the data buffer `buf` to the partitions
    // corresponding to the keys.
    void map_buffer(const std::vector<key_val_pair_t>& buf);

    // Returns the corresponding partition ID for the key-value pair
    // `key_val_pair`.
    static std::size_t get_partition_id(const key_val_pair_t& key_val_pair);

    // Flushes the buffer of the partition with ID `partition_ID` to disk and
    // clears the buffer.
    void flush(std::size_t partition_id);

    // Closes the deposit stream incoming from the producers and flushes the
    // remaining in-memory content to disk.
    void close_deposit_stream();


public:


    // Constructs a key-value pair collection object to collate (and iterate
    // over) key-value pairs produced from multiple producers. Temporary disk-
    // files used throughout the process will be stored at the path-prefix
    // `work_file_pref`.
    Key_Value_Collator(const std::string& work_file_pref = work_file_pref_default);

    ~Key_Value_Collator();

    // Deposits the buffer content of `buf` to the collator.
    void deposit(const std::vector<key_val_pair_t>& buf);
};

}



#endif

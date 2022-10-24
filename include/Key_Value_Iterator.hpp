
#ifndef KEY_VALUE_ITERATOR_HPP
#define KEY_VALUE_ITERATOR_HPP



#include "Spin_Lock.hpp"

#include <cstddef>
#include <string>
#include <utility>
#include <cstdio>


// =============================================================================

namespace key_value_collator
{


// A class to iterate over key-value pairs of type `(T_key_, T_val_)`, collated
// by the class `Key_Value_Collator`.
template <typename T_key_, typename T_val_>
class Key_Value_Iterator
{
    template <T_key_, T_val_, typename T_hasher_> friend class Key_Value_Collator;

    typedef std::pair<T_key_, T_val_> key_val_pair_t;

private:

    const std::string work_pref;    // Path to the working files used by the collator.

    std::FILE* file_ptr;    // Pointer to the current (collated) file.

    std::size_t pos;    // Absolute index (sequential ID of the key-block) into the collated collection.
    key_val_pair_t* buf;    // Buffer to read in chunks of key-value pairs.
    std::size_t buf_elem_count; // Number of pairs currently in the buffer.
    std::size_t buf_idx;    // Index of the next pair to process from the buffer.
    static constexpr std::size_t buf_sz = 5lu * 1024lu * 1024lu / sizeof(key_val_pair_t);   // Size of the buffer in elements: total 5MB.

    key_val_pair_t elem;    // Current pair to process.

    Spin_Lock lock; // Mutually-exclusive access lock for the iterator users.


    // Constructs a null iterator.
    Key_Value_Iterator();

    // Constructs an iterator for the collated files at path `work_pref`.
    Key_Value_Iterator(const std::string& work_pref);


public:

    // Copy constructs an iterator from the iterator `other`.
    Key_Value_Iterator(const Key_Value_Iterator& other);

    // Destructs the iterator.
    ~Key_Value_Iterator();

    // Returns the key of the current pair.
    T_key_ operator*();

    // Advances the iterator by one key-block.
    Key_Value_Iterator& operator++();

    // Returns `true` iff this iterator and `rhs` point to the same key-block of
    // the same collection.
    bool operator==(const Key_Value_Iterator& rhs) const;

    // Returns `true` iff this iterator and `rhs` point to different key-blocks
    // of some collection(s).
    bool operator!=(const Key_Value_Iterator& rhs) const;

    // Tries to read-in at most `count` key-value pairs into `buf`. Returns the
    // number of elements read, which is 0 in the case when the end of the
    // collection has been reached. It is thread-safe.
    std::size_t advance(key_val_pair_t* buf, std::size_t count);
};

}



#endif

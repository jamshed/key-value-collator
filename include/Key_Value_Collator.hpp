
#ifndef KEY_VALUE_COLLATOR_HPP
#define KEY_VALUE_COLLATOR_HPP



// =============================================================================

namespace key_value_collator
{

// A class to: collate a collection of key-value pairs, deposited from multiple
// producers; and to iterate over the collated key-value collection. Keys are of
// type `T_key_`, values are of type `T_val_`, and the keys are hashed to their
// corresponding partitions with `operator()(T_key_ key)` of class `T_hasher_`.
template <typename T_key_, typename T_val_, typename T_hasher_>
class Key_Value_Collator
{};

}



#endif

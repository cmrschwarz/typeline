// This module implements a run-length encoded, dynamically typed data
// structure (FieldData) the type information for each entry is stored in a
// separate vec from the main data the type information itself is run-length
// encoded even if the data for the two entries is different

// SAFETY: due to its nature, this datastructure requires a lot of unsafe code,
// some of which is very repetitive. So far, nobody could be bothered
// with annotating every little piece of it.

pub mod command_buffer;
pub mod field_data;
pub mod field_manager;
pub mod iter_hall;
pub mod iters;
pub mod match_set_manager;
pub mod push_interface;
pub mod record_buffer;
pub mod record_set;
pub mod ref_iter;
pub mod stream_value_manager;
pub mod typed;
pub mod typed_iters;

#[macro_use]
extern crate abomonation;
extern crate differential_dataflow;

mod scored_item;

use abomonation::{Abomonation, encode, decode};
use differential_dataflow::Data;
use std::cmp::Ordering;

use scored_item::ScoredItem;

unsafe_abomonate!(ScoredItem : item, score);


#[test]
fn test_serialization() {

  let original_item = ScoredItem { item : 56, score: 7.3 };

  let mut bytes = Vec::new();
  unsafe { encode(&original_item, &mut bytes); }

  if let Some((restored_item, rest)) = unsafe { decode::<ScoredItem>(&mut bytes) } {
    assert!(restored_item == &original_item);
    assert!(rest.len() == 0);
    assert_eq!(original_item.score.partial_cmp(&restored_item.score), Some(Ordering::Equal));
  }
}

fn data_using_function<T: Data>(data: T) {}

#[test]
fn test_usable_as_data() {

  let item = ScoredItem { item : 56, score: 7.3 };
  data_using_function(&item);

}

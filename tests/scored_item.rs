use std::cmp::Ordering;

#[derive(Debug)]
pub struct ScoredItem {
  pub item: usize,
  pub score: f64,
}

impl Eq for ScoredItem {}

impl PartialEq for ScoredItem {
  fn eq(&self, other: &ScoredItem) -> bool {
    self.item == other.item
  }
}

impl Ord for ScoredItem {
  fn cmp(&self, other: &Self) -> Ordering {
    self.item.cmp(&other.item)
  }
}

impl PartialOrd for ScoredItem {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(&other))
  }
}
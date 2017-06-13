use std::cmp::Ordering;

#[derive(Eq,PartialEq,Debug)]
pub struct ScoredItem {
  pub item: usize,
  pub score: isize,
}

fn cmp_reverse(scored_item_a: &ScoredItem, scored_item_b: &ScoredItem) -> Ordering {
  match scored_item_a.score.cmp(&scored_item_b.score) {
    Ordering::Less => Ordering::Greater,
    Ordering::Greater => Ordering::Less,
    Ordering::Equal => Ordering::Equal
  }
}

impl Ord for ScoredItem {
  fn cmp(&self, other: &Self) -> Ordering { cmp_reverse(self, other) }
}

impl PartialOrd for ScoredItem {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(cmp_reverse(self, other)) }
}
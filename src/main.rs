extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::*;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::input::InputSession;

mod loglikelihoodratio;
mod scored_item;

use scored_item::ScoredItem;

fn main() {

  // define a new timely dataflow computation.
  timely::execute_from_args(std::env::args(), move |worker| {

    // capture parameters of the experiment.
    let users: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let items: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let scale: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(4).unwrap().parse().unwrap();
    let k: usize = std::env::args().nth(5).unwrap().parse().unwrap();

    let index = worker.index();
    let peers = worker.peers();

    let (mut input, probe) = worker.dataflow(|scope| {

      // input of (user, item) collection.
      let (input, occurrences) = scope.new_input();
      let occurrences = occurrences.as_collection();

      //TODO clarify assumption that we get no duplicates
      //TODO adjust code to only work with upper triangular half of cooccurrence matrix!

      /* Compute the cooccurrence matrix C = A'A from the binary interaction matrix A. */
      let cooccurrences = occurrences
        .join_map(&occurrences, |_user, &item_a, &item_b| (item_a, item_b))
        .filter(|&(item_a, item_b)| item_a != item_b)
        .count();

      /* compute the rowsums of C indicating how often we encounter individual items. */
      let row_sums = cooccurrences
        .map(|((item_a, _), num_cooccurrences)| (item_a, num_cooccurrences))
        .group(|_item, items_with_counts, output| {

          let row_sum = items_with_counts.iter()
            .fold (0, |sum, &item_with_count| sum + item_with_count.1);

          output.push((row_sum, 1));
        });

      /* Join the cooccurrence pairs with the corresponding row sums. */
      let cooccurrences_with_row_sums = cooccurrences
        .map(|((item_a, item_b), num_cooccurrences)| (item_a, (item_b, num_cooccurrences)))
        .join_map(&row_sums, |&item_a, &(item_b, num_cooccurrences), &row_sum_a| {
          assert!(row_sum_a > 0);
          (item_b, (item_a, num_cooccurrences, row_sum_a))
        })
        .join_map(&row_sums, |&item_b, &(item_a, num_cooccurrences, row_sum_a), &row_sum_b| {
          assert!(row_sum_a > 0);
          assert!(row_sum_b > 0);
          (item_a, (item_b, num_cooccurrences, row_sum_a, row_sum_b))
        });

//      cooccurrences_with_row_sums
//        .inspect(|record| println!("[cooccurrences_with_row_sums] {:?}", record));

      /* Compute LLR scores and emit highest scoring items per item. */
      let topk_cooccurring_items = cooccurrences_with_row_sums
        .group(move |_, items_with_infos, output| {

          //TODO we could compute the row_sum_a here and save a join

          let mut scored_items = items_with_infos.iter()
            .map(|&((item_b, num_cooccurrences, row_sum_a, row_sum_b), _)| {

            let k11: isize = num_cooccurrences;
            let k12: isize = row_sum_a as isize - k11;
            let k21: isize = row_sum_b as isize - k11;
            let k22: isize = 10000 - k12 - k21 + k11;

            let llr_score = loglikelihoodratio::log_likelihood_ratio(k11, k12, k21, k22);

            let llr_score = (llr_score * 1000.0) as isize;

            ScoredItem { item: item_b, score: llr_score }
          })
          .collect::<Vec<_>>();

          scored_items.sort();

          for scored_item in scored_items.into_iter().take(k) {
            output.push((scored_item.item, 1))
          }
        });


      let probe = topk_cooccurring_items
          .inspect(|x| println!("change: {:?}", x))
          .probe();

      (input, probe)
    });

    let seed: &[_] = &[1, 2, 3, index];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);  // rng for edge additions
    //let mut rng2: StdRng = SeedableRng::from_seed(seed);  // rng for edge deletions

    let mut input = InputSession::from(&mut input);

    for count in 0 .. scale {
      if count % peers == index {
        let user = rng1.gen_range(0, users);
        let item = rng1.gen_range(0, items);
        println!("[INITIAL INPUT] ({}, {})", user, item);
        input.insert((user, item));
      }
    }

    // load the initial data up!
    while probe.less_than(input.time()) { worker.step(); }

    for round in 1 .. 3 {

      println!("--------- Starting round {}", round);

      for element in (round * batch) .. ((round + 1) * batch) {
        if element % peers == index {
          // advance the input timestamp.
          input.advance_to(round * batch);
          // insert a new item.
          let user = rng1.gen_range(0, users);
          let item = rng1.gen_range(0, items);
          println!("[INPUT] ({}, {})", user, item);
          //TODO make sure we don't produce duplicates
          input.insert((user, item));
          // remove an old item.
          //let user = rng2.gen_range(0, users);
          //let item = rng2.gen_range(0, items);
          //input.remove((user, item));
        }
      }

      input.advance_to(round * batch);
      input.flush();

      while probe.less_than(input.time()) { worker.step(); }
    }
  }).unwrap();
}

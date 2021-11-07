use rand::Rng;

use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};

// NOTE: Overwrites output file.

fn main() {
  // Read and check arguments.
  let args: Vec<String> = env::args().collect();
  let in_file_name = &args[1];
  let out_file_name = &args[2];
  let sample_ratio = &args[3].parse::<f64>().unwrap();

  println!("input file: {}", in_file_name);
  println!("output file: {}", out_file_name);
  println!("sample ratio: {}", sample_ratio);

  // Open parquet reader.
  let file = File::open(&Path::new(in_file_name)).unwrap();
  let reader = SerializedFileReader::new(file).unwrap();

  // Get num rows.
  let parquet_metadata = reader.metadata();
  let file_metadata = parquet_metadata.file_metadata();
  let num_rows = file_metadata.num_rows();
  println!("num rows: {}", num_rows);

  // Calculate sample size.
  let sample_size = (num_rows as f64 * sample_ratio) as usize;
  println!("sample size: {}", sample_size);

  // Get `sample_size` random indexes.
  let mut random_indexes = HashSet::<usize>::new();
  let mut rng = rand::thread_rng();
  while random_indexes.len() < sample_size {
    let index = rng.gen_range(0..num_rows) as usize;
    random_indexes.insert(index);
  }

  // Copy set into sorted vector.
  let mut random_indexes_sorted: Vec<_> = random_indexes.into_iter().collect();
  random_indexes_sorted.sort();

  println!("reading input...");

  // Iterate through the entire input file (parquet doesn't support random access) and
  // collect sampled rows.
  let mut sampled_rows: Vec<parquet::record::Row> = Vec::new();
  let mut curr_row = 0;
  let mut curr_index = 0; // Index into `random_indexes_sorted`.
  let mut iter = reader.get_row_iter(None).unwrap();
  while let Some(row) = iter.next() {
    if curr_row % 1000000 == 0 {
      println!(
        "read {}% of input",
        (curr_row as f64 * 100.0 / num_rows as f64) as usize
      );
    }

    if curr_index == random_indexes_sorted.len() {
      // Enough samples => done.
      break;
    }

    if curr_row == random_indexes_sorted[curr_index] {
      sampled_rows.push(row);
      curr_index += 1;
    }

    curr_row += 1;
  }

  println!("finished reading input");

  parquet_sampler::write_output(&sampled_rows, file_metadata.schema(), out_file_name);
}

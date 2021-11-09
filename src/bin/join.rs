use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::path::Path;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;

// NOTE: Overwrites output file.

// EXAMPLE USAGE:
// ./join dim2.parquet d2_id attrib_0.001.parquet attrib_d2_id dim2_sj_reduced.parquet

fn main() {
  // Read and check arguments.
  let args: Vec<String> = env::args().collect();
  let in_file_name = &args[1];
  let in_file_col_name = &args[2];
  let semi_join_file_name = &args[3];
  let semi_join_file_col_name = &args[4];
  let out_file_name = &args[5];

  println!("input file: {}", in_file_name);
  println!("output file: {}", out_file_name);
  println!("semi join file: {}", semi_join_file_name);
  println!("semi join col: {}", semi_join_file_col_name);

  // 1) Read semi join file.

  // Open parquet reader.
  let semi_join_file = File::open(&Path::new(semi_join_file_name)).unwrap();
  let semi_join_reader = SerializedFileReader::new(semi_join_file).unwrap();

  // 2) Insert join column values into hash table.
  let semi_join_file_metadata = semi_join_reader.metadata().file_metadata();
  let num_build_rows = semi_join_file_metadata.num_rows();
  println!("num build side rows: {}", num_build_rows);

  println!("building hash table...");
  let mut ht = HashSet::<String>::new();

  // Build a schema projection.
  let fields = semi_join_file_metadata.schema().get_fields();
  let mut selected_fields = fields.to_vec();
  selected_fields.retain(|f| f.name() == semi_join_file_col_name);
  assert_eq!(selected_fields.len(), 1);
  let schema_projection = Type::group_type_builder("schema")
    .with_fields(&mut selected_fields)
    .build()
    .unwrap();

  // Scan column and build hash table.
  let mut curr_row = 0;
  let mut iter = semi_join_reader
    .get_row_iter(Some(schema_projection))
    .unwrap();
  while let Some(row) = iter.next() {
    if curr_row % 1000000 == 0 {
      println!(
        "read {}% of input",
        (curr_row as f64 * 100.0 / num_build_rows as f64) as usize
      );
    }

    // Access column value as string.
    let val = row
      .get_column_iter()
      .map(|c| c.1.to_string())
      .nth(0)
      .unwrap();

    ht.insert(val);

    curr_row += 1;
  }

  println!("finished building hash table (num entries: {})", ht.len());

  // 3) Read input file, probe hash table, and add matching rows to output.

  // Open parquet reader.
  let in_file = File::open(&Path::new(in_file_name)).unwrap();
  let in_reader = SerializedFileReader::new(in_file).unwrap();

  let in_file_metadata = in_reader.metadata().file_metadata();
  let num_probe_rows = in_file_metadata.num_rows();
  println!("num probe side rows: {}", num_probe_rows);

  // Resolve index of probe side column.
  let probe_side_fields = in_file_metadata.schema().get_fields();
  let mut probe_side_col_idx = 0;
  let probe_side_fields_vec = probe_side_fields.to_vec();
  for f in probe_side_fields.to_vec() {
    if f.name() == in_file_col_name {
      break;
    }
    probe_side_col_idx += 1;
  }
  assert_ne!(probe_side_col_idx, probe_side_fields_vec.len());

  println!("probing hash table...");

  let mut output_rows: Vec<parquet::record::Row> = Vec::new();
  curr_row = 0;
  iter = in_reader.get_row_iter(None).unwrap();
  while let Some(row) = iter.next() {
    if curr_row % 1000000 == 0 {
      println!(
        "read {}% of input",
        (curr_row as f64 * 100.0 / num_probe_rows as f64) as usize
      );
    }

    // Access column value as string.
    let val = row
      .get_column_iter()
      .map(|c| c.1.to_string())
      .nth(probe_side_col_idx)
      .unwrap();

    // Add matching rows to output.
    if ht.contains(&val) {
      output_rows.push(row);
    }

    curr_row += 1;
  }

  println!("finished probing hash table");

  println!(
    "semi join reduced input by {} rows",
    num_probe_rows as usize - output_rows.len()
  );

  parquet_sampler::write_output(&output_rows, in_file_metadata.schema(), out_file_name);
}

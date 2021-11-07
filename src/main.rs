use rand::Rng;

use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::record::RowAccessor;

use parquet::schema::parser::parse_message_type;
use parquet::schema::printer;

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

  // Create output parquet file.
  let out_path = Path::new(&out_file_name);
  let file = File::create(&out_path).unwrap();

  // Parquet writer requires the schema as a string.
  let mut buf = Vec::new();
  printer::print_schema(&mut buf, &file_metadata.schema());
  let string_schema = String::from_utf8(buf).unwrap();

  // Create parquet writer.
  let schema = Arc::new(parse_message_type(&string_schema).unwrap());
  let props = Arc::new(WriterProperties::builder().build());
  let mut file_writer = SerializedFileWriter::new(file, schema, props).unwrap();
  let mut row_group_writer = file_writer.next_row_group().unwrap();

  println!("writing output...");

  let mut curr_column = 0;
  while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    // We need to pass "definition levels" to `write_batch()` since the schema might
    // contain OPTIONAL fields.
    let mut def_levels = Vec::with_capacity(sampled_rows.len());

    match col_writer {
      ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_bool(curr_column) {
            col.push(val);
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_int(curr_column) {
            col.push(val);
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_long(curr_column) {
            col.push(val);
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      ColumnWriter::FloatColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_float(curr_column) {
            col.push(val);
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      ColumnWriter::DoubleColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_double(curr_column) {
            col.push(val);
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(sampled_rows.len());
        for row in &sampled_rows {
          if let Ok(val) = row.get_string(curr_column) {
            col.push(parquet::data_type::ByteArray::from(&val[..]));
            def_levels.push(1);
          } else {
            def_levels.push(0);
          }
        }
        typed_writer
          .write_batch(&col[..], Some(&def_levels[..]), None)
          .unwrap();
      }
      _ => {
        unimplemented!();
      }
    }
    row_group_writer.close_column(col_writer).unwrap();
    curr_column += 1;
  }

  file_writer.close_row_group(row_group_writer).unwrap();
  file_writer.close().unwrap();

  println!("finished writing output");
}

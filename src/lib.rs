use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::column::writer::ColumnWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::{FileWriter, SerializedFileWriter};
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;
use parquet::schema::printer;
use parquet::schema::types::Type;

pub fn write_output(rows: &Vec<parquet::record::Row>, schema: &Type, file_name: &String) {
  // Create output parquet file.
  let path = Path::new(&file_name);
  let file = File::create(&path).unwrap();

  // Parquet writer requires the schema as a string.
  let mut buf = Vec::new();
  printer::print_schema(&mut buf, schema);
  let str_schema = String::from_utf8(buf).unwrap();

  // Create parquet writer.
  let parsed_schema = Arc::new(parse_message_type(&str_schema).unwrap());
  let props = Arc::new(WriterProperties::builder().build());
  let mut file_writer = SerializedFileWriter::new(file, parsed_schema, props).unwrap();
  let mut row_group_writer = file_writer.next_row_group().unwrap();

  println!("writing output...");

  let mut curr_column = 0;
  while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
    // We need to pass "definition levels" to `write_batch()` since the schema might
    // contain OPTIONAL fields.
    let mut def_levels = Vec::with_capacity(rows.len());

    match col_writer {
      ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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
        let mut col = Vec::with_capacity(rows.len());
        for row in rows {
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

# parquet-sampler

`sample` downsamples parquet files.

`join` reduces a dimension table using a downsampled fact table.

## Build
```
git clone git@github.com:andreaskipf/parquet-sampler.git
cd parquet-sampler/
cargo build --release
cd target/release/
```

## Sample fact tables individually
```
./sample attrib.parquet attrib_0.001.parquet 0.001
./sample fact.parquet fact_0.001.parquet 0.001
```

## Semi-join reduce dimension tables
```
./join dim1.parquet d1_id fact_0.001.parquet f_d1_id dim1_sj_reduced.parquet
./join dim2.parquet d2_id attrib_0.001.parquet attrib_d2_id dim2_sj_reduced.parquet
./join dim3.parquet d3_id fact_0.001.parquet f_d3_id dim3_sj_reduced.parquet
```

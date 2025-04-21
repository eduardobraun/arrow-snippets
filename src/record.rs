#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{Array, AsArray, Float32Array, Int32Array, RecordBatch},
        datatypes::{Float32Type, Int32Type},
    };

    #[test]
    fn create_record_batch() {
        let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
        let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

        let mut batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
        assert_eq!(2, batch.schema().fields().len());
        let _ = batch.remove_column(0);
        assert_eq!(1, batch.schema().fields().len());
    }

    #[test]
    fn add_new_column() {
        let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
        let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;
        let batch = RecordBatch::try_from_iter([("col1", col_1), ("col2", col_2)]).unwrap();

        // compute the new column
        let col_1 = batch.column(0).as_primitive::<Int32Type>();
        let col_2 = batch.column(1).as_primitive::<Float32Type>();
        let new_col = Arc::new(
            arrow::compute::binary::<_, _, _, Float32Type>(col_1, col_2, |a, b| a as f32 * b)
                .unwrap(),
        ) as Arc<dyn Array>;

        // clone the columns from previous batch, its cheap since they are Arc<dyn Array>
        let mut cols: Vec<_> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.name().as_str(),
                    batch
                        // could use `column(index)`, but not sure if the order is consistent
                        // across schema and batch (probably)
                        .column_by_name(field.name())
                        .expect("should be there since its in the schema")
                        .clone(),
                )
            })
            .collect();
        // add new column
        cols.push(("new_col", new_col));
        // create new batch with all the columns
        let new_batch = RecordBatch::try_from_iter(cols).unwrap();

        let new_col = new_batch
            .column_by_name("new_col")
            .unwrap()
            .as_primitive::<Float32Type>();
        assert_eq!(&[1.0, 12.6, 12.0], new_col.values());
    }
}

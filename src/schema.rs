#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn simple_schema() {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Boolean, false);
        let schema = Schema::new(vec![field_a, field_b]);
        assert_eq!(2, schema.fields().len());
        // select only column 1
        let new_schema = schema.project(&[1]).unwrap();
        assert_eq!(1, new_schema.fields().len());
    }
}

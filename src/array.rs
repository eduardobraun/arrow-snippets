#[cfg(test)]
mod test {
    use arrow::{
        array::{Array, AsArray, Int32Builder, StringBuilder, StructBuilder, UInt64Builder},
        datatypes::{DataType, Field, UInt64Type},
    };

    #[test]
    fn primitive_array() {
        let mut builder = Int32Builder::new();
        builder.append_values(&[1, 2, 3, 4], &[true, true, true, true]);
        builder.append_value(5);
        let array = builder.finish();
        assert_eq!(&[1, 2, 3, 4, 5], array.values())
    }

    #[test]
    fn primitive_array_map() {
        let mut builder = Int32Builder::new();
        builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);
        let array = builder.finish();
        // unary_mut reutilizes the internal array structure, its useful when mapping from T -> T
        let new_array = array.unary_mut(|value| value * 2).unwrap();
        // we cannot use original array anymore
        // assert_eq!(5, array.len());
        assert_eq!(&[2, 4, 6, 8, 10], new_array.values())
    }

    #[test]
    fn primitive_array_map2() {
        let mut builder = Int32Builder::new();
        builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);
        let array = builder.finish();
        // unary allows us to map to other types
        let new_array = array.unary::<_, UInt64Type>(|value| (value * 2) as u64);
        // we can still use the original array
        assert_eq!(5, array.len());
        assert_eq!(&[2, 4, 6, 8, 10], new_array.values());
    }

    #[test]
    fn primitive_array_map3() {
        let mut builder = Int32Builder::new();
        builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);
        let array1 = builder.finish();
        let mut builder = UInt64Builder::new();
        builder.append_values(&[1, 2, 3, 4, 5], &[true, true, true, true, true]);
        let array2 = builder.finish();
        // `arrow::compute::binary` allows to map two arrays to a new one
        // similar to `a.iter().zip(b.iter()).map()`
        let array3 =
            arrow::compute::binary::<_, _, _, UInt64Type>(&array1, &array2, |a, b| a as u64 + b)
                .unwrap();
        assert_eq!(5, array3.len());
        assert_eq!(&[2, 4, 6, 8, 10], array3.values());
    }

    #[test]
    fn struct_array() {
        let mut struct_builder = StructBuilder::from_fields(
            vec![
                Field::new("a", DataType::UInt64, false),
                Field::new("b", DataType::Utf8, false),
            ],
            5,
        );
        // you may want to take the field builders ref only once, but this complicates the
        // `struct_builder.append()` in this example
        for i in 0u64..5 {
            if let [field_0_builder, field_1_builder] = struct_builder.field_builders_mut() {
                let u64_builder: &mut UInt64Builder =
                    field_0_builder.as_any_mut().downcast_mut().unwrap();
                let string_builder: &mut StringBuilder =
                    field_1_builder.as_any_mut().downcast_mut().unwrap();
                u64_builder.append_value(i + 1);
                string_builder.append_value(format!("string #{i}"));
            }
            // must keep struct appends consistent with the fields
            struct_builder.append(true);
        }
        let struct_array = struct_builder.finish();

        let field_0_array = struct_array.column(0);
        let field_1_array = struct_array.column(1);
        // there are helpers for casting dyn Array. `as_*` can panic, `as_*_opt` returns None on
        // failure
        let u64_array = field_0_array.as_primitive::<UInt64Type>();
        let string_array = field_1_array.as_string_opt::<i32>().unwrap();
        // but you can also downcast directly:
        // let string_array = field_1_array
        //     .as_any()
        //     .downcast_ref::<StringArray>()
        //     .unwrap();

        assert_eq!(&[1, 2, 3, 4, 5], u64_array.values());
        assert_eq!(5, string_array.len());
        for i in 0..5 {
            assert_eq!(format!("string #{i}"), string_array.value(i));
        }
    }
}

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::writer::InMemoryWriteableCursor;
use uuid::Uuid;

pub struct File {
    pub name: String,
    pub content: Vec<u8>,
}

pub struct Writer {
    schema: SchemaRef,
}

impl Writer {
    pub fn new(schema: SchemaRef) -> Writer {
        Writer { schema }
    }

    pub fn write(&self, record_batch: RecordBatch) -> File {
        let cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(cursor.clone(), self.schema.clone(), None).unwrap();
        writer.write(&record_batch).unwrap();
        writer.close().unwrap();

        // TODO: use column stats to generate name.
        File {
            name: format!("parquet-{}", Uuid::new_v4()),
            content: cursor.data(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Writer;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use log::debug;
    use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
    use parquet::file::serialized_reader::{SerializedFileReader, SliceableCursor};
    use std::sync::Arc;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn roundtrip() {
        init();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let a = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()]).unwrap();

        let writer = Writer::new(schema.clone());
        let file = writer.write(batch);

        let cursor = SliceableCursor::new(file.content);
        let reader = SerializedFileReader::new(cursor).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));

        let readers = vec![
            arrow_reader
                .get_record_reader_by_columns(vec![0], 1024)
                .unwrap(),
            arrow_reader.get_record_reader(1024).unwrap(),
        ];

        for mut reader in readers {
            let actual_batch = reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");
            debug!("Actual_batch: {:#?}", actual_batch);
            assert_eq!(3, actual_batch.num_rows());
            assert_eq!(1, actual_batch.num_columns());
            let actual_col = actual_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(*a, *actual_col);
        }
    }
}

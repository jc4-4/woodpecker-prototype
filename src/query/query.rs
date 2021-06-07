#[cfg(test)]
mod tests {
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn roundtrip() -> datafusion::error::Result<()> {
        init();

        let mut ctx = ExecutionContext::new();
        ctx.register_csv("example", "testinput/example.csv", CsvReadOptions::new())?;

        // create a plan to run a SQL query
        let df = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a ORDER BY a LIMIT 100")?;

        // execute and print results
        let results: Vec<RecordBatch> = df.collect().await?;
        assert_eq!(
            pretty_format_batches(&results)?,
            "+---+--------+\n\
            | a | MIN(b) |\n\
            +---+--------+\n\
            | 1 | a      |\n\
            | a | 1      |\n\
            +---+--------+\n"
        );
        Ok(())
    }
}

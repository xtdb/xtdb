use adbc_core::options::{AdbcVersion, IngestMode, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::Result;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use std::env;
use std::sync::Arc;
use uuid::Uuid;

fn connect(db_name: &str) -> Result<ManagedConnection> {
    let host = env::var("FLIGHT_SQL_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("FLIGHT_SQL_PORT").unwrap_or_else(|_| "3000".to_string());

    let mut driver = ManagedDriver::load_from_name("flightsql", None, AdbcVersion::default(), LOAD_FLAG_DEFAULT, None)?;
    let database = driver.new_database_with_opts([
        (OptionDatabase::Uri, OptionValue::String(format!("grpc://{host}:{port}"))),
        (
            OptionDatabase::Other("adbc.flight.sql.rpc.call_header.x-xtdb-database".to_string()),
            OptionValue::String(db_name.to_string()),
        ),
    ])?;

    Ok(database.new_connection()?)
}

#[test]
fn test_ingest_creates_a_table_on_a_fresh_database() -> Result<()> {
    let mut conn = connect(&Uuid::new_v4().to_string())?;

    let mut ingest = conn.new_statement()?;
    ingest.set_option(OptionStatement::TargetTable, OptionValue::String("foo".to_string()))?;
    ingest.set_option(OptionStatement::IngestMode, IngestMode::CreateAppend.into())?;
    ingest.bind(RecordBatch::try_from_iter([
        ("_id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("v", Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef),
    ])?)?;
    ingest.execute_update()?;

    let mut query = conn.new_statement()?;
    query.set_sql_query("SELECT _id, v FROM foo ORDER BY _id")?;

    let mut rows = Vec::new();
    for batch in query.execute()? {
        let batch = batch?;
        let ids = batch.column_by_name("_id").unwrap().as_primitive::<Int64Type>();
        let vs = batch.column_by_name("v").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), vs.value(i).to_string()));
        }
    }

    assert_eq!(rows, vec![(1, "a".to_string()), (2, "b".to_string())]);

    Ok(())
}

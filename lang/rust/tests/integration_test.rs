use anyhow::Result;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::env;
use uuid::Uuid;

async fn create_pool() -> Result<PgPool> {
    let host = env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("PG_PORT")
        .unwrap_or_else(|_| "5439".to_string())
        .parse::<u16>()?;
    let database = Uuid::new_v4().to_string();

    let options = PgConnectOptions::new()
        .host(&host)
        .port(port)
        .database(&database);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;

    Ok(pool)
}

#[tokio::test]
async fn test_basic_connectivity() -> Result<()> {
    let pool = create_pool().await?;

    let row = sqlx::query("SELECT 1 as value")
        .fetch_one(&pool)
        .await?;

    let value: i64 = row.get("value");
    assert_eq!(value, 1);

    Ok(())
}

#[tokio::test]
async fn test_insert_and_query() -> Result<()> {
    let pool = create_pool().await?;

    sqlx::query("INSERT INTO foo (_id, msg) VALUES ($1, $2)")
        .bind(1_i64)
        .bind("Hello world!")
        .execute(&pool)
        .await?;

    let row = sqlx::query("SELECT _id, msg FROM foo")
        .fetch_one(&pool)
        .await?;

    let id: i64 = row.get("_id");
    let msg: String = row.get("msg");

    assert_eq!(id, 1);
    assert_eq!(msg, "Hello world!");

    Ok(())
}

#[tokio::test]
async fn test_temporal_queries() -> Result<()> {
    let pool = create_pool().await?;

    // Insert first version
    sqlx::query("INSERT INTO foo (_id) VALUES ($1)")
        .bind(1_i64)
        .execute(&pool)
        .await?;

    // Insert second version (same _id)
    sqlx::query("INSERT INTO foo (_id) VALUES ($1)")
        .bind(1_i64)
        .execute(&pool)
        .await?;

    // Insert third version (same _id)
    sqlx::query("INSERT INTO foo (_id) VALUES ($1)")
        .bind(1_i64)
        .execute(&pool)
        .await?;

    // Query current state (should only see one row)
    let rows = sqlx::query("SELECT _id FROM foo")
        .fetch_all(&pool)
        .await?;
    assert_eq!(rows.len(), 1);

    // Query all temporal versions - should have more than just the current version
    let rows = sqlx::query("SELECT _id FROM foo FOR ALL VALID_TIME FOR ALL SYSTEM_TIME")
        .fetch_all(&pool)
        .await?;
    assert!(rows.len() > 1, "Expected multiple temporal versions, got {}", rows.len());

    Ok(())
}

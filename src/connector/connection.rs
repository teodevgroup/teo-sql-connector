use std::fmt::{Debug, Formatter};
use std::sync::{Arc};
use tokio::sync::Mutex;
use async_trait::async_trait;
use once_cell::sync::Lazy;
use quaint_forked::{pooled::Quaint};
use quaint_forked::connector::start_owned_transaction;
use crate::connector::transaction::SQLTransaction;
use crate::migration::migrate::SQLMigration;
use crate::schema::dialect::SQLDialect;
use crate::url::url_utils;
use teo_runtime::connection::connection::Connection;
use teo_result::{Error, Result};
use teo_runtime::connection::transaction::Transaction;

pub(crate) struct SQLConnection {
    dialect: SQLDialect,
    pool: Quaint,
    memory_mode: bool,
}

impl Debug for SQLConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl SQLConnection {

    pub(crate) async fn new(dialect: SQLDialect, url: &str, reset: bool) -> Self {
        SQLMigration::create_database_if_needed(dialect, url, reset).await;
        let url = url_utils::normalized_url(dialect, url);
        let pool = Quaint::builder(url.as_str()).unwrap().build();
        Self { dialect, pool, memory_mode: url.to_string().contains(":memory:") }
    }

    async fn sqlite_memory_transaction(&self) -> Result<Arc<dyn Transaction>> {
        let mut connection = UNIQUE_TRANSACTION.lock().await;
        if connection.is_none() {
            let result = {
                let pooled_connection = self.pool.check_out().await;
                if pooled_connection.is_err() {
                    Err(Error::new(format!("cannot create pooled connection: {}", pooled_connection.err().unwrap().to_string())))
                } else {
                    Ok(Arc::new(SQLTransaction::new(self.dialect, Arc::new(pooled_connection.unwrap()), None)))
                }
            }?;
            *connection = Some(result.clone());
            Ok(result)
        } else {
            Ok(connection.clone().unwrap())
        }
    }
}

static UNIQUE_TRANSACTION: Lazy<Mutex<Option<Arc<dyn Transaction>>>> = Lazy::new(|| {
    Mutex::new(None)
});

#[async_trait]
impl Connection for SQLConnection {

    async fn transaction(&self) -> Result<Arc<dyn Transaction>> {
        if self.memory_mode && self.dialect.is_sqlite() {
            return self.sqlite_memory_transaction().await;
        }
        match self.pool.check_out().await {
            Ok(pooled_connection) => {
                let pooled_connection = Arc::new(pooled_connection);
                let transaction = start_owned_transaction(pooled_connection.clone(), None).await.unwrap();
                Ok(Arc::new(SQLTransaction {
                    dialect: self.dialect,
                    conn: pooled_connection,
                    tran: Some(Arc::new(transaction)),
                }))
            }
            Err(err) => {
                Err(Error::new(format!("cannot create pooled connection: {}", err.to_string())))
            }
        }
    }

    async fn no_transaction(&self) -> Result<Arc<dyn Transaction>> {
        if self.memory_mode && self.dialect.is_sqlite() {
            return self.sqlite_memory_transaction().await;
        }
        let pooled_connection = self.pool.check_out().await;
        if pooled_connection.is_err() {
            Err(Error::new(format!("cannot create pooled connection: {}", pooled_connection.err().unwrap().to_string())))
        } else {
            Ok(Arc::new(SQLTransaction::new(self.dialect, Arc::new(pooled_connection.unwrap()), None)))
        }
    }
}

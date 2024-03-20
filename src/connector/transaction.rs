use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_trait::async_trait;
use itertools::Itertools;
use quaint_forked::{prelude::*, ast::Query as QuaintQuery};
use quaint_forked::error::DatabaseConstraint;
use quaint_forked::error::ErrorKind::UniqueConstraintViolation;
use quaint_forked::pooled::PooledConnection;
use quaint_forked::connector::owned_transaction::OwnedTransaction;
use quaint_forked::connector::start_owned_transaction;
use teo_runtime::model::Model;
use crate::execution::Execution;
use crate::migration::migrate::SQLMigration;
use crate::query::Query;
use crate::stmts::SQL;
use crate::schema::dialect::SQLDialect;
use crate::schema::value::decode::RowDecoder;
use crate::schema::value::encode::ToSQLString;
use crate::schema::value::encode::PSQLArrayToSQLString;
use teo_runtime::action::Action;
use teo_runtime::model::object::input::Input;
use teo_runtime::model::Object;
use teo_runtime::connection::connection::Connection;
use teo_runtime::{teon, value::Value};
use teo_result::{Result, Error};
use teo_runtime::connection::transaction;
use teo_runtime::model::field::column_named::ColumnNamed;
use teo_runtime::connection::transaction::Transaction;
use teo_runtime::model::field::typed::Typed;
use teo_runtime::error_ext;
use teo_runtime::request::Ctx;
use key_path::KeyPath;

#[derive(Clone)]
pub struct SQLTransaction {
    pub dialect: SQLDialect,
    pub conn: Arc<PooledConnection>,
    pub tran: Option<Arc<OwnedTransaction>>,
    pub committed: Arc<AtomicBool>,
}

impl Debug for SQLTransaction {

    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl SQLTransaction {
    pub(super) fn new(dialect: SQLDialect, conn: Arc<PooledConnection>, tran: Option<Arc<OwnedTransaction>>) -> Self {
        Self {
            dialect, conn, tran, committed: Arc::new(AtomicBool::new(false))
        }
    }
}

impl SQLTransaction {

    fn queryable(&self) -> &dyn Queryable {
        if let Some(tran) = &self.tran {
            tran.as_ref()
        } else {
            self.conn()
        }
    }

    fn dialect(&self) -> SQLDialect {
        self.dialect
    }

    fn conn(&self) -> &PooledConnection {
        self.conn.as_ref()
    }

    fn tran(&self) -> Option<&Arc<OwnedTransaction>> {
        self.tran.as_ref()
    }

    async fn create_object(&self, object: &Object, path: KeyPath) -> teo_result::Result<()> {
        let model = object.model();
        let keys = object.keys_for_save();
        let auto_keys = &model.cache.auto_keys;
        let mut values: Vec<(&str, String)> = vec![];
        for key in keys {
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                let val = object.get_value(key).unwrap();
                if !(field.auto_increment && val.is_null()) {
                    values.push((column_name, PSQLArrayToSQLString::to_string_with_ft(&val, self.dialect(), field.r#type())));
                }
            } else if let Some(property) = model.property(key) {
                let val: Value = object.get_property_value(key).await?;
                values.push((key, PSQLArrayToSQLString::to_string_with_ft(&val, self.dialect(), property.r#type())));
            }
        }
        let value_refs: Vec<(&str, &str)> = values.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let stmt = SQL::insert_into(&model.table_name).values(value_refs).returning(auto_keys).to_string(self.dialect());
        // println!("create stmt: {}", stmt);
        if self.dialect() == SQLDialect::PostgreSQL {
            match self.queryable().query(QuaintQuery::from(stmt)).await {
                Ok(result_set) => {
                    let columns = result_set.columns().clone();
                    let result = result_set.into_iter().next();
                    if result.is_some() {
                        let value = Execution::row_to_value(object.namespace(), model, &result.unwrap(), &columns, self.dialect());
                        for (k, v) in value.as_dictionary().unwrap() {
                            object.set_value(k, v.clone())?;
                        }
                    }
                    Ok(())
                }
                Err(err) => {
                    Err(self.handle_err_result(err, path))
                }
            }
        } else {
            match self.conn().query(QuaintQuery::from(stmt)).await {
                Ok(result) => {
                    if let Some(id) = result.last_insert_id() {
                        for key in auto_keys {
                            if model.field(key).unwrap().r#type().is_int() {
                                object.set_value(key, Value::Int(id as i32))?;
                            } else {
                                object.set_value(key, Value::Int64(id as i64))?;
                            }
                        }
                    }
                    Ok(())
                }
                Err(err) => {
                    Err(self.handle_err_result(err, path))
                }
            }
        }
    }

    async fn update_object(&self, object: &Object, path: KeyPath) -> teo_result::Result<()> {
        let model = object.model();
        let keys = object.keys_for_save();
        let mut values: Vec<(&str, String)> = vec![];
        for key in &keys {
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                if let Some(updator) = object.get_atomic_updator(key) {
                    let (key, val) = Input::key_value(updator.as_dictionary().unwrap());
                    match key {
                        "increment" => values.push((column_name, format!("{} + {}", column_name, ToSQLString::to_string(&val, self.dialect())))),
                        "decrement" => values.push((column_name, format!("{} - {}", column_name, ToSQLString::to_string(&val, self.dialect())))),
                        "multiply" => values.push((column_name, format!("{} * {}", column_name, ToSQLString::to_string(&val, self.dialect())))),
                        "divide" => values.push((column_name, format!("{} / {}", column_name, ToSQLString::to_string(&val, self.dialect())))),
                        "push" => values.push((column_name, format!("ARRAY_APPEND({}, {})", column_name, ToSQLString::to_string(&val, self.dialect())))),
                        _ => unreachable!(),
                    }
                } else {
                    let val = object.get_value(key).unwrap();
                    values.push((column_name, PSQLArrayToSQLString::to_string_with_ft(&val, self.dialect(), field.r#type())));
                }
            } else if let Some(property) = model.property(key) {
                let val: Value = object.get_property_value(key).await?;
                values.push((key, PSQLArrayToSQLString::to_string_with_ft(&val, self.dialect(), property.r#type())));
            }
        }
        let value_refs: Vec<(&str, &str)> = values.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let identifier = object.identifier();
        let r#where = Query::where_from_previous_identifier(object, self.dialect());
        if !value_refs.is_empty() {
            let stmt = SQL::update(&model.table_name).values(value_refs).r#where(&r#where).to_string(self.dialect());
            // println!("update stmt: {}", stmt);
            let result = self.conn().execute(QuaintQuery::from(stmt)).await;
            if result.is_err() {
                return Err(error_ext::unknown_database_write_error(path.clone(), format!("{:?}", result.err().unwrap())));
            }
        }
        let result = Execution::query(object.namespace(), self.queryable(), model, &teon!({"where": identifier, "take": 1i64}), self.dialect(), path.clone()).await?;
        if result.is_empty() {
            Err(Error::not_found_pathed(path.clone(), "not found"))
        } else {
            object.set_from_database_result_value(result.get(0).unwrap(), None, None);
            Ok(())
        }
    }

    fn handle_err_result(&self, err: quaint_forked::error::Error, path: KeyPath) -> teo_result::Error {
        match err.kind() {
            UniqueConstraintViolation { constraint } => {
                match constraint {
                    DatabaseConstraint::Fields(fields) => {
                        if fields.len() == 1 {
                            error_ext::unique_value_duplicated(path + fields.get(0).unwrap(), fields.get(0).unwrap().to_string())
                        } else {
                            error_ext::unique_value_duplicated(path, fields.iter().map(|f| f).join(", "))
                        }
                    }
                    DatabaseConstraint::Index(index) => {
                        error_ext::unique_value_duplicated(path, index.clone())
                    }
                    _ => {
                        error_ext::unknown_database_write_error(path, format!("{}", err))
                    }
                }
            }
            _ => {
                error_ext::unknown_database_write_error(path, format!("{}", err))
            }
        }
    }

}

#[async_trait]
impl Transaction for SQLTransaction {

    async fn migrate(&self, models: Vec<&Model>, dry_run: bool, reset_database: bool, silent: bool) -> Result<()> {
        SQLMigration::migrate(self.dialect(), self.queryable(), models, self).await
    }

    async fn purge(&self, models: Vec<&Model>) -> Result<()> {
        for model in models {
            let escape = self.dialect().escape();
            self.conn().execute(QuaintQuery::from(format!("DELETE FROM {escape}{}{escape}", &model.table_name))).await.unwrap();
        }
        Ok(())
    }

    async fn query_raw(&self, value: &Value) -> Result<Value> {
        let result = self.queryable().query(QuaintQuery::from(value.as_str().unwrap())).await;
        if result.is_err() {
            let err = result.unwrap_err();
            let msg = err.original_message();
            return Err(error_ext::invalid_sql_query(msg.unwrap()).into());
        } else {
            let result = result.unwrap();
            return if result.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(RowDecoder::decode_raw_result_set(result))
            }
        }
    }

    async fn save_object(&self, object: &Object, path: KeyPath) -> teo_result::Result<()> {
        if object.is_new() {
            self.create_object(object, path).await
        } else {
            self.update_object(object, path).await
        }
    }

    async fn delete_object(&self, object: &Object, path: KeyPath) -> teo_result::Result<()> {
        if object.is_new() {
            return Err(error_ext::object_is_not_saved_thus_cant_be_deleted(path));
        }
        let model = object.model();
        let r#where = Query::where_from_identifier(object, self.dialect());
        let stmt = SQL::delete_from(&model.table_name).r#where(r#where).to_string(self.dialect());
        // println!("see delete stmt: {}", stmt);
        let result = self.queryable().execute(QuaintQuery::from(stmt)).await;
        if result.is_err() {
            return Err(error_ext::unknown_database_write_error(path, format!("{:?}", result.err().unwrap())));
        } else {
            Ok(())
        }
    }

    async fn find_unique(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: transaction::Ctx, req_ctx: Option<Ctx>, path: KeyPath) -> teo_result::Result<Option<Object>> {
        let objects = Execution::query_objects(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), action, transaction_ctx, req_ctx, path).await?;
        if objects.is_empty() {
            Ok(None)
        } else {
            Ok(Some(objects.get(0).unwrap().clone()))
        }
    }

    async fn find_many(&self, model: &'static Model, finder: &Value, ignore_select_and_include: bool, action: Action, transaction_ctx: transaction::Ctx, req_ctx: Option<Ctx>, path: KeyPath) -> teo_result::Result<Vec<Object>> {
        Execution::query_objects(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), action, transaction_ctx, req_ctx, path).await
    }

    async fn count(&self, model: &'static Model, finder: &Value, transaction_ctx: transaction::Ctx, path: KeyPath) -> teo_result::Result<Value> {
        Execution::query_count(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), path).await
    }

    async fn count_objects(&self, model: &'static Model, finder: &Value, transaction_ctx: transaction::Ctx, path: KeyPath) -> teo_result::Result<usize> {
        Execution::query_count_objects(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), path).await
    }

    async fn count_fields(&self, model: &'static Model, finder: &Value, transaction_ctx: transaction::Ctx, path: KeyPath) -> teo_result::Result<Value> {
        Execution::query_count_fields(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), path).await
    }

    async fn aggregate(&self, model: &'static Model, finder: &Value, transaction_ctx: transaction::Ctx, path: KeyPath) -> teo_result::Result<Value> {
        Execution::query_aggregate(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), path).await
    }

    async fn group_by(&self, model: &'static Model, finder: &Value, transaction_ctx: transaction::Ctx, path: KeyPath) -> teo_result::Result<Vec<Value>> {
        Execution::query_group_by(transaction_ctx.namespace(), self.queryable(), model, finder, self.dialect(), path).await
    }

    fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    fn is_transaction(&self) -> bool {
        self.tran.is_some()
    }

    async fn commit(&self) -> Result<()> {
        if let Some(tran) = &self.tran {
            match tran.commit().await {
                Ok(()) => (),
                Err(err) => return Err(Error::new(err.to_string()))
            }
        }
        self.committed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn abort(&self) -> Result<()> {
        if let Some(tran) = &self.tran {
            match tran.rollback().await {
                Ok(()) => (),
                Err(err) => return Err(Error::new(err.to_string()))
            };
        }
        Ok(())
    }

    async fn spawn(&self) -> Result<Arc<dyn Transaction>> {
        Ok(Arc::new(SQLTransaction {
            dialect: self.dialect,
            conn: self.conn.clone(),
            tran: Some(Arc::new(start_owned_transaction(self.conn.clone(), None).await.unwrap())),
            committed: Arc::new(AtomicBool::new(false)),
        }))
    }
}

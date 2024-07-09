use std::collections::HashSet;
use std::fs;
use itertools::Itertools;
use maplit::hashset;
use quaint_forked::pooled::{Quaint};
use quaint_forked::prelude::Queryable;
use quaint_forked::ast::Query;
use crate::migration::sql::{sqlite_auto_increment_query, sqlite_list_indices_query};
use super::super::url::url_utils;
use crate::schema::column::decoder::{ColumnDecoder, ColumnManipulation};
use crate::stmts::create::table::SQLCreateTableStatement;
use crate::stmts::SQL;
use crate::schema::column::SQLColumn;
use crate::schema::dialect::SQLDialect;
use teo_runtime::model::Model;
use crate::schema::value::encode::ToSQLString;
use teo_runtime::connection::connection::Connection;
use teo_runtime::connection::transaction::Transaction;
use teo_runtime::sort::Sort;
use teo_runtime::model::{Index, index::Item};
use teo_runtime::model::index::Type;
use teo_result::{Result};
use crate::exts::database_type::DatabaseTypeToSQLString;
use crate::exts::index::IndexExt;
use crate::exts::sort::SortExt;

pub(crate) struct SQLMigration { }

impl SQLMigration {

    // Create database

    pub(crate) async fn create_database_if_needed(dialect: SQLDialect, url: &str, reset: bool) {
        match dialect {
            SQLDialect::SQLite => Self::create_sqlite_database_if_needed(url, reset).await,
            _ => Self::create_server_database_if_needed(dialect, url, reset).await,
        }
    }

    pub(crate) async fn create_sqlite_database_if_needed(url: &str, reset: bool) {
        let url = url_utils::remove_scheme(url);
        if url_utils::is_memory_url(url) {
            return
        }
        let absolutized_url = url_utils::absolutized(url);
        if absolutized_url.exists() && reset {
            // delete the old one
            let _ = fs::remove_file(&absolutized_url);
        }
        if !absolutized_url.exists() || reset {
            // create a new one
            fs::File::create(absolutized_url).expect("SQLite database file create failed.");
        }
    }

    pub(crate) async fn create_server_database_if_needed(dialect: SQLDialect, url: &str, reset: bool) {
        let url = url_utils::normalized_url(dialect, url);
        let db_name = &url.path()[1..];
        let url_without_db = url_utils::remove_db_path(dialect, &url);
        let pool = Quaint::builder(url_without_db.as_str()).unwrap().build();
        let conn = pool.check_out().await.unwrap();
        // drop database if needed
        if reset {
            let stmt = SQL::drop().database(db_name).if_exists().to_string(dialect);
            conn.execute(Query::from(stmt)).await.unwrap();
        }
        // create database if needed
        if dialect == SQLDialect::PostgreSQL {
            let stmt = format!("select from pg_database where datname = '{}'", db_name);
            let result = conn.query(Query::from(stmt)).await.unwrap();
            if result.is_empty() {
                let stmt = SQL::create().database(db_name).to_string(dialect);
                conn.execute(Query::from(stmt)).await.unwrap();
            }
        } else {
            let stmt = SQL::create().database(db_name).if_not_exists().to_string(dialect);
            conn.execute(Query::from(stmt)).await.unwrap();
        }
        // use database
        if dialect == SQLDialect::PostgreSQL {
            let stmt = format!("SET search_path TO {db_name}");
            conn.execute(Query::from(stmt)).await.unwrap();
        } else {
            let stmt = SQL::r#use().database(db_name).to_string(dialect);
            conn.raw_cmd(&stmt).await.unwrap();
        }
    }

    // Migrate

    pub(crate) async fn db_columns(conn: &dyn Queryable, dialect: SQLDialect, table_name: &str) -> HashSet<SQLColumn> {
        match dialect {
            SQLDialect::SQLite => {
                let columns_result = conn.query(Query::from(format!("pragma table_info('{}')", table_name))).await.unwrap();
                let indices_result = conn.query(Query::from(sqlite_list_indices_query(table_name))).await.unwrap();
                let auto_increment_result = conn.query(Query::from(sqlite_auto_increment_query(table_name))).await.unwrap();
                let db_columns = ColumnDecoder::decode_sqlite_columns(columns_result, indices_result, auto_increment_result);
                db_columns
            }
            _ => {
                let mut results = hashset! {};
                let db_table_columns = conn.query(if dialect == SQLDialect::PostgreSQL {
                    let desc = format!("SELECT * FROM information_schema.columns where table_name = '{}'", table_name);
                    Query::from(desc)
                } else {
                    let desc = SQL::describe(table_name).to_string(dialect);
                    Query::from(desc)
                }).await.unwrap();
                for db_table_column in db_table_columns {
                    let db_column = ColumnDecoder::decode(db_table_column, dialect, conn, table_name).await;
                    results.insert(db_column);
                }
                results
            }
        }
    }

    pub(crate) async fn get_db_user_tables(dialect: SQLDialect, conn: &dyn Queryable) -> Vec<String> {
        match dialect {
            SQLDialect::MySQL => {
                let sql = "SHOW TABLES";
                let db_result = conn.query(Query::from(sql)).await.unwrap();
                db_result.into_iter().map(|result| { result.into_single().unwrap().to_string().unwrap() }).collect()
            }
            SQLDialect::PostgreSQL => {
                let sql = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
                let db_result = conn.query(Query::from(sql)).await.unwrap();
                db_result.into_iter().map(|result| { result.into_single().unwrap().to_string().unwrap() }).collect()
            }
            SQLDialect::SQLite => {
                let sql = "SELECT name FROM sqlite_master WHERE type in ('table') AND name not like 'sqlite?_%' escape '?'";
                let db_result = conn.query(Query::from(sql)).await.unwrap();
                db_result.into_iter().map(|result| { result.into_single().unwrap().to_string().unwrap() }).collect()
            }
            _ => unreachable!()
        }
    }

    pub(crate) async fn rename_table(dialect: SQLDialect, conn: &dyn Queryable, old_name: &str, new_name: &str) {
        let escape = dialect.escape();
        let sql = format!("ALTER TABLE {escape}{old_name}{escape} RENAME TO {escape}{new_name}{escape}");
        conn.execute(Query::from(sql)).await.unwrap();
    }

    pub(crate) async fn table_has_records(dialect: SQLDialect, conn: &dyn Queryable, table_name: &str) -> bool {
        let escape = dialect.escape();
        let sql = format!("select * from {escape}{table_name}{escape} limit 1");
        !conn.query(Query::from(sql)).await.unwrap().is_empty()
    }

    pub(crate) async fn migrate(dialect: SQLDialect, conn: &dyn Queryable, models: Vec<&Model>, pconn: &dyn Transaction) -> Result<()> {
        let mut db_tables = Self::get_db_user_tables(dialect, conn).await;
        // compare each table and do migration
        for model in models {
            let table_name = model.table_name();
            if let Some(migration) = Some(model.migration()) {
                if !db_tables.iter().any(|x| x == table_name) {
                    if let Some(old_name) = &migration.renamed {
                        for old_name in old_name {
                            if db_tables.contains(old_name) {
                                // rename
                                Self::rename_table(dialect, conn, old_name.as_str(), table_name).await;
                                let index = db_tables.clone().iter().find_position(|v| *v == old_name).unwrap().0;
                                db_tables.remove(index);
                                db_tables.push(table_name.to_string());
                                break;
                            }
                        }
                    }
                }

            }
            let is_table_exist = db_tables.iter().any(|x| x == table_name);
            if !is_table_exist {
                // table not exist, create table
                Self::create_table(dialect, conn, model).await;
            } else {
                // remove from list
                let index = db_tables.clone().iter().find_position(|x| *x == table_name).unwrap().0;
                db_tables.remove(index);
                // start migrate for this table
                let model_columns = ColumnDecoder::decode_model_columns(model);
                let db_columns = Self::db_columns(conn, dialect, table_name).await;
                let need_to_alter_any_column = ColumnDecoder::need_to_alter_any_columns(&db_columns, &model_columns);
                if need_to_alter_any_column && dialect == SQLDialect::SQLite {
                    panic!("SQLite doesn't support column altering");
                }
                let table_has_records = Self::table_has_records(dialect, conn, table_name).await;
                let db_indices = Self::db_indices(dialect, conn, model).await;
                let model_indices = Self::normalized_model_indices(model.indexes().values().collect(), dialect, table_name);
                // here update columns and indices
                let manipulations = ColumnDecoder::manipulations(&db_columns, &model_columns, &db_indices, &model_indices, model);
                if table_has_records && manipulations.iter().find(|m| m.is_add_column_non_null()).is_some() && model.allows_drop_when_migrate() {
                    Self::drop_table(dialect, conn, table_name).await;
                    Self::create_table(dialect, conn, model).await;
                } else {
                    for m in manipulations.iter() {
                        match m {
                            ColumnManipulation::CreateIndex(index) => {
                                if !index.name().starts_with("teo_primary_sqlite_index") {
                                    let create = index.to_sql_create(dialect, table_name);
                                    conn.execute(Query::from(create)).await.unwrap();
                                }
                            }
                            ColumnManipulation::DropIndex(index) => {
                                if !index.name().starts_with("teo_primary_sqlite_index") {
                                    let drop = index.to_sql_drop(dialect, table_name);
                                    conn.execute(Query::from(drop)).await.unwrap();
                                }
                            }
                            ColumnManipulation::AddColumn(column, default) => {
                                if column.not_null() && default.is_none() {
                                    // if any records, just raise here
                                    let has_records = Self::table_has_records(dialect, conn, table_name).await;
                                    if has_records {
                                        panic!("Cannot add new non null column `{}', table `{}' has records. Consider add a default value or drop the table.", column.name(), table_name)
                                    }
                                }
                                let mut c = column.clone().clone();
                                if default.is_some() {
                                    c.set_default(Some(ToSQLString::to_string(&default.as_ref().unwrap(), dialect)));
                                }
                                let stmt = SQL::alter_table(table_name).add(c).to_string(dialect);
                                conn.execute(Query::from(stmt)).await.unwrap();
                            }
                            ColumnManipulation::AlterColumn(old_column, new_column) => {
                                if dialect != SQLDialect::PostgreSQL {
                                    let alter = SQL::alter_table(table_name).modify(new_column.clone().clone()).to_string(dialect);
                                    conn.execute(Query::from(alter)).await.unwrap();
                                } else {
                                    let clauses = Self::psql_alter_clauses(table_name, *old_column, *new_column);
                                    for clause in clauses {
                                        conn.execute(Query::from(clause)).await.unwrap();
                                    }
                                }
                            }
                            ColumnManipulation::RemoveColumn(name) => {
                                let stmt = SQL::alter_table(table_name).drop_column(name).to_string(dialect);
                                conn.execute(Query::from(stmt)).await.unwrap();
                            }
                            ColumnManipulation::RenameColumn { old, new } => {
                                let stmt = if dialect == SQLDialect::PostgreSQL {
                                    format!("ALTER TABLE {} RENAME COLUMN '{}' TO '{}'", table_name, old, new)
                                } else {
                                    format!("ALTER TABLE {} RENAME COLUMN `{}` TO `{}`", table_name, old, new)
                                };
                                conn.execute(Query::from(stmt)).await.unwrap();
                            }
                        }
                    }
                }
            }
        }
        // drop tables
        for table in db_tables {
            Self::drop_table(dialect, conn, &table).await;
        }
        Ok(())
    }

    async fn drop_table(dialect: SQLDialect, conn: &dyn Queryable, table: &str) {
        let escape = dialect.escape();
        let sql = format!("DROP TABLE {escape}{table}{escape}");
        conn.execute(Query::from(sql)).await.unwrap();
    }

    async fn create_table(dialect: SQLDialect, conn: &dyn Queryable, model: &Model) {
        // create table
        let stmt = SQLCreateTableStatement::from(model).to_string(dialect);
        conn.execute(Query::from(stmt)).await.unwrap();
        // create indices
        for (_name, index) in model.indexes() {
            // primary is created when creating table
            if index.r#type().is_primary() { continue }
            let stmt = index.to_sql_create(dialect, model.table_name());
            conn.execute(Query::from(stmt)).await.unwrap();
        }
    }

    fn psql_alter_clauses(table: &str, old_column: &SQLColumn, new_column: &SQLColumn) -> Vec<String> {
        let mut result = vec![];
        let name = new_column.name();
        let escape = SQLDialect::PostgreSQL.escape();
        if old_column.r#type() != new_column.r#type() {
            result.push(format!("ALTER TABLE {escape}{table}{escape} ALTER COLUMN {escape}{name}{escape} TYPE {}", new_column.r#type().to_sql_string()));
        }
        if old_column.default().is_none() && new_column.default().is_some() {
            result.push(format!("ALTER TABLE {escape}{table}{escape} ALTER COLUMN {escape}{name}{escape} SET DEFAULT {}", new_column.default().unwrap()));
        } else if old_column.default().is_some() && new_column.default().is_none() {
            result.push(format!("ALTER TABLE {escape}{table}{escape} ALTER COLUMN {escape}{name}{escape} DROP DEFAULT"));
        } else if old_column.default().is_some() && new_column.default().is_some() {
            if old_column.default() == new_column.default() {
                result.push(format!("ALTER TABLE {escape}{table}{escape} ALTER COLUMN {escape}{name}{escape} SET DEFAULT {}", new_column.default().unwrap()));
            }
        }
        result
    }

    fn normalized_model_indices(indices: Vec<&Index>, dialect: SQLDialect, table_name: &str) -> HashSet<Index> {
        let mut results: Vec<Index> = indices.iter().map(|index| {
            let sql_name_cow = index.sql_name(table_name, dialect);
            let sql_name = sql_name_cow.as_ref().to_owned();
            Index::new(index.r#type(), sql_name, index.items().clone())
        }).collect();
        if dialect == SQLDialect::PostgreSQL {
            if let Some(primary) = results.iter().find(|r| r.r#type().is_primary()) {
                let unique_for_primary = primary.psql_primary_to_unique(table_name);
                results.push(unique_for_primary);
            }
        }
        results.into_iter().collect()
    }

    async fn db_indices(dialect: SQLDialect, conn: &dyn Queryable, model: &Model) -> HashSet<Index> {
        match dialect {
            SQLDialect::PostgreSQL => Self::psql_db_indices(conn, model).await,
            SQLDialect::MySQL => Self::mysql_db_indices(conn, model).await,
            SQLDialect::SQLite => Self::sqlite_db_indices(conn, model).await,
            _ => unreachable!(),
        }
    }

    async fn mysql_db_indices(conn: &dyn Queryable, model: &Model) -> HashSet<Index> {
        let table_name = model.table_name();
        let sql = format!("SHOW INDEX FROM `{}`", table_name);
        let result_set = conn.query(Query::from(sql)).await.unwrap();
        let mut indices = vec![];
        for row in result_set {
            let index_name = row.get("Key_name").unwrap().to_string().unwrap();
            let column_name = row.get("Column_name").unwrap().to_string().unwrap();
            let order = Sort::from_mysql_str(row.get("Collation").unwrap().as_str().unwrap()).unwrap();
            if let Some(position) = indices.iter().position(|m: &Index| m.name() == &index_name) {
                let model_index = indices.get_mut(position).unwrap();
                let mut items = model_index.items().clone();
                items.push(Item::new(column_name, order, None));
                let new_model_index = Index::new(
                    model_index.r#type(),
                    model_index.name().to_string(),
                    items
                );
                indices[position] = new_model_index;
            } else {
                let is_unique = !row.get("Non_unique").unwrap().as_bool().unwrap();
                let item = Item::new(column_name, order, None);
                indices.push(Index::new(
                    if &index_name == "PRIMARY" { Type::Primary } else if is_unique { Type::Unique } else { Type::Index },
                    index_name.to_owned(),
                    vec![item],
                ))
            }
        }
        indices.into_iter().collect()
    }

    async fn psql_db_indices(conn: &dyn Queryable, model: &Model) -> HashSet<Index> {
        let table_name = model.table_name();
        let sql = format!(r#"SELECT     irel.relname                           AS index_name,
           a.attname                              AS column_name,
           i.indisunique                          AS is_unique,
           i.indisprimary                         AS is_primary,
           1 + Array_position(i.indkey, a.attnum) AS column_position,
           CASE o.OPTION
                                 & 1
                      WHEN 1 THEN 'DESC'
                      ELSE 'ASC'
           END      AS order
FROM       pg_index AS i
join       pg_class AS trel
ON         trel.oid = i.indrelid
join       pg_namespace AS tnsp
ON         trel.relnamespace = tnsp.oid
join       pg_class AS irel
ON         irel.oid = i.indexrelid
cross join lateral unnest (i.indkey) WITH ordinality    AS c (colnum, ordinality)
left join  lateral unnest (i.indoption) WITH ordinality AS o (OPTION, ordinality)
ON         c.ordinality = o.ordinality
join       pg_attribute AS a
ON         trel.oid = a.attrelid
AND        a.attnum = c.colnum
WHERE      tnsp.nspname='public'
AND        trel.relname='{table_name}'
GROUP BY   tnsp.nspname,
           trel.relname,
           irel.relname,
           i.indisunique,
           i.indisprimary,
           a.attname,
           array_position(i.indkey, a.attnum),
           o.OPTION ORDER BY column_position
"#);
        let result_set = conn.query(Query::from(sql)).await.unwrap();
        let mut indices = vec![];
        for row in result_set {
            let index_name = row.get("index_name").unwrap().to_string().unwrap();
            let column_name = row.get("column_name").unwrap().to_string().unwrap();
            let order = Sort::from_str(row.get("order").unwrap().as_str().unwrap()).unwrap();
            if let Some(position) = indices.iter().position(|m: &Index| m.name() == index_name) {
                let model_index = indices.get_mut(position).unwrap();
                let mut items = model_index.items().clone();
                items.push(Item::new(column_name, order, None));
                let new_model_index = Index::new(
                    model_index.r#type(),
                    model_index.name().to_string(),
                    items
                );
                indices[position] = new_model_index;
            } else {
                let is_unique = row.get("is_unique").unwrap().as_bool().unwrap();
                let is_primary = row.get("is_primary").unwrap().as_bool().unwrap();
                let item = Item::new(column_name, order, None);
                indices.push(Index::new(
                    if is_primary { Type::Primary } else if is_unique { Type::Unique} else { Type::Index },
                    index_name.clone(),
                    vec![item],
                ))
            }
        }
        indices.into_iter().collect()
    }

    async fn sqlite_db_indices(conn: &dyn Queryable, model: &Model) -> HashSet<Index> {
        let table_name = model.table_name();
        let sql = format!(r#"SELECT
    il.name as index_name,
    ii.name as column_name,
    CASE il.origin when 'pk' then 1 else 0 END as is_primary,
    CASE il.[unique] when 1 then 0 else 1 END as non_unique,
    il.[unique] as is_unique,
    il.partial,
    il.seq as seq,
    ii.desc as `desc`
FROM sqlite_master AS m,
    pragma_index_list(m.name) AS il,
    pragma_index_xinfo(il.name) AS ii
WHERE
    m.type = 'table'
    and m.tbl_name = '{}'
    and ii.name is not null
GROUP BY
    m.tbl_name,
    il.name,
    ii.name,
    il.origin,
    il.partial,
    il.seq,
    ii.`desc`
ORDER BY 1,6"#, table_name);
        let result_set = conn.query(Query::from(sql)).await.unwrap();
        let mut indices = vec![];
        for row in result_set {
            let index_name = row.get("index_name").unwrap().to_string().unwrap();
            let column_name = row.get("column_name").unwrap().to_string().unwrap();
            let order = Sort::from_desc_bool(row.get("desc").unwrap().as_bool().unwrap());
            if let Some(position) = indices.iter().position(|m: &Index| m.name() == &index_name) {
                let model_index = indices.get_mut(position).unwrap();
                let item = Item::new(column_name, order, None);
                let mut items = model_index.items().clone();
                items.push(item);
                indices[position] = Index::new(
                    model_index.r#type(),
                    model_index.name().to_string(),
                    model_index.items().clone()
                );
            } else {
                let is_unique = row.get("is_unique").unwrap().as_bool().unwrap();
                let is_primary = row.get("is_primary").unwrap().as_bool().unwrap();
                let item = Item::new(column_name, order, None);
                indices.push(Index::new(
                    if is_primary { Type::Primary } else if is_unique { Type::Unique} else { Type::Index },
                    index_name.clone(),
                    vec![item],
                ))
            }
        }
        let mut results: Vec<Index> = indices.into_iter().collect();
        // let includes_primary = results.iter().find(|r| {
        //     r.r#type() == Type::Primary
        // }).is_some();
        // if !includes_primary {
        //     let sql = format!("SELECT * FROM pragma_table_info(\"{table_name}\") WHERE pk = 1");
        //     let result_set = conn.query(Query::from(sql)).await.unwrap();
        //     let row = result_set.into_single().unwrap();
        //     let column_name = row.get("name").unwrap().to_string().unwrap();
        //     let leaked = column_name;
        //     let index = Index::new(Type::Primary, format!("sqlite_autoindex_{table_name}_1"), vec![
        //         Item::new(leaked, Sort::Asc, None)
        //     ]);
        //     results.push(index);
        // }
        results.into_iter().collect()
    }
}

use crate::stmts::alter_table::SQLAlterTableStatement;
use crate::stmts::create::SQLCreateStatement;
use crate::stmts::delete_from::SQLDeleteFromStatement;
use crate::stmts::describe::SQLDescribeStatement;
use crate::stmts::drop::SQLDropStatement;
use crate::stmts::insert_into::SQLInsertIntoStatement;
use crate::stmts::r#use::SQLUseStatement;
use crate::stmts::select::SQLSelectStatement;
use crate::stmts::show::SQLShowStatement;
use crate::stmts::update::SQLUpdateStatement;

pub mod create;
pub mod drop;
pub mod insert_into;
pub mod r#use;
pub mod show;
pub mod describe;
pub mod alter_table;
pub mod select;
pub mod update;
pub mod delete_from;

pub(crate) struct SQL { }

impl SQL {
    pub(crate) fn create() -> SQLCreateStatement {
        SQLCreateStatement { }
    }

    pub(crate) fn drop() -> SQLDropStatement {
        SQLDropStatement { }
    }

    pub(crate) fn r#use() -> SQLUseStatement {
        SQLUseStatement { }
    }

    pub(crate) fn show() -> SQLShowStatement {
        SQLShowStatement { }
    }

    pub(crate) fn describe(table: impl Into<String>) -> SQLDescribeStatement {
        SQLDescribeStatement { table: table.into() }
    }

    pub(crate) fn alter_table(table: impl Into<String>) -> SQLAlterTableStatement {
        SQLAlterTableStatement { table: table.into() }
    }

    pub(crate) fn insert_into(table: &str) -> SQLInsertIntoStatement {
        SQLInsertIntoStatement { table, values: vec![], returning: vec![] }
    }

    pub(crate) fn update(table: &str) -> SQLUpdateStatement {
        SQLUpdateStatement { table, values: vec![], r#where: "" }
    }

    pub(crate) fn delete_from(from: &str) -> SQLDeleteFromStatement {
        SQLDeleteFromStatement { from, r#where: None }
    }

    pub(crate) fn select<'a>(columns: Option<&'a Vec<&'a str>>, from: &'a str) -> SQLSelectStatement<'a> {
        SQLSelectStatement { columns, from, r#where: None, order_by: None, limit: None, left_join: None, inner_join: None }
    }
}

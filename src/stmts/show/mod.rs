use crate::stmts::show::index_from::SQLShowIndexFromStatement;
use crate::stmts::show::tables::SQLShowTablesStatement;

pub mod tables;
pub mod index_from;

pub(crate) struct SQLShowStatement { }

impl SQLShowStatement {
    pub(crate) fn tables(&self) -> SQLShowTablesStatement {
        SQLShowTablesStatement { like: None }
    }

    pub(crate) fn index_from(&self, table: impl Into<String>) -> SQLShowIndexFromStatement {
        SQLShowIndexFromStatement { table: table.into() }
    }
}

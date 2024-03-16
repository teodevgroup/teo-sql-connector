use std::borrow::Cow;
use teo_runtime::index::Type;
use teo_runtime::model::index::Item;
use crate::exts::sort::SortExt;
use crate::schema::dialect::SQLDialect;

pub trait IndexExt {

    fn psql_primary_to_unique(&self, table_name: &str) -> Self;

    fn sql_name(&self, table_name: &str, dialect: SQLDialect) -> Cow<str>;

    fn joined_names(&self) -> String;

    fn psql_suffix(&self) -> &str;

    fn normalize_name_psql(&self, table_name: &str) -> String;

    fn normalize_name_normal(&self, table_name: &str) -> String;

    fn normalize_name(&self, table_name: &str, dialect: SQLDialect) -> String;

    fn to_sql_drop(&self, dialect: SQLDialect, table_name: &str) -> String;

    fn to_sql_create(&self, dialect: SQLDialect, table_name: &str) -> String;

    fn sql_format_item(dialect: SQLDialect, item: &Item, table_create_mode: bool) -> String;
}

impl IndexExt for teo_runtime::model::Index {

    fn psql_primary_to_unique(&self, table_name: &str) -> Self {
        Self {
            r#type: Type::Unique,
            name: format!("{table_name}_{}_pkey", self.joined_names()),
            items: self.items.clone(),
            cache: self.cache.clone(),
        }
    }

    fn sql_name(&self, table_name: &str, dialect: SQLDialect) -> Cow<str> {
        if self.r#type.is_primary() {
            Cow::Owned(self.normalize_name(table_name, dialect))
        } else {
            if dialect.is_sqlite() {
                Cow::Owned(format!("{}_{}", table_name, self.name()))
            } else {
                Cow::Borrowed(self.name.as_str())
            }
        }
    }

    fn joined_names(&self) -> String {
        self.cache.keys.join("_")
    }

    fn psql_suffix(&self) -> &str {
        if self.r#type.is_primary() {
            "pkey"
        } else {
            "idx"
        }
    }

    fn normalize_name_psql(&self, table_name: &str) -> String {
        if self.r#type.is_primary() {
            format!("{table_name}_{}", self.psql_suffix())
        } else {
            format!("{table_name}_{}_{}", self.joined_names(), self.psql_suffix())
        }
    }

    fn normalize_name_normal(&self, table_name: &str) -> String {
        format!("{table_name}_{}", self.joined_names())
    }

    fn normalize_name(&self, table_name: &str, dialect: SQLDialect) -> String {
        match self.r#type {
            Type::Primary => match dialect {
                SQLDialect::MySQL => "PRIMARY".to_owned(),
                SQLDialect::SQLite => format!("sqlite_autoindex_{}_1", table_name),
                SQLDialect::PostgreSQL => self.normalize_name_psql(table_name),
                _ => unreachable!()
            },
            _ => match dialect {
                SQLDialect::PostgreSQL => self.normalize_name_psql(table_name),
                _ => self.normalize_name_normal(table_name),
            }
        }
    }

    fn to_sql_drop(&self, dialect: SQLDialect, table_name: &str) -> String {
        let escape = dialect.escape();
        let index_name_cow = self.sql_name(table_name, dialect);
        let index_name = index_name_cow.as_ref();
        if dialect == SQLDialect::PostgreSQL {
            format!("DROP INDEX {escape}{index_name}{escape}")
        } else {
            format!("DROP INDEX {escape}{index_name}{escape} ON {escape}{table_name}{escape}")
        }
    }

    fn to_sql_create(&self, dialect: SQLDialect, table_name: &str) -> String {
        let escape = dialect.escape();
        let index_name_cow = self.sql_name(table_name, dialect);
        let index_name = index_name_cow.as_ref();
        let unique = if self.r#type().is_unique() { "UNIQUE " } else { "" };
        let fields: Vec<String> = self.items.iter().map(|item| {
            Self::sql_format_item(dialect, item, false)
        }).collect();
        format!("CREATE {unique}INDEX {escape}{index_name}{escape} ON {escape}{table_name}{escape}({})", fields.join(","))
    }

    fn sql_format_item(dialect: SQLDialect, item: &Item, table_create_mode: bool) -> String {
        let escape = dialect.escape();
        let name = &item.field;
        let sort = item.sort.to_str();
        let len = if let Some(len) = item.len {
            if dialect == SQLDialect::MySQL {
                Cow::Owned(format!("({})", len))
            } else {
                Cow::Borrowed("")
            }
        } else {
            Cow::Borrowed("")
        };
        if table_create_mode && dialect == SQLDialect::PostgreSQL {
            format!("{escape}{name}{escape}")
        } else {
            format!("{escape}{name}{escape}{len} {sort}")
        }
    }
}
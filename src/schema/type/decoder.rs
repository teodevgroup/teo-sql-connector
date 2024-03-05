use std::str::FromStr;
use teo_runtime::database::r#type::DatabaseType;
use regex::Regex;
use snailquote::unescape;
use teo_runtime::database::mysql::r#type::{MySQLEnum, MySQLType};
use teo_runtime::database::postgres::r#type::PostgreSQLType;
use teo_runtime::database::sqlite::r#type::SQLiteType;
use crate::schema::dialect::SQLDialect;

pub(crate) struct SQLTypeDecoder { }

impl SQLTypeDecoder {
    pub(crate) fn decode(r#type: &str, dialect: SQLDialect) -> DatabaseType {
        match dialect {
            SQLDialect::MySQL => DatabaseType::MySQLType(mysql_type_to_database_type(r#type)),
            SQLDialect::PostgreSQL => DatabaseType::PostgreSQLType(postgresql_type_to_database_type(r#type)),
            SQLDialect::SQLite => DatabaseType::SQLiteType(sqlite_type_to_database_type(r#type)),
            SQLDialect::MSSQL => panic!(),
        }
    }
}

fn mysql_type_to_database_type(r#type: &str) -> MySQLType {
    let r#type_string = r#type.to_lowercase();
    let r#type: &str = r#type_string.as_str();
    let regex = Regex::new("([^ \\(\\)]+)( (.+))?(\\((.+)\\))?").unwrap();
    match regex.captures(r#type) {
        None => panic!("Unhandled database type '{}' '{}'.", r#type, regex),
        Some(captures) => {
            let name = captures.get(1).unwrap().as_str();
            let trailing1 = captures.get(3).map(|m| m.as_str());
            let arg = captures.get(5).map(|m| m.as_str());
            match name {
                "bit" => MySQLType::Bit(arg.map(|a| i32::from_str(a).unwrap())),
                "tinyint" => MySQLType::TinyInt(arg.map(|a| i32::from_str(a).unwrap()), trailing1.is_some()),
                "smallint" => MySQLType::SmallInt(arg.map(|a| i32::from_str(a).unwrap()), trailing1.is_some()),
                "mediumint" => MySQLType::MediumInt(arg.map(|a| i32::from_str(a).unwrap()), trailing1.is_some()),
                "int" => MySQLType::Int(arg.map(|a| i32::from_str(a).unwrap()), trailing1.is_some()),
                "bigint" => MySQLType::BigInt(arg.map(|a| i32::from_str(a).unwrap()), trailing1.is_some()),
                "float" => MySQLType::Float,
                "double" => MySQLType::Double,
                "char" => MySQLType::Char(arg.map(|a| i32::from_str(a).unwrap()).unwrap()),
                "varchar" => MySQLType::VarChar(arg.map(|a| i32::from_str(a).unwrap()).unwrap()),
                "text" => MySQLType::Text,
                "mediumtext" => MySQLType::MediumText,
                "longtext" => MySQLType::LongText,
                "date" => MySQLType::Date,
                "datetime" => MySQLType::DateTime(i32::from_str(arg.unwrap()).unwrap()),
                "decimal" => {
                    if let Some(args) = arg {
                        let args = args.split(",").into_iter().collect::<Vec<&str>>();
                        MySQLType::Decimal(args.get(0).unwrap().parse().unwrap(), args.get(1).unwrap().parse().unwrap())
                    } else {
                        panic!()
                    }
                }
                "enum" => {
                    let choices = arg.unwrap();
                    let choices_vec = choices.split(",");
                    let unescaped: Vec<String> = choices_vec.map(|s| unescape(s).unwrap()).collect();
                    MySQLType::Enum(MySQLEnum { variants: unescaped })
                }
                _ => panic!("Unhandled type '{}' '{:?}' '{:?}'.", name, trailing1, arg)
            }
        }
    }
}

fn postgresql_type_to_database_type(r#type: &str) -> PostgreSQLType {
    let lower = r#type.to_lowercase();
    let lower_str = lower.as_str();
    match lower_str {
        "integer" | "int4" => PostgreSQLType::Integer,
        "text" => PostgreSQLType::Text,
        "timestamp with time zone" => PostgreSQLType::Timestamp(3, true),
        "timestamp without time zone" | "timestamp" => PostgreSQLType::Timestamp(3, false),
        "boolean" | "bool" => PostgreSQLType::Boolean,
        "bigint" | "int8" => PostgreSQLType::BigInt,
        "double precision" | "float8" => PostgreSQLType::DoublePrecision,
        "real" | "float4" => PostgreSQLType::Real,
        "date" => PostgreSQLType::Date,
        "numeric" => PostgreSQLType::Decimal(65, 30),
        _ => if lower_str.starts_with("array|") {
            let inner = &lower_str[6..];
            PostgreSQLType::Array(Box::new(postgresql_type_to_database_type(inner)))
        } else {
            panic!("Unhandled database type {}", r#type)
        }
    }
}

fn sqlite_type_to_database_type(r#type: &str) -> SQLiteType {
    let r#type_string = r#type.to_lowercase();
    let r#type: &str = r#type_string.as_str();
    let regex = Regex::new("([^ \\(\\)]+)( (.+))?(\\((.+)\\))?").unwrap();
    match regex.captures(r#type) {
        None => panic!("Unhandled database type '{}' '{}'.", r#type, regex),
        Some(captures) => {
            let name = captures.get(1).unwrap().as_str();
            let trailing1 = captures.get(3).map(|m| m.as_str());
            let arg = captures.get(5).map(|m| m.as_str());
            match name {
                "integer" => SQLiteType::Integer,
                "text" => SQLiteType::Text,
                "real" => SQLiteType::Real,
                "double" => SQLiteType::Real,
                "decimal" => SQLiteType::Decimal,
                _ => panic!("Unhandled type '{}' '{:?}' '{:?}'.", name, trailing1, arg)
            }
        }
    }
}

fn mssql_type_to_database_type(r#type: &str) -> DatabaseType {
    match r#type.to_lowercase().as_str() {
        _ => panic!("Unhandled database type.")
    }
}
